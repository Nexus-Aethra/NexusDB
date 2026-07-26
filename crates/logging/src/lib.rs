//! nlog: NexusDB 日志模块 — io_uring + 协程融合的累积批量写入 logger.
//!
//! ## 架构 (与数据路径同构, 复用 inbox/eventfd 成熟模式)
//!
//! ```text
//! 任意线程 (shard/worker/acceptor/main)      Log 线程 (自建 Scheduler + io_uring)
//!   nlog::info!(...)                           loop {
//!     → level 过滤 (AtomicU8, 被过滤=零开销)       poll(eventfd, timeout=flush_interval)
//!     → 格式化一条记录                             drain 无锁队列 → 追加累积缓冲
//!     → 无锁 MPSC ring push                       量 >= buffer_bytes 或时间到 →
//!     → coalesced eventfd 通知 (首条才写)            协程 io_ops::write + fsync (io_uring)
//!                                              }
//! ```
//!
//! - 前端热路径: 1 次 atomic load + 无锁 push, 最多 1 次 eventfd write (搭车 0 次)
//! - 落盘全走 `scheduler::io_ops` (io_uring), 写盘期间双缓冲继续 drain
//! - 背压策略: 队列满时 Debug/Trace 丢弃计数, Error/Warn 自旋重试 — 绝不阻塞数据路径
//! - Error/Warn 额外直通 stderr (低频, 保障故障可见)

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::os::unix::io::{IntoRawFd, RawFd};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_queue::ArrayQueue;

// =====================================================================
// Level
// =====================================================================

/// 日志级别 (数值越小越严重).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        }
    }

    /// 解析级别字符串 (大小写不敏感).
    pub fn parse(s: &str) -> Option<Level> {
        match s.to_ascii_lowercase().as_str() {
            "error" => Some(Level::Error),
            "warn" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            "trace" => Some(Level::Trace),
            _ => None,
        }
    }
}

/// 全局级别开关. 0 = 未初始化 (全部过滤, Error/Warn 保底 stderr).
static LEVEL: AtomicU8 = AtomicU8::new(0);

/// 纯函数: 给定当前全局级别值, 判断某条日志是否放行.
#[inline]
pub fn enabled_at(current: u8, level: Level) -> bool {
    current != 0 && (level as u8) <= current
}

/// 热路径过滤: 一次 atomic load.
#[inline]
pub fn enabled(level: Level) -> bool {
    enabled_at(LEVEL.load(Ordering::Relaxed), level)
}

// =====================================================================
// LogQueue: 无锁 MPSC + coalesced eventfd (复用 inbox 模式)
// =====================================================================

const DEFAULT_QUEUE_CAP: usize = 16384;

/// 多生产者无锁日志队列. 前端 push, log 线程 drain.
pub struct LogQueue {
    ring: ArrayQueue<String>,
    eventfd: RawFd,
    /// coalesced 通知计数: 首条 push (0→1) 才写 eventfd, 后续搭车.
    pending: AtomicU64,
    /// 背压丢弃计数 (Debug/Trace 队列满时丢弃).
    dropped: AtomicU64,
}

impl LogQueue {
    pub fn with_capacity(cap: usize) -> Self {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        assert!(fd >= 0, "eventfd creation failed");
        Self {
            ring: ArrayQueue::new(cap),
            eventfd: fd,
            pending: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// 前端 push. 队列满时: Error/Warn 自旋重试 (不可丢), 其余丢弃计数.
    pub fn push(&self, level: Level, line: String) {
        let mut item = line;
        loop {
            match self.ring.push(item) {
                Ok(()) => break,
                Err(rejected) => {
                    if (level as u8) <= Level::Warn as u8 {
                        item = rejected;
                        std::thread::yield_now();
                    } else {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            }
        }
        // coalesced 通知: 首条才写 eventfd
        if self.pending.fetch_add(1, Ordering::AcqRel) == 0 {
            self.notify();
        }
    }

    /// 无条件写 eventfd (shutdown 唤醒用).
    pub fn notify(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(self.eventfd, &val as *const u64 as *const libc::c_void, 8);
        }
    }

    /// log 线程 drain. **先重置 pending 再 pop** (防丢唤醒, 同 inbox 修复).
    pub fn drain_into(&self, acc: &mut Vec<u8>) -> usize {
        self.pending.store(0, Ordering::Release);
        let mut n = 0;
        while let Some(line) = self.ring.pop() {
            acc.extend_from_slice(line.as_bytes());
            n += 1;
        }
        n
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// 累计丢弃条数 (背压).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn eventfd(&self) -> RawFd {
        self.eventfd
    }
}

impl Drop for LogQueue {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.eventfd);
        }
    }
}

unsafe impl Send for LogQueue {}
unsafe impl Sync for LogQueue {}

// =====================================================================
// Backend: 专用 log 线程 (自建 Scheduler + io_uring 落盘)
// =====================================================================

/// 后端配置.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// 日志文件路径.
    pub file: PathBuf,
    /// 累积量阈值 (bytes).
    pub buffer_bytes: usize,
    /// 时间阈值.
    pub flush_interval: Duration,
}

/// log 后端: 持有 stop 标志 + join handle.
pub struct Backend {
    queue: Arc<LogQueue>,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Backend {
    /// 启动 log 线程. 打开 (append 语义, 自管 offset) 日志文件.
    pub fn start(cfg: BackendConfig, queue: Arc<LogQueue>) -> io::Result<Self> {
        if let Some(parent) = cfg.file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false) // append 语义: 自管 offset 续写, 不截断历史日志
            .write(true)
            .open(&cfg.file)?;
        let start_offset = file.metadata()?.len();
        let fd = file.into_raw_fd();

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let queue2 = queue.clone();
        let join = std::thread::Builder::new()
            .name("nlog-flusher".to_string())
            .spawn(move || backend_main(fd, start_offset, cfg, queue2, stop2))
            .map_err(|e| io::Error::other(format!("spawn nlog-flusher: {e}")))?;

        Ok(Self {
            queue,
            stop,
            join: Some(join),
        })
    }

    /// 优雅退出: 置停止标志 + 唤醒 → log 线程 final flush + fsync → join.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        self.queue.notify();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// log 线程主函数: 自建 Scheduler (独立 io_uring ring), 遵守
/// "spawn/drive 全在本线程" 的多线程契约 (同 shard_thread_main).
fn backend_main(
    fd: RawFd,
    start_offset: u64,
    cfg: BackendConfig,
    queue: Arc<LogQueue>,
    stop: Arc<AtomicBool>,
) {
    let rt = scheduler::SchedHandle::new(scheduler::Scheduler::new());
    // io_ops 通过 thread-local current scheduler 提交 SQE, 必须先注册
    rt.set_current();
    let mut offset = start_offset;
    let mut acc: Vec<u8> = Vec::with_capacity(cfg.buffer_bytes * 2);
    let mut last_flush = Instant::now();
    let timeout_ms = cfg.flush_interval.as_millis().max(1) as i32;

    loop {
        let stopping = stop.load(Ordering::Acquire);
        if !stopping && acc.len() < cfg.buffer_bytes {
            // 慢路径: poll 真阻塞等 eventfd (零 CPU), 时间阈值由 timeout 兜底
            let mut fds = [libc::pollfd {
                fd: queue.eventfd(),
                events: libc::POLLIN,
                revents: 0,
            }];
            unsafe {
                libc::poll(fds.as_mut_ptr(), 1, timeout_ms);
            }
            if fds[0].revents & libc::POLLIN != 0 {
                let mut v: u64 = 0;
                unsafe {
                    libc::read(queue.eventfd(), &mut v as *mut u64 as *mut libc::c_void, 8);
                }
            }
        }

        queue.drain_into(&mut acc);

        let timed_out = last_flush.elapsed() >= cfg.flush_interval;
        if !acc.is_empty() && (stopping || acc.len() >= cfg.buffer_bytes || timed_out) {
            let buf = std::mem::take(&mut acc);
            offset = flush_uring(&rt, fd, offset, buf, &queue, &mut acc);
            last_flush = Instant::now();
        }

        if stopping && queue.is_empty() && acc.is_empty() {
            break;
        }
    }
    unsafe {
        libc::close(fd);
    }
}

/// 用 io_uring 协程把 buf 写入 fd (write_all + fsync).
/// 写盘期间双缓冲: 继续 drain 新日志进 next_acc.
fn flush_uring(
    rt: &scheduler::SchedHandle,
    fd: RawFd,
    offset: u64,
    buf: Vec<u8>,
    queue: &LogQueue,
    next_acc: &mut Vec<u8>,
) -> u64 {
    let done: Rc<RefCell<Option<u64>>> = Rc::new(RefCell::new(None));
    let done2 = done.clone();
    scheduler::spawn_on(
        rt,
        Box::pin(async move {
            let mut off = offset;
            let mut written = 0usize;
            while written < buf.len() {
                match scheduler::io_ops::write(fd, &buf[written..], off).await {
                    Ok(n) if n > 0 => {
                        written += n;
                        off += n as u64;
                    }
                    // 写失败: 放弃本批 (日志不可反压业务), 保留已写偏移
                    _ => break,
                }
            }
            let _ = scheduler::io_ops::fsync(fd).await;
            *done2.borrow_mut() = Some(off);
        }),
    );
    loop {
        if let Some(off) = *done.borrow() {
            return off;
        }
        rt.clone().drive_until_idle(256);
        // 双缓冲: 写盘等待期间继续吸收新日志
        queue.drain_into(next_acc);
    }
}

// =====================================================================
// 全局 Logger + 宏
// =====================================================================

/// 初始化参数 (由上层从 config 映射).
#[derive(Debug, Clone)]
pub struct LogSettings {
    pub level: Level,
    /// None = 不写文件, 全部走 stderr.
    pub dir: Option<PathBuf>,
    pub buffer_bytes: usize,
    pub flush_interval: Duration,
    /// Error/Warn 直通 stderr.
    pub stderr: bool,
}

struct GlobalLogger {
    queue: Option<Arc<LogQueue>>,
    backend: Mutex<Option<Backend>>,
    stderr: bool,
}

static GLOBAL: OnceLock<GlobalLogger> = OnceLock::new();

/// 初始化全局 logger. 重复调用返回 Err.
pub fn init(settings: &LogSettings) -> io::Result<()> {
    let (queue, backend) = match &settings.dir {
        Some(dir) => {
            let queue = Arc::new(LogQueue::with_capacity(DEFAULT_QUEUE_CAP));
            let file = dir.join(format!("nexusdb-{}.log", today_yyyymmdd()));
            let backend = Backend::start(
                BackendConfig {
                    file,
                    buffer_bytes: settings.buffer_bytes,
                    flush_interval: settings.flush_interval,
                },
                queue.clone(),
            )?;
            (Some(queue), Some(backend))
        }
        None => (None, None),
    };
    GLOBAL
        .set(GlobalLogger {
            queue,
            backend: Mutex::new(backend),
            stderr: settings.stderr,
        })
        .map_err(|_| io::Error::other("nlog already initialized"))?;
    LEVEL.store(settings.level as u8, Ordering::Release);
    Ok(())
}

/// 优雅退出: final flush + fsync + join log 线程. main 退出前调用.
pub fn shutdown() {
    LEVEL.store(0, Ordering::Release);
    if let Some(g) = GLOBAL.get()
        && let Some(backend) = g.backend.lock().unwrap().take()
    {
        backend.shutdown();
    }
}

/// 宏后端入口: 过滤 → 格式化 → 分发.
pub fn log(level: Level, module: &str, args: fmt::Arguments) {
    let cur = LEVEL.load(Ordering::Relaxed);
    if !enabled_at(cur, level) {
        // 未初始化时 Error/Warn 保底可见 (等价旧 eprintln 行为)
        if cur == 0 && (level as u8) <= Level::Warn as u8 {
            eprintln!("[{}][{module}] {args}", level.as_str());
        }
        return;
    }
    let line = format_line(level, module, args);
    let g = GLOBAL.get().expect("LEVEL != 0 implies initialized");
    let urgent = (level as u8) <= Level::Warn as u8;
    if g.stderr && urgent {
        eprint!("{line}");
    }
    match &g.queue {
        Some(q) => q.push(level, line),
        // 无文件后端: 非紧急日志也走 stderr (紧急的上面已经打过)
        None => {
            if !(g.stderr && urgent) {
                eprint!("{line}");
            }
        }
    }
}

/// 格式化: `[2026-07-24 01:23:45.678][INFO][module] msg\n`
fn format_line(level: Level, module: &str, args: fmt::Arguments) -> String {
    let (tm, millis) = now_tm();
    format!(
        "[{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}][{}][{}] {}\n",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        millis,
        level.as_str(),
        module,
        args
    )
}

fn now_tm() -> (libc::tm, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() as libc::time_t;
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe {
        libc::localtime_r(&secs, &mut tm);
    }
    (tm, now.subsec_millis())
}

fn today_yyyymmdd() -> String {
    let (tm, _) = now_tm();
    format!("{:04}{:02}{:02}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday)
}

// ===== 宏 =====

#[macro_export]
macro_rules! error {
    ($module:expr, $($arg:tt)*) => { $crate::log($crate::Level::Error, $module, format_args!($($arg)*)) };
}
#[macro_export]
macro_rules! warn {
    ($module:expr, $($arg:tt)*) => { $crate::log($crate::Level::Warn, $module, format_args!($($arg)*)) };
}
#[macro_export]
macro_rules! info {
    ($module:expr, $($arg:tt)*) => { $crate::log($crate::Level::Info, $module, format_args!($($arg)*)) };
}
#[macro_export]
macro_rules! debug {
    ($module:expr, $($arg:tt)*) => { $crate::log($crate::Level::Debug, $module, format_args!($($arg)*)) };
}
#[macro_export]
macro_rules! trace {
    ($module:expr, $($arg:tt)*) => { $crate::log($crate::Level::Trace, $module, format_args!($($arg)*)) };
}

// =====================================================================
// 测试 (实例级, 不碰全局单例避免测试间干扰)
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_and_filter() {
        assert_eq!(Level::parse("INFO"), Some(Level::Info));
        assert_eq!(Level::parse("bogus"), None);
        // enabled_at 纯函数
        assert!(!enabled_at(0, Level::Error)); // 未初始化全过滤
        assert!(enabled_at(Level::Info as u8, Level::Warn));
        assert!(!enabled_at(Level::Info as u8, Level::Debug));
    }

    #[test]
    fn queue_backpressure_drops_low_levels_keeps_urgent() {
        let q = LogQueue::with_capacity(4);
        for i in 0..10 {
            q.push(Level::Info, format!("line {i}\n"));
        }
        // 容量 4, 后 6 条 Info 被丢弃
        assert_eq!(q.dropped(), 6);
        // 腾出空间后 Warn 必须成功 (自旋重试路径)
        let mut acc = Vec::new();
        q.drain_into(&mut acc);
        q.push(Level::Warn, "urgent\n".to_string());
        let mut acc2 = Vec::new();
        assert_eq!(q.drain_into(&mut acc2), 1);
    }

    fn start_test_backend(
        buffer_bytes: usize,
        interval_ms: u64,
    ) -> (tempfile::TempDir, PathBuf, Arc<LogQueue>, Backend) {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("test.log");
        let queue = Arc::new(LogQueue::with_capacity(1024));
        let backend = Backend::start(
            BackendConfig {
                file: file.clone(),
                buffer_bytes,
                flush_interval: Duration::from_millis(interval_ms),
            },
            queue.clone(),
        )
        .unwrap();
        (tmp, file, queue, backend)
    }

    #[test]
    fn size_threshold_triggers_flush_before_interval() {
        // interval 拉到 10s, 只可能靠量阈值触发
        let (_tmp, file, queue, backend) = start_test_backend(64, 10_000);
        for i in 0..20 {
            queue.push(Level::Info, format!("size-trigger line {i}\n"));
        }
        // 等 log 线程完成量触发落盘 (远小于 10s interval)
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let len = std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
            if len > 0 || Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.contains("size-trigger line 0"), "got: {content:?}");
        backend.shutdown();
    }

    #[test]
    fn time_threshold_triggers_flush() {
        // buffer 拉到 1MB, 只可能靠时间阈值触发
        let (_tmp, file, queue, backend) = start_test_backend(1 << 20, 100);
        queue.push(Level::Info, "time-trigger\n".to_string());
        std::thread::sleep(Duration::from_millis(500));
        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.contains("time-trigger"), "got: {content:?}");
        backend.shutdown();
    }

    #[test]
    fn shutdown_flushes_everything() {
        let (_tmp, file, queue, backend) = start_test_backend(1 << 20, 10_000);
        for i in 0..100 {
            queue.push(Level::Info, format!("final {i}\n"));
        }
        // 量/时间都不会触发 — 只有 shutdown 落盘
        backend.shutdown();
        let content = std::fs::read_to_string(&file).unwrap();
        let count = content.lines().filter(|l| l.starts_with("final ")).count();
        assert_eq!(count, 100);
    }

    #[test]
    fn append_across_backend_restarts() {
        let (_tmp, file, queue, backend) = start_test_backend(1 << 20, 10_000);
        queue.push(Level::Info, "first-run\n".to_string());
        backend.shutdown();
        // 重启 backend, offset 应接在文件尾
        let queue2 = Arc::new(LogQueue::with_capacity(64));
        let backend2 = Backend::start(
            BackendConfig {
                file: file.clone(),
                buffer_bytes: 1 << 20,
                flush_interval: Duration::from_millis(10_000),
            },
            queue2.clone(),
        )
        .unwrap();
        queue2.push(Level::Info, "second-run\n".to_string());
        backend2.shutdown();
        let content = std::fs::read_to_string(&file).unwrap();
        assert!(content.contains("first-run"));
        assert!(content.contains("second-run"));
    }
}
