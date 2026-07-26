//!
//! 编译时常驻 (零开销单 atomic load), 由 `NLOG_PROBE=1` 环境变量开启打点.
//! 用 16-bucket 粗粒度 histogram 定位长尾来源, 找到瓶颈后移除.

use std::sync::atomic::{AtomicU64, Ordering};

/// 16 个对数分布 bucket (单位: ns). 覆盖 1μs..~1s 范围.
const BUCKETS: [u64; 16] = [
    1_000,
    2_000,
    5_000,
    10_000,
    20_000,
    50_000,
    100_000,
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    20_000_000,
    50_000_000,
    u64::MAX,
];

/// 整体延迟 histogram (count + sum ns + 每 bucket count).
#[derive(Default)]
pub struct Histogram {
    pub count: AtomicU64,
    pub sum_ns: AtomicU64,
    pub buckets: [AtomicU64; 16],
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; 16],
        }
    }

    pub fn record(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        let idx = bucket_index(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// 输出 ASCII 直方图 (10ms+ 单独标注为长尾).
    pub fn dump(&self, label: &str) -> String {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return format!("[{label}] (no samples)\n");
        }
        let mut s = format!("[{label}] samples={count} avg_ns={}\n", self.sum_ns.load(Ordering::Relaxed) / count);
        for (i, &b) in BUCKETS.iter().enumerate() {
            let c = self.buckets[i].load(Ordering::Relaxed);
            if c > 0 {
                let pct = c * 100 / count;
                let flag = if b >= 10_000_000 { " <-- TAIL" } else { "" };
                s.push_str(&format!("  [{:>10}ns] {:>5} ({pct:>2}%){flag}\n", b, c));
            }
        }
        s
    }
}

#[inline]
fn bucket_index(ns: u64) -> usize {
    let mut i = 0;
    while i < 15 && ns > BUCKETS[i] {
        i += 1;
    }
    i
}

/// 全局探针集 (静态, 每个 worker/shard 共享计数).
pub static PROBE: Probe = Probe::new();

pub struct Probe {
    enabled: AtomicU64,
    /// ⭐ 怀疑点 1: swap_backpressure_sync_ns — chunk 满 swap 时, MAX_INFLIGHT 超限退化同步写的耗时
    pub backpressure_sync_ns: Histogram,
    /// ⭐ 怀疑点 2: drive_async_flush_round_ns — 每次 drive_async_flush (含 maybe_periodic_flush) 总耗时
    pub drive_round_ns: Histogram,
    /// ⭐ 怀疑点 3: poll_wait_ns — 主循环 poll() 睡眠到醒来间隔 (无数据时的等待开销)
    pub poll_wait_ns: Histogram,
    /// ⭐ 怀疑点 4: drive_until_idle_ns — drive_async_flush 内 drive_until_idle(256) 单次耗时
    pub drive_until_idle_ns: Histogram,
    /// ⭐ 怀疑点 5: sync_write_coroutine_ns — 单个落盘协程 (write+fsync) 完成的耗时
    pub sync_write_coroutine_ns: Histogram,
    /// ⭐ 怀疑点 6: block_on_io_ns — block_on_io 单次调用耗时 (凡是用到的位置)
    pub block_on_io_ns: Histogram,
    pub backpressure_fallbacks: AtomicU64,
    pub in_flight_peak: AtomicU64,
}

impl Default for Probe {
    fn default() -> Self {
        Self::new()
    }
}

impl Probe {
    pub const fn new() -> Self {
        Self {
            enabled: AtomicU64::new(0),
            backpressure_sync_ns: Histogram::new(),
            drive_round_ns: Histogram::new(),
            poll_wait_ns: Histogram::new(),
            drive_until_idle_ns: Histogram::new(),
            sync_write_coroutine_ns: Histogram::new(),
            block_on_io_ns: Histogram::new(),
            backpressure_fallbacks: AtomicU64::new(0),
            in_flight_peak: AtomicU64::new(0),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(1, Ordering::Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire) == 1
    }

    pub fn dump_all(&self) -> String {
        if !self.is_enabled() {
            return String::from("(probes disabled, set NLOG_PROBE=1 to enable)\n");
        }
        let mut s = String::new();
        s.push_str(&self.backpressure_sync_ns.dump("backpressure_sync_write_ns"));
        s.push_str(&self.drive_round_ns.dump("drive_async_flush_round_ns"));
        s.push_str(&self.poll_wait_ns.dump("poll_wait_ns"));
        s.push_str(&self.drive_until_idle_ns.dump("drive_until_idle_ns"));
        s.push_str(&self.sync_write_coroutine_ns.dump("flush_coroutine_total_ns"));
        s.push_str(&self.block_on_io_ns.dump("block_on_io_ns"));
        s.push_str(&format!(
            "backpressure_fallbacks={}\nin_flight_peak={}\n",
            self.backpressure_fallbacks.load(Ordering::Relaxed),
            self.in_flight_peak.load(Ordering::Relaxed),
        ));
        s
    }
}