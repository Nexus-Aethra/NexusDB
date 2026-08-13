# NexusDB 设计文档

> 定位演进: 2026-07-25 从“嵌入式 KV 引擎”定位演进为“**独立数据库服务**”，新增多协议门面 (Redis RESP2 ✅ / PostgreSQL / MySQL / Mongo 待实施)、双 server (Binary + RESP)、异步 chunk 落盘 + 有界背压. 详见 `CHANGELOG.md` F33-F41.

## 一、目标与定位

**NexusDB** 是一个面向写密集、低延迟、高并发的独立单机数据库服务（原定位: 嵌入式 KV 数据库引擎, 2026-07-25 演进）。

核心设计哲学：**Share-Nothing + Per-Core Thread + io_uring + Rust 无栈协程**，用软件架构手段消除锁，而不是靠更细的锁来降低冲突。

参考业界成熟方案：
- 网络/调度层：[Seastar](https://github.com/scylladb/seastar)（ScyllaDB 内核）、[glommio](https://github.com/DataDog/glommio)、[monoio](https://github.com/bytedance/monoio)
- 存储层：**LCB-Tree**（Log-Chunked B+Tree，学术原型 Bε-Tree / LSB-Tree 的工程化变种，工业参考 SQLite WAL、LMDB 的 COW + 映射表思路）
- 多协议接入：长期目标为 Redis (已实现) / PostgreSQL / MySQL / Mongo 共存的数据互联服务 (统一记录编码 + value type tag)

## 二、架构总览

```
                        Client / 上层调用方
                              │
                              ▼
                       ┌──────────────┐
                       │  Hash Router │  key → shard_id = fnv(key) % N
                       └──────────────┘
                              │
        ┌─────────┬─────────┬─┴───────┬─────────┐
        ▼         ▼         ▼         ▼         ▼
    ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐
    │Shard 0│ │Shard 1│ │Shard 2│ │Shard 3│ │Shard N│   ← 1 线程 = 1 分片
    │       │ │       │ │       │ │       │ │       │   ← 线程内多协程
    │ ┌───┐ │ │ ┌───┐ │ │ ┌───┐ │ │ ┌───┐ │ │ ┌───┐ │
    │ │Co │ │ │ │Co │ │ │ │Co │ │ │ │Co │ │ │ │Co │ │   ← monoio 协程
    │ └───┘ │ │ └───┘ │ │ └───┘ │ │ └───┘ │ │ └───┘ │
    │ io_ring│ │io_ring│ │io_ring│ │io_ring│ │io_ring│   ← 每线程独立 SQ
    │  MemTable│ │ ... │ │ ... │ │ ... │ │ ... │
    │  WAL    │ │     │ │     │ │     │ │     │
    └───────┘ └───────┘ └───────┘ └───────┘ └───────┘
```

**关键不变量**：一个 key 永远只在同一个 shard/线程/协程中处理 ⇒ 同一 key 的读写天然串行，**无需任何互斥锁**。

## 三、核心机制详解

### 3.1 Hash 路由

```rust
fn shard_of(key: &[u8], n: usize) -> usize {
    let h = fnv1a_64(key);          // 64 位 FNV-1a，分布好、极快
    (h as usize) % n
}
```

- `n` 通常 = CPU 物理核数（如 `num_cpus::get_physical()`）。
- 路由决策在调用方线程执行（微秒级），不跨线程同步。
- 不做一致性哈希：因为单进程，分片数量固定即可，重新分片走离线迁移。

### 3.2 Per-Core Shard Thread

每个 shard 跑一个独立 OS 线程 + 一个独立 `io_uring` 实例 + 一组协程。

```rust
struct Shard {
    id: usize,
    ring: IoUring,                  // monoio Driver 底层
    rt: Runtime,                    // 每线程一个 tokio-style runtime
    memtable: MemTable,             // 跳表 / SkipMap
    wal: Wal,                       // 顺序写日志
    pending: TaskQueue,             // 跨 shard 任务通道
}
```

线程模型：
- **无锁**：线程间不共享任何状态，跨 shard 通信走 SPSC/MPMC 通道。
- **CPU 亲和**：使用 `sched_setaffinity` 把每个 shard 线程绑到一个物理核，避免 cache 抖动。
- **无 GIL 等价**：Rust `Send`/`Sync` 边界天然保证 `Shard` 不跨线程。

### 3.3 协程 + io_uring 调度

采用 [monoio](https://github.com/bytedance/monoio) 提供的协程原语（基于 io_uring 的 epoll 替代）。

```rust
async fn get(shard: &Shard, key: &[u8]) -> Result<Vec<u8>> {
    // 1. 先查内存表（无 IO）
    if let Some(v) = shard.memtable.get(key) {
        return Ok(v);
    }
    // 2. 未命中 → 发起异步读
    let offset = shard.bloom_maybe_contains(key);   // 减少无效 IO
    if !offset {
        return Ok(None);
    }
    let buf = shard.read_sst_async(key).await?;    // ← io_uring, 不阻塞线程
    Ok(buf)
}
```

**为什么不会出现"IO 等待浪费线程"**：
- 协程 `await` 时，runtime 把当前任务挂起，把执行权让给同一线程上的其他就绪协程。
- io_uring 的 completion queue 用 `io_uring_wait_cqe` 唤醒被挂起的协程。
- 一个线程可以同时承载上千个协程，单核吞吐不输传统多线程 + epoll。

### 3.4 Per-Shard 调度器：任务批量 + 协程池 + io_uring

#### 3.4.1 总体架构

每个 shard 线程内部署一个**自研调度器**，由三层组成：

```
┌────────────────────────────────────────────────────────────────┐
│  Layer 1: Task Queue（MPSC，跨线程收任务）                      │
│   - Hash Router 把外部 put/get 投递到对应 shard 的队列         │
│   - 队列容量 = 16384，超过时 backpressure                       │
└──────────────┬─────────────────────────────────────────────────┘
               │ 批量 drain 最多 200 条
               ▼
┌────────────────────────────────────────────────────────────────┐
│  Layer 2: Batch Scheduler（每轮调度 200 个 task）              │
│   - 把 task 封装为 Future → 推入协程池                          │
│   - 协程池容量固定 = 1024（复用，不动态扩缩）                   │
│   - round-robin 分配协程句柄                                    │
└──────────────┬─────────────────────────────────────────────────┘
               │ Future::poll
               ▼
┌────────────────────────────────────────────────────────────────┐
│  Layer 3: io_uring Driver（monoio 提供）                       │
│   - 所有异步 IO（read/write/fsync）走 io_uring SQ/CQ            │
│   - 协程 .await 挂起时注册到 driver，等待 CQE                   │
│   - 唤醒后 future 重新 poll，IO 完成即返回                      │
└────────────────────────────────────────────────────────────────┘
```

#### 3.4.2 核心数据结构

```rust
/// 单个 shard 的调度器
pub struct ShardScheduler {
    /// 任务入口（MPSC 队列，跨线程接收）
    task_queue: ArrayQueue<Task>,                    // 16384 容量
    /// 协程池（固定大小，复用协程句柄避免动态创建开销）
    coroutine_pool: Vec<CoroutineSlot>,              // 1024 个 slot
    /// 当前 ready 协程队列（待 poll）
    ready_queue: VecDeque<CoroutineHandle>,
    /// 等待 io_uring 唤醒的协程注册表（cqe_data → handle）
    pending_io: HashMap<u64, CoroutineHandle>,
    /// io_uring 实例（每个 shard 一个）
    ring: IoUring,
}

/// 协程池的一个 slot
struct CoroutineSlot {
    /// 当前正在跑的 Future（None = 空闲）
    future: Option<BoxFuture<'static, ()>>,
    /// 协程句柄（用于被 io_uring CQE 唤醒）
    handle: CoroutineHandle,
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
```

#### 3.4.3 调度循环

```rust
impl ShardScheduler {
    /// 调度器主循环，永不退出
    pub fn run(mut self) {
        loop {
            // === Phase 1: 从 task_queue 批量取任务 ===
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {                  // BATCH_SIZE = 200
                if let Some(task) = self.task_queue.pop() {
                    batch.push(task);
                } else {
                    break;
                }
            }

            // === Phase 2: 把 task 封装为 Future，分配协程 ===
            for task in batch {
                let future = self.spawn_task(task);   // 业务 Future
                let slot = self.acquire_slot();       // 从池里拿一个
                slot.future = Some(future);
                self.ready_queue.push_back(slot.handle);
            }

            // === Phase 3: poll 所有 ready 协程 ===
            let mut made_progress = true;
            while made_progress {
                made_progress = false;
                let ready = std::mem::take(&mut self.ready_queue);
                for handle in ready {
                    let slot = &mut self.coroutine_pool[handle.id];
                    if let Some(fut) = slot.future.as_mut() {
                        match fut.as_mut().poll(...) {
                            Poll::Ready(()) => {
                                // 协程完成，释放 slot
                                slot.future = None;
                                self.free_slot(handle);
                            }
                            Poll::Pending => {
                                // 协程挂起（通常是因为 .await io_uring）
                                // 已经在 fut 内部注册到 pending_io
                                // 不会回到 ready_queue
                                made_progress = false;
                            }
                        }
                    }
                }
                // 处理本轮 poll 中重新就绪的协程
                if !self.ready_queue.is_empty() {
                    made_progress = true;
                }
            }

            // === Phase 4: 提交所有未提交的 SQEs + 等待 CQE ===
            self.ring.submit_and_wait(1, Duration::from_millis(0))?;
            // 完成事件由 io_uring 唤醒 → 把对应 handle 推回 ready_queue
            // （这一步在另一个线程或 epoll 回调里完成，见 3.4.5）
        }
    }
}
```

#### 3.4.4 任务封装（task → future）

```rust
fn spawn_task(&self, task: Task) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        match task {
            Task::Put { key, value, resp } => {
                let pid = self.write_page_async(&key, &value).await;
                resp.send(pid);    // 通过 oneshot 回给调用方
            }
            Task::Get { key, resp } => {
                let val = self.read_page_async(&key).await;
                resp.send(val);
            }
            Task::Commit { resp } => {
                self.flush_chunk().await;     // 触发 chunk flush
                resp.send(());
            }
            Task::Compact { resp } => {
                self.run_compaction().await;
                resp.send(());
            }
        }
    })
}
```

#### 3.4.5 io_uring 唤醒机制（关键）

**问题**：协程 `.await` 一个 io_uring 操作时，怎么被 CQE 唤醒？

```rust
/// Future 实现：等待 io_uring read 完成
struct IoUringRead {
    ring: Rc<IoUring>,
    fd: RawFd,
    buf: *mut u8,
    len: usize,
    offset: u64,
    /// 提交 SQ 时拿到的 user_data，作为 CQE 回来的"身份证"
    user_data: u64,
}

impl Future for IoUringRead {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 1. 检查 CQE 是否已经到了（poll 时机可能晚于 CQE）
        if let Some(cqe) = self.ring.peek_cqe_by_user_data(self.user_data) {
            return Poll::Ready(Ok(cqe.result));
        }

        // 2. 注册 waker：未来 CQE 到达时用这个 waker 唤醒协程
        self.ring.register_waker(self.user_data, cx.waker().clone());

        // 3. 如果 SQ 还没提交，现在提交
        if !self.ring.submitted(self.user_data) {
            let sqe = self.ring.get_sqe();
            unsafe { io_uring_prep_read(sqe, self.fd, self.buf, self.len, self.offset) };
            io_uring_sqe_set_data(sqe, self.user_data);
            self.ring.submit();
        }

        Poll::Pending   // 协程挂起
    }
}
```

**CQE 回调路径**（由 io_uring 内核触发 + monoio 的 reactor）：

```
内核完成 IO
    ↓ 写入 CQ ring
io_uring CQE 中断（或 epoll 唤醒，fallback）
    ↓
monoio reactor.run_once()
    ↓ 遍历 CQE，取 user_data
reactor.wakers[user_data].wake()
    ↓ 把 waker 注入协程调度器
scheduler.ready_queue.push(handle)
    ↓ 下一轮 Phase 3 poll 该协程
协程从 .await 处恢复，拿到 IO 结果
```

#### 3.4.6 批量提交优化

**为什么"最多 200 个 tasks 一批"**：
- 太小（如 1）：每次 round-trip 调度器 → 业务 → io_uring 开销占比高。
- 太大（如 10000）：长尾协程饥饿；CQE 回来时可能积压太多 waker。
- **200 是经验值**：大约填满 io_uring SQ 的一半（默认 SQ=1024），同时保证低延迟。

```rust
const BATCH_SIZE: usize = 200;
const COROUTINE_POOL_SIZE: usize = 1024;   // = 5 × BATCH_SIZE，留 buffer
```

#### 3.4.7 协程池复用策略

```rust
fn acquire_slot(&mut self) -> &mut CoroutineSlot {
    // 1. 优先从空闲链表拿
    if let Some(idx) = self.free_slots.pop_front() {
        return &mut self.coroutine_pool[idx];
    }
    // 2. 没有空闲 → 复用最旧的（round-robin）
    let idx = self.rr_cursor;
    self.rr_cursor = (self.rr_cursor + 1) % COROUTINE_POOL_SIZE;
    // ⚠️ 如果该 slot 仍有 future 在跑，需要先 abort（cancel io_uring）
    &mut self.coroutine_pool[idx]
}
```

**关键不变量**：协程池大小（1024）必须 ≥ 一个 batch 中最大 IO 挂起数。
否则会出现"旧协程还在等 IO，新协程被强制 abort 抢占 slot"的性能塌方。

#### 3.4.8 与"无锁"的协同

| 资源 | 是否需要锁 | 原因 |
|---|---|---|
| `task_queue` (MPSC) | ❌ | 跨线程唯一同步点，用无锁 MPMC 队列 |
| `coroutine_pool[i]` | ❌ | 每个 slot 只被本线程访问 |
| `ready_queue` | ❌ | 线程本地 VecDeque |
| `pending_io` map | ❌（**读多写少用 DashMap 兜底**） | 同一 user_data 不会并发注册 |
| io_uring SQ/CQ | ❌ | 线程独占一个 ring |

唯一需要注意的是 **跨 shard 通信**：用 `crossbeam-channel` 或 `monoio::sync::channel`（基于 sharded mailbox），仍然零锁。

#### 3.4.9 单线程事件循环伪代码

```rust
fn shard_main(shard: Shard) {
    let mut scheduler = ShardScheduler::new(shard);
    scheduler.run();   // 永不返回
}

// 调用方（任意线程）
fn client_put(db: &NexusDB, key: &[u8], value: &[u8]) -> impl Future<Output = ()> {
    let shard_id = shard_of(key, db.shard_count());
    let (tx, rx) = oneshot::channel();
    db.shards[shard_id].task_queue.push(Task::Put {
        key: key.to_vec(),
        value: value.to_vec(),
        resp: tx,
    });
    async move { rx.await.unwrap() }
}
```

调用方线程**完全不阻塞**，只是把 Task 推进对应 shard 的队列就返回。  
shard 线程在下一轮调度循环自动 pick up → 协程化 → 跑业务 → 走 io_uring → 回写。

### 3.5 无锁并发的关键

| 路径 | 实现 | 是否需要锁 |
|---|---|---|
| 同 key 读写 | 同 shard 串行 | ❌ |
| 跨 shard 事务 | 2PC + 协程 channel | ❌（仅消息传递） |
| MemTable 并发 | SkipMap（无锁）或 sharded map | ❌ |
| WAL 写入 | 单线程顺序 append | ❌ |
| IO 提交 | 线程内 SQ/CQ | ❌ |

唯一的同步点是 **跨 shard 通信**，使用 `crossbeam` 或 `monoio` 的 MPMC channel。

## 四、存储引擎：LCB-Tree

> **命名说明**：LCB-Tree = **L**og-**C**hunked **B+Tree**。
> 与学术界的 LSB-Tree（Log-Structured B-Tree, Wisconsin 2007）一脉相承，
> 区别在于用 **Chunk（10MB block → 1MB chunk）** 作为 IO 与持久化的固定边界，
> 并把映射表（vpid→pid）本身也用同一结构存储，全系统只有一种数据结构。

放弃纯 LSM 结构（B+Tree 查询快但随机写慢、LSM 写快但读放大严重），
采用 **B+Tree 逻辑视图 + Log 物理追加** 的混合架构：**LCB-Tree**。

### 4.1 设计动机

| 结构 | 写 | 读 | 适合场景 |
|---|---|---|---|
| B+Tree（原地更新） | 慢（随机写 + 页分裂） | 快 | 读多写少 |
| LSM-Tree | 快（顺序追加） | 慢（多层级查找 + Compaction） | 写多读少 |
| **LCB-Tree（本设计）** | **快**（chunk 追加 + COW） | **快**（一次 pid 解析 + 顺序页读） | 通用 |

核心洞察：**B+Tree 的"页分裂/随机写"不是 B+Tree 本身的问题，而是"原地更新"的问题**。
如果我们不改原地，而是把新版本追加到 log、用 vpid→pid 映射指向最新版本，就同时拿到了两种结构的优势。

### 4.2 物理布局

#### 4.2.1 三层地址空间

```
┌─────────────────────────────────────────────────────────┐
│  逻辑层：vpid（Virtual Page ID）                         │
│   - 由 B+Tree 内部维护，对外完全隐藏                     │
│   - vpid 从 0 自增，永不复用（COW 友好）                 │
└──────────────────────┬──────────────────────────────────┘
                       │ vpid_to_pid[vpid] = (file_id, offset)
                       ▼
┌─────────────────────────────────────────────────────────┐
│  物理层：pid（Physical Page Location）                   │
│   - file_id: 数据文件序号                               │
│   - offset:  文件内字节偏移                              │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  设备层：Block（10MB）→ Chunk（1MB）                     │
│   - 数据文件被切分为定长 block（10MB）                   │
│   - 每个 block 内切 10 个 chunk（1MB）                   │
│   - chunk 是 IO 与持久化的最小单位                        │
└─────────────────────────────────────────────────────────┘
```

#### 4.2.2 文件布局

```
data/
├── 000001.block       # 10MB，写满后只读
│   ├── chunk-0 (1MB)
│   ├── chunk-1 (1MB)
│   ├── ...
│   └── chunk-9 (1MB)
├── 000002.block
├── 000003.block       # 当前活跃 block，新 chunk 只写这里
└── ...
```

- **Block 满 10MB 后冻结**，类似 LSM 的 SST 不可变语义，方便后台合并/回收。
- **Chunk 是写盘与映射的最小单位**，未满 1MB 的 chunk 不单独刷盘。

#### 4.2.3 Page 与 Item 设计

LCB-Tree 的 Page 是逻辑视图的最小单位，所有 Page 固定大小 **16 KiB**。

##### 4.2.3.1 三种 Page 类型

| Page 类型 | 用途 | 是否存数据 |
|---|---|---|
| **Meta Page** | 描述 B+Tree 元信息（root vpid、节点数、max_vpid） | ❌ |
| **Internal Page** | B+Tree 内部节点，存 `[separator_key, child_vpid]` | ❌ |
| **Leaf Page** | B+Tree 叶子节点，存 `[key, value]` | ✅ |

每个 page 头部用 1 字节 type 字段区分：

```rust
#[repr(u8)]
pub enum PageType {
    Meta     = 1,
    Internal = 2,
    Leaf     = 3,
}
```

##### 4.2.3.2 Page 总体布局

```
┌──────────────────────────────────────────────────────────────┐
│  Page Header (固定 40 字节)                                   │
│   magic(4) │ type(1) │ flags(1) │ key_count(2) │ free_off(2)│
│   prefix_overlap(2) │ checksum(8) │ version(4) │ vpid(8)    │
│   chunk_log_off(2) │ reserved(6)                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Item Area (key_count 个 item，从前往后增长)                  │
│   [item 0] [item 1] ... [item N-1]                          │
│                                                              │
│   ── (free space) ──                                         │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  Checkpoint Array (从尾部往前长)                              │
│   [checkpoint M-1] [checkpoint M-2] ... [checkpoint 0]       │
├──────────────────────────────────────────────────────────────┤
│  Checkpoint Header (8 字节)                                  │
│   checkpoint_count(2) │ min_per_cp(2) │ max_per_cp(2) │ ... │
└──────────────────────────────────────────────────────────────┘
```

- **Item Area** 从 page 起始往后长。
- **Checkpoint Array** 从 page 末尾往前长。
- 两者向中间靠拢，相遇时触发 **page split（COW）**。

##### 4.2.3.3 Item 编码（前缀压缩）

每个 item 是变长字节串，编码方式：

```rust
/// Item 头（固定 4 字节）
#[repr(C)]
pub struct ItemHeader {
    /// 与上一个 key 共享的前缀长度（leaf 内第一个 item 时为 0）
    pub shared_prefix_len: u16,
    /// key 不重合部分的长度
    pub key_unshared_len:  u16,
    // 紧跟着是：
    //   - key_unshared 部分（key_unshared_len 字节）
    //   - value 长度（varint，1~5 字节）
    //   - value bytes
    //   - 子节点 vpid（仅 InternalPage 有，8 字节）
}
```

**完整 Item 结构**：

```
┌─────────────────────┬─────────────┬────────┬──────────┬────────┐
│ hdr (4B)            │ key_unique  │ vint  │ value    │ child  │
│ shared_prefix_len   │ (变长)      │ vleng │ (变长)   │ vpid   │
│ key_unshared_len    │             │       │          │ (仅Int)│
└─────────────────────┴─────────────┴────────┴──────────┴────────┘
```

**Item 类型**：

| ItemKind | 是否存 value | 是否存 child vpid | 用于 |
|---|---|---|---|
| `LeafItem` | ✅ | ❌ | Leaf Page |
| `InternalItem` | ❌（separator_key） | ✅ | Internal Page |

**还原完整 key 的算法**：

```rust
fn reconstruct_key(items: &[Item], idx: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);
    // 用 shared_prefix_len 沿父链累加
    for i in 0..=idx {
        let it = &items[i];
        key.truncate(it.shared_prefix_len as usize);  // 截断到共享前缀
        key.extend_from_slice(it.key_unshared());
    }
    key
}
```

**prefix 长度上限** = 当前 item 之前的累计 key 长度，按 `u16` 存储足够（最大 65535）。

**段首 item 特殊规则**：每个 checkpoint 段（segment）的第一个 item 必须令 `shared_prefix_len = 0`（即完整存储 key）。原因是 checkpoint 数组中记录了该段的 `first_item_off`，查找时可以直接跳转到该偏移量解码 item，此时没有上一 item 的 key 上下文可以用于前缀恢复。段内后续 item 仍正常使用前缀压缩。

##### 4.2.3.4 检查点数组（Checkpoint Array）

由于 item 是变长的，**无法用固定偏移做页内二分**。
解决方案：在 page 末尾维护一个**稀疏检查点数组**。

```rust
#[repr(C)]
pub struct Checkpoint {
    /// 该段内第一个 item 的逻辑 index
    pub start_item_idx: u16,   // 0 .. key_count-1
    /// 本段 item 数（用于精确限定段范围，不越过段边界）
    pub item_count:     u16,   // 8 ~ 32
    /// 该段内第一个 item 在 page 内的字节偏移
    pub first_item_off: u16,
    /// 该段起始 item 的完整 key 前 N 字节（用于粗排序判断）
    /// N 固定 = 10 字节（不命中时回退到段内二分）
    pub prefix_sample: [u8; 10],
}

#[repr(C)]
pub struct CheckpointHeader {
    pub checkpoint_count: u16,  // 检查点个数
    pub min_per_cp:      u16,   // 每个检查点覆盖 item 下限（默认 8）
    pub max_per_cp:      u16,   // 每个检查点覆盖 item 上限（默认 32）
    pub flags:           u16,   // 预留标志位
}
```

**约束**：
- 每个 checkpoint 覆盖 **8 ~ 32 个 item**。
- 若插入导致某段超过 32 个 item → 对半拆分为两段（前段和后段各约一半），新段首 item 以 `shared_prefix_len = 0` 重新编码（保证直接跳转到 `first_item_off` 时可独立解码）。
- 若删除导致某段少于 8 个 item → 与相邻段合并。

##### 4.2.3.5 页内查找算法

```
page_search(key):
  1. 读取 checkpoint 数组（O(M)，M ≤ key_count / 16）
  2. 广域二分：比较 key 与 checkpoint.prefix_sample
     - 命中某段 [start, end) 后，定位到具体段
  3. 段内二分：在该段 item 上执行常规二分
     - 由于段最多 32 个 item，最多 5 次比较
  4. 比较时调用 reconstruct_key(item) 还原真实 key
```

**总查找成本**：`O(log M + log 32) ≈ O(log key_count)`

##### 4.2.3.6 Leaf Page 与 Internal Page 的区别

| 字段 | Leaf Page | Internal Page |
|---|---|---|
| Item Kind | `LeafItem` | `InternalItem` |
| Item 存 value | ✅ | ❌ |
| Item 存 child_vpid | ❌ | ✅（8 字节） |
| 紧凑度目标 | 极高（数据本体） | 中等（仅导航） |
| prefix 压缩 | ✅ | ✅ |
| checkpoint | ✅ | ✅ |

##### 4.2.3.7 页面物理布局示例

```
Leaf Page (16 KiB)
┌─────────────────────────────────────────────────────────┐
│ Header: type=Leaf, key_count=180, free_off=14820,        │
│          vpid=0x0000000000002A3C, version=7               │
├─────────────────────────────────────────────────────────┤
│ item0 (k=20B, v=128B)                                   │
│ item1 (k=8B,  v=64B)   ← 与 item0 共享 12B 前缀        │
│ item2 (k=12B, v=256B)                                   │
│ ...                                                     │
├─────────────────────────────────────────────────────────┤
│ (free space ≈ 800B)                                     │
├─────────────────────────────────────────────────────────┤
│ cp[5] {idx=160, off=14700, prefix=0xAB12...}            │
│ cp[4] {idx=128, off=12500, prefix=0x7F3E...}            │
│ cp[3] {idx=96,  off=10200, prefix=0x...}                │
│ cp[2] {idx=64,  off=8000,  prefix=0x...}                │
│ cp[1] {idx=32,  off=4500,  prefix=0x...}                │
│ cp[0] {idx=0,   off=128,   prefix=0x...}                │
├─────────────────────────────────────────────────────────┤
│ CPHeader: count=6, min=16, max=32                       │
└─────────────────────────────────────────────────────────┘
```

##### 4.2.3.8 Page Header 的 vpid 字段

**Header 中显式记录本页的 vpid（8 字节）**，承担三个作用：

| 场景 | 用途 |
|---|---|
| **崩溃恢复** | 扫描磁盘时，用 header.vpid 与 vpid_to_pid 的映射交叉校验，过滤掉半写的脏 page |
| **后台 compaction** | compaction 协程读取 block 时，无需先去查 vpid_to_pid 就知道这是哪个 vpid 的新版本 |
| **快速定位 stale** | 多个版本的 page 共存时，header.vpid 与 PidLocation.file_id 比对即可识别 stale COW 副本 |

> 与 4.2.3.2 中的 `chunk_log_off` 配合：
> - `vpid`：这是哪个虚拟页
> - `chunk_log_off`：该 vpid 的变更日志在所在 chunk 内的偏移（用于重放）

##### 4.2.3.9 Page Footer（校验区）

page 末尾预留 16 字节 footer（与 checkpoint 数组分离）：

```
┌──────────┬───────────┬──────────┐
│ magic(4) │ version(4)│ checksum(8)│  ← 共 16B
│ "LCBP"   │           │ (xxhash64)│
└──────────┴───────────┴──────────┘
```

- `magic "LCBP"` 区分 page 与 checkpoint 数组（防止读取越界误识别）。
- `version` 与 header 中 version 冗余比对。
- `checksum` 用 xxhash64（比 CRC32 快 5x，足够检测位错误）。
- 崩溃恢复时：`magic + checksum` 双重过滤脏数据；
  真正的 vpid 校验由 **header.vpid** 承担（见 4.2.3.8）。

### 4.3 寻址与 vpid→pid 映射

#### 4.3.1 Page 大小约定

**page_size = 16 KiB**（固定，可编译期常量）。

理由：
- NVMe 物理页 4KB，16KB = 4 个物理页，单次 IO 对齐 4KB 边界友好。
- B+Tree 一个 leaf page 可容纳更多 KV（≈ 256 个 64 字节条目），树高通常 ≤ 3。
- chunk = 1MB / 16KB = **64 个 page**（10MB block = 640 page）。

#### 4.3.2 PidLocation 编码

```rust
/// 单条 vpid→pid 映射，固定 8 字节
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PidLocation {
    pub file_id:   u32,   // 哪个 .block 文件（≤ 2^32，足够）
    pub chunk_idx: u8,    // block 内第几个 chunk（0..=9）
    pub offset:    u16,   // chunk 内 page 偏移（0..=63，因为 1MB/16KB=64）
    pub flags:     u8,    // bit0: alive, bit1: in_txn, bit2-7: reserved
}
```

> `length` 不必存进 PidLocation：因为 page 大小固定 16KB。
> `offset` 用 u16 而不是 u32，因为 chunk 最多 64 个 page，16 bit 足够。

#### 4.3.3 page.mate 文件（映射表）

**核心思想**：把映射表抽出来作为一个**纯数组文件**，下标即 vpid，启动时载入内存。

```
data/
├── page.mate        # 映射表文件，下标 = vpid
│   slot[0]  → PidLocation(8B)
│   slot[1]  → PidLocation(8B)
│   slot[2]  → PidLocation(8B)
│   ...
├── 000001.block
├── 000002.block
└── ...
```

**数组规模估算**：
- 8 字节 × 1M vpid = 8 MB（数组可放内存）
- 8 字节 × 1G vpid = 8 GB（必须 mmap，不能整段加载）

#### 4.3.4 Meta 内存缓存：两层数组树 + 预读窗口

放弃"段式 mmap 全量加载"，改用 **固定容量的两层数组树**：

```
┌─────────────────────────────────────────────────────────────┐
│  Level-1: Data Array（10MB，1.25M 个 vpid slot）             │
│   - 固定分配，常驻内存，永不释放                              │
│   - 每个 slot 8B = PidLocation                                │
│   - 这才是真正的"映射表本体"                                 │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ 每个 Level-2 entry 指向一段 1MB
                              │ (128K 个 slot)
┌─────────────────────────────────────────────────────────────┐
│  Level-2: Index Array（10 个 entry）                          │
│   - 每个 entry 记录一段 1MB 范围的 [start_vpid, end_vpid)   │
│   - 范围在 Data Array 内的偏移 base_offset                   │
│   - 按 start_vpid 升序排列，支持二分查找                     │
└─────────────────────────────────────────────────────────────┘
```

**容量定义**：

| 参数 | 值 | 说明 |
|---|---|---|
| `MATE_CACHE_SIZE` | 10 MB | Data Array 总大小（固定） |
| `INDEX_SIZE` | 1 MB | 每个 index entry 覆盖范围 |
| `INDEX_COUNT` | 10 | Level-2 数组长度 |
| `SLOTS_PER_INDEX` | 128 K = 1MB/8B | 每个 entry 管理的 slot 数 |
| `TOTAL_SLOTS` | 1.25 M = 10MB/8B | Data Array 总 slot 数 |

#### 4.3.5 查询算法

```rust
/// 二分查找 vpid 落在哪个 index entry，返回 (index_idx, slot_offset)
fn locate(cache: &MetaCache, vpid: u64) -> Option<(u8, u32)> {
    // 1. 二分 index（10 个 entry，最多 log2(10) ≈ 4 次比较）
    let idx = cache.index.binary_search_by_key(&vpid, |e| e.start_vpid);
    let entry = match idx {
        Ok(i)  => &cache.index[i],                          // 精确命中 entry 起始
        Err(i) => if i == 0 { return None; } else { &cache.index[i-1] },
    };
    // 2. 判断 vpid 是否在该 entry 范围内
    if vpid >= entry.start_vpid && vpid < entry.end_vpid {
        let slot_off = (vpid - entry.start_vpid) as u32;
        Some((entry.data_offset as u8, slot_off))   // 命中
    } else {
        None   // 未命中 → 需要替换
    }
}
```

#### 4.3.6 预读窗口（关键优化）

查 vpid `N` 时，**不直接读 `N` 自身，而是读 `[N - 1MB/8B, N + 1MB/8B)` 共 1MB 范围**：

```rust
/// 读取时：触发一次预读窗口加载
fn read_with_prefetch(cache: &mut MetaCache, vpid: u64) -> Option<PidLocation> {
    if let Some((idx, off)) = locate(cache, vpid) {
        return Some(cache.data[idx as usize * SLOTS_PER_INDEX + off as usize]);
    }
    // 未命中 → 触发替换 + 预读
    load_window(cache, vpid);
    locate(cache, vpid).map(|(idx, off)|
        cache.data[idx as usize * SLOTS_PER_INDEX + off as usize]
    )
}

/// 把 vpid 所在的 1MB 窗口加载到 Data Array
fn load_window(cache: &mut MetaCache, vpid: u64) {
    let window_start = (vpid / SLOTS_PER_INDEX as u64) * SLOTS_PER_INDEX as u64;
    let window_end   = window_start + SLOTS_PER_INDEX as u64;

    // 1. 从 page.mate 文件读 1MB 范围的 slot
    let mut buf = vec![0u8; INDEX_SIZE as usize];
    pread(&cache.mate_file, &mut buf, window_start * 8)?;

    // 2. 找一个空的 index slot，或替换最近的
    let victim = find_victim(cache, vpid);
    let data_off = victim.data_offset;

    // 3. 拷贝数据到 Data Array
    let dst = &mut cache.data[data_off as usize * SLOTS_PER_INDEX
                              .. (data_off + 1) as usize * SLOTS_PER_INDEX];
    dst.copy_from_slice(&buf);

    // 4. 更新 index entry
    cache.index[victim.idx as usize] = IndexEntry {
        start_vpid: window_start,
        end_vpid:   window_end,
        data_offset: data_off,
        last_used:  current_tick(),
    };
}
```

**预读窗口的意义**：  
B+Tree 一次查询常常要访问**多个连续的 vpid**（叶节点分裂、范围扫描）。  
预读 1MB 窗口 ≈ 128K 个 slot，覆盖后续 128K 次查找，**命中率显著高于按需单条加载**。

#### 4.3.7 替换策略（LRU-最近邻）

10 个 index entry 全被占用时，选 victim：

```rust
fn find_victim(cache: &MetaCache, current_vpid: u64) -> Victim {
    // 策略 A：LRU（按 last_used）
    // 策略 B：最近邻（找离 current_vpid 最远的 entry）  ← 推荐
    let mut best = 0;
    let mut best_dist = 0u64;
    for (i, e) in cache.index.iter().enumerate() {
        let dist = if current_vpid < e.start_vpid {
            e.start_vpid - current_vpid
        } else if current_vpid >= e.end_vpid {
            current_vpid - e.end_vpid
        } else {
            0   // 实际上 locate 已经排除了这种情况
        };
        if dist > best_dist {
            best_dist = dist;
            best = i;
        }
    }
    Victim { idx: best as u8, data_offset: cache.index[best].data_offset }
}
```

**为什么用"最近邻"而不是 LRU**：
- LRU 看时间，但**真正影响命中率的是空间局部性**。
- 当前查 vpid 10000，缓存里若有一个 entry 管 [5M, 6M]、另一个管 [100K, 101K]，
  显然 [100K, 101K] 离 10000 更近，未来更可能继续访问，**应该保留**。
- 离得最远的（最不可能近期再访问）才是 victim。

#### 4.3.8 修改（写）路径

写 meta 时**不需要触发磁盘 IO**，只需要更新 Data Array + 标记对应 index entry 为 dirty：

```rust
fn write_meta(cache: &mut MetaCache, vpid: u64, pid: PidLocation) {
    if let Some((idx, off)) = locate(cache, vpid) {
        let slot = &mut cache.data[idx as usize * SLOTS_PER_INDEX + off as usize];
        *slot = pid;
        cache.index[idx as usize].dirty = true;
    } else {
        // vpid 不在缓存 → 加载窗口后再写
        load_window(cache, vpid);
        write_meta(cache, vpid, pid);
    }
}
```

**脏标记的作用**：在 chunk flush 时，只把 dirty 的窗口写回 `page.mate` 文件，**避免全量 10MB 重写**。

#### 4.3.9 启动加载

```rust
fn open(dir: &Path) -> Result<MetaCache> {
    let mut cache = MetaCache {
        data: vec![0u8; MATE_CACHE_SIZE as usize],   // 10MB 全 0
        index: vec![IndexEntry::empty(); INDEX_COUNT as usize],
        mate_file: File::open(dir.join("page.mate"))?,
    };

    // 1. 预热第一个 1MB 窗口（vpid 0 .. 128K）
    load_window(&mut cache, 0);

    // 2. 其余 9 个 index entry 标记为 invalid（首次访问时按需加载）
    for i in 1..INDEX_COUNT {
        cache.index[i].data_offset = i as u16;
        cache.index[i].valid = false;
    }
    Ok(cache)
}
```

**启动延迟**：只读 1MB（一次 4KB 对齐 IO），**毫秒级**，与数据库大小无关。

#### 4.3.10 一致性协议（关键）

写入路径必须保证：**page 数据 和 meta slot 必须同时持久化**。

```
commit_chunk():
  1. 把要写的 page 写入 .block 文件的当前 chunk
  2. 把 vpid→pid 的 meta 更新 append 到 .block 末尾（同事务）
  3. fsync(.block)              ← 一次 fsync
  4. 把 MetaCache 中 dirty 的窗口回写到 page.mate
     （窗口大小 1MB，对齐 1MB 边界，可批量 writev）
  5. fsync(page.mate)           ← 二次 fsync
```

> ⚠️ 注意：第 3 步和第 5 步不是原子的。
> 崩溃后处理：扫描最后一个 .block 的 vpid 日志，比对 page.mate 当前状态，
> **以 .block 末尾日志为准**（因为 .block 写入先于 .mate 写入）。

恢复算法：

```rust
fn recover(shard: &Shard) -> Result<()> {
    // 1. 只预热 MetaCache 第 0 窗口（毫秒级）
    let mut meta = MetaCache::open(&shard.dir)?;
    // 2. 扫描最后一个未冻结 block 的所有 chunk
    let last_block = shard.last_block()?;
    for chunk in last_block.chunks() {
        for entry in chunk.vpid_log() {
            // 用 .block 的日志覆盖 .mate（因为 .block 先写）
            meta.write_meta(entry.vpid, entry.pid);
        }
    }
    // 3. 数据完整但 .mate 还没刷盘的 page（孤儿 page）忽略
    Ok(())
}
```

#### 4.3.11 vpid / pid 序号管理

##### 核心抽象

```
pid = encode(file_id, chunk_idx, page_idx)   ← 纯函数，由 file_id + offset 算
vpid = 自增整数                              ← 需要原子分配
```

**pid 不需要"分配器"**：pid 由 chunk 文件结构决定，chunk 内 page 顺序追加，pid 可以从 `(file_id, chunk_idx, page_idx)` 直接算出。
但当前 chunk 内的 `page_idx` 是需要"取最新 + 加一"的状态，因此也需要一个原子计数器。

##### vpid 分配器

```rust
pub struct VpidAllocator {
    next_vpid: AtomicU64,        // 下一个未分配 vpid
    free_head: AtomicU64,        // 空闲链表头 vpid（0 表示空）
    free_count: AtomicU64,       // 当前空闲数量
}

impl VpidAllocator {
    /// 分配新 vpid（永远返回未使用过的 id）
    pub fn alloc(&self) -> u64 {
        // 优先从空闲链表拿
        loop {
            let head = self.free_head.load(Acquire);
            if head == 0 {
                break;  // 空闲链表空，走 fast path
            }
            // CAS 取出链表头
            // （这里的 next_free 需要从 page.mate 的对应 slot 读出）
            if self.free_head
                .compare_exchange(head, next_free, Release, Acquire)
                .is_ok()
            {
                self.free_count.fetch_sub(1, Relaxed);
                return head;
            }
        }
        // fast path：直接自增
        self.next_vpid.fetch_add(1, Relaxed)
    }

    /// 回收 vpid（push 到空闲链表头）
    pub fn free(&self, vpid: u64) {
        loop {
            let head = self.free_head.load(Acquire);
            // 写入 page.mate[vpid].next_free = head
            write_meta_slot(vpid, head);
            if self.free_head
                .compare_exchange(head, vpid, Release, Acquire)
                .is_ok()
            {
                self.free_count.fetch_add(1, Relaxed);
                return;
            }
        }
    }
}
```

##### pid 分配器（chunk 内的 page_idx）

```rust
pub struct PidAllocator {
    /// 当前活跃 chunk 内的下一个 page_idx（达到 64 时触发 chunk 切换）
    next_page_in_chunk: AtomicU8,    // 0..=63
    /// 当前 chunk 的 (file_id, chunk_idx)
    current_chunk: AtomicU32,        // 高 24 位 file_id，低 8 位 chunk_idx
}

impl PidAllocator {
    /// 分配一个新 pid（用于写入 page 数据）
    pub fn alloc(&self) -> PidLocation {
        let page_idx = self.next_page_in_chunk.fetch_add(1, Relaxed);
        if page_idx >= 64 {
            // 当前 chunk 满，触发 chunk 切换（后台协程）
            self.rotate_chunk();
            return self.alloc();  // 重试
        }
        let (file_id, chunk_idx) = self.decode_current_chunk();
        PidLocation { file_id, chunk_idx, page_idx, flags: ALIVE }
    }
}
```

##### 物理地址换算（pid → 字节偏移）

```rust
/// 从 pid 直接算出文件内的字节偏移，**O(1) 算术运算**
pub fn pid_to_offset(pid: &PidLocation, page_size: usize) -> u64 {
    // 物理布局：
    //   block_file_offset = file_id * 10MB
    //   chunk_offset      = chunk_idx * 1MB
    //   page_offset       = page_idx * 16KB
    let block_off = (pid.file_id as u64) * BLOCK_SIZE;       // 10MB
    let chunk_off = (pid.chunk_idx as u64) * CHUNK_SIZE;     // 1MB
    let page_off  = (pid.page_idx as u64) * page_size as u64;
    block_off + chunk_off + page_off
}
```

##### 使用场景

| 场景 | 调用 | 返回 |
|---|---|---|
| 新建 vpid（B+Tree 节点分裂 / 新 key 插入） | `vpid_alloc.alloc()` | 新 vpid |
| 已有 vpid 的 page 被替换（COW） | `vpid_alloc.alloc()` | 新 vpid；旧 vpid 进 free |
| 写入新 page 到 chunk | `pid_alloc.alloc()` | 新 PidLocation |
| crash recovery 时扫描重建 | 直接遍历 chunk 内 page | 不需要 alloc |

##### 4.3.12 关于 Rust 所有权

> **你担心的地方：**
> "原子分配会不会被 Rust 借用检查器拒绝？"

**不会**。Rust 的所有权规则对 `AtomicU64` 完全透明：

```rust
// ✅ 合法：AtomicU64 提供内部可变性
let counter = AtomicU64::new(0);
counter.fetch_add(1, Ordering::Relaxed);     // 多个线程并发调用
counter.fetch_add(1, Ordering::Relaxed);

// ❌ 非法：u64 没有内部可变性，必须独占
let mut counter = 0u64;
counter += 1;  // 需要 &mut self，多线程会被 borrow checker 拒绝
```

**关键点**：
- `std::sync::atomic::AtomicU64` 实现了 `Sync`，可以在多线程间共享 `&self` 引用。
- `fetch_add` 只接受 `&self`，**不需要可变借用**。
- 编译期保证安全，运行时硬件保证原子。

**如果用 `u64` 而不是 `AtomicU64`**：
- 多个协程同时 `&mut counter += 1` 会编译失败（`&mut` 不能多线程共享）。
- 强行用 `Mutex<u64>` 会退化为串行，性能反而比 `AtomicU64` 差一个数量级。

**所以设计要求**：所有"取最新 + 加一"的计数器（vpid、pid、checkpoint seq 等）**必须用 `AtomicU64`**。

##### 4.3.13 启动时的序号恢复

```rust
fn recover_alloc_state(shard: &Shard) -> (VpidAllocator, PidAllocator) {
    // 1. 从最后一个 block 的 vpid 日志找 max_vpid
    let max_vpid = scan_max_vpid(&shard.last_block);
    let vpid_alloc = VpidAllocator {
        next_vpid: AtomicU64::new(max_vpid + 1),
        free_head:  AtomicU64::new(0),  // 重建空闲链表（可选）
        free_count: AtomicU64::new(0),
    };

    // 2. 从当前活跃 chunk 找 next_page_idx
    let next_page = scan_current_chunk(&shard.last_block);
    let pid_alloc = PidAllocator {
        next_page_in_chunk: AtomicU8::new(next_page),
        current_chunk:       AtomicU32::new(current_chunk_id),
    };
    (vpid_alloc, pid_alloc)
}
```

### 4.4 写入路径

```
write(k, v) ──► Shard
                  │
                  ▼
              1. 查 B+Tree 找到目标 vpid（leaf page）
                  │
                  ▼
              2. 用 vpid 查 vpid_to_pid → 旧 pid（chunk 位置）
                  │
                  ▼
              3. 读出旧 page payload 到内存（异步 IO）
                  │
                  ▼
              4. 在内存中修改/构造新 page
                  │
                  ▼
              5. 把新 vpid 写入「vpid 变更日志」(mem buffer)
                  │
                  ▼
              6. 把变更日志一次性 fsync 到当前 chunk（≤1MB）
                  │
                  ▼
              7. 更新内存中 vpid_to_pid 映射
```

**关键技巧**（来自你的设计）：
> "我们内存中每次会参考 LSM 一样每次将虚拟页码写入到最新页面并且一次性持久化最新的 chunk"

实现为 **Group Commit + Chunk Flush**：

```rust
struct WalBuffer {
    pending_vpids: Vec<VpidEntry>,     // 待刷盘的 vpid 变更
    pending_pages: Vec<EncodedPage>,   // 待刷盘的 page 数据
    current_chunk: Vec<u8>,            // 当前 chunk 的字节缓冲
    chunk_size: usize,                 // ≤ 1MB
}

impl WalBuffer {
    /// 把当前 chunk 原子刷盘：一次性 writev + fsync
    async fn flush_chunk(&mut self, ring: &IoUring) -> Result<()> {
        if self.current_chunk.is_empty() { return Ok(()) }
        // 1. io_uring writev 提交所有 page
        // 2. io_uring fsync 阻塞等完成
        // 3. 把 vpid→pid 映射的变更也原子写入
        // 4. 清空 buffer，复用当前 block 的下一个 chunk
    }
}
```

- **chunk 满 1MB 或显式 commit 时触发 flush**。
- **一次 fsync 同时持久化数据和映射**，崩溃后可以从 chunk 头重放 vpid 日志恢复。

### 4.5 读取路径

```
read(k, v) ──► Shard
                  │
                  ▼
              1. 查 B+Tree 找到 vpid（≤ 3 次 IO，缓存命中则 0 次）
                  │
                  ▼
              2. 查 vpid_to_pid → pid(file_id, chunk_idx, offset)
                  │
                  ▼
              3. mmap/file IO 读 chunk（异步），按 offset 截取 page
                  │
                  ▼
              4. 校验 checksum，返回 payload
```

**为什么比纯 LSM 快**：
- LSM 读需要查 memtable + N 层 SST + bloom；本方案只需 **1 次 B+Tree 查找 + 1 次 pid 解析 + 1 次 page 读取**。
- B+Tree 的 leaf 是顺序链表，**范围扫描天然友好**。

### 4.6 分裂与合并

#### 分裂
- 当某个 B+Tree node 的 page 写满 → 申请新 vpid（左半/右半），写入新 chunk。
- 父节点的指针更新同样走 COW，沿路径直到 root，root 也会获得新 vpid。
- vpid_to_pid 中只保留「最新版本」，旧版本留在磁盘上等 GC。

#### 合并（后台协程）
- 当 block 满 10MB 被冻结后，后台 compaction 协程扫描该 block 内的所有 vpid：
  - 同一 vpid 的多版本只保留最新，旧的 chunk 标记为空洞。
- 空洞率超过阈值（如 30%）触发 **block 重写**：把存活 vpid 按 vpid 顺序重写到新 block，让物理布局接近 B+Tree 的真实顺序，进一步提升 range scan 性能。

### 4.7 崩溃恢复

启动时：
1. 顺序扫描最后一个 block 的所有 chunk，重放 vpid 变更日志。
2. 重建内存中 vpid_to_pid。
3. 未完成的 page（只有数据无 vpid 记录）丢弃，保证一致性。

恢复时间 = O(最后一个 block 大小) ≈ O(10MB)，毫秒级。

### 4.8 内存数据结构

| 组件 | 数据结构 | 说明 |
|---|---|---|
| 内存 B+Tree 索引 | `BTreeMap<Key, Vpid>` | 红黑树，仅用于未刷盘的新数据 |
| vpid→pid 元表 | `MetaCache`（10MB 两层数组 + 预读窗口） | 启动只预热 1MB，其余按需加载 |
| vpid→pid 缓存 | `HashMap<Vpid, PidLocation>` | 跳过 mmap 的热点 vpid |
| 写缓冲 | `WalBuffer` | 累积 chunk 后批量刷盘 |
| 已冻结 block 元信息 | `Vec<BlockMeta>` | 顺序记录，mmap 即用 |

### 4.9 与 LSM 的对比

| 维度 | LSM | LCB-Tree（本设计） |
|---|---|---|
| 写吞吐 | 极高（纯追加） | 高（chunk 追加 + COW） |
| 读延迟 | P99 抖动大（多层查找） | 稳定（深度有限） |
| 范围扫描 | 需 merge sort | 顺序遍历 leaf 链 |
| 空间放大 | 严重（多版本） | 轻（block 重写后接近 1） |
| Compaction | 复杂（leveled/universal） | 简单（block 冻结 + 空洞回收） |
| 代码复杂度 | 高 | **低（统一一种结构）** |

## 五、关键技术选型

| 组件 | 选型 | 理由 |
|---|---|---|
| 语言 | Rust 2024 edition | 内存安全 + 无 GC + Send/Sync 强制无锁 |
| 异步运行时 | **monoio**（io_uring 原生协程） | 单线程内协程 + 真异步 IO，比 tokio+epoll 更适合存储 |
| 哈希 | FNV-1a 64 | 极快，分布可接受 |
| 内存表 | crossbeam-skiplist 或自研 SkipMap | 无锁并发读 |
| WAL | 顺序追加，组提交（group commit） | 摊薄 fsync 延迟 |
| SST | 自定义 block 格式 + mmap 读 | 读路径少一次拷贝 |
| 压缩 | Snappy / LZ4 | 速度优先 |

## 六、API 草案

```rust
pub struct NexusDB { shards: Vec<ShardHandle> }

impl NexusDB {
    pub async fn open<P: AsRef<Path>>(dir: P, opts: Options) -> Result<Self>;
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub async fn delete(&self, key: &[u8]) -> Result<()>;
    pub async fn batch(&self, ops: &[Op]) -> Result<()>;   // 单 shard 批
    pub async fn range(&self, lo: &[u8], hi: &[u8]) -> Result<Scan>;  // 跨 shard 合并
}
```

## 七、性能预期与风险

### 预期
- 写吞吐：单分片 ~50–150k ops/s（受 fsync 限制，启用组提交更高）
- 读吞吐：内存命中 ~1M+ ops/s/核
- 尾延迟 P99：协程+io_uring 比 epoll 更稳定

### 风险
1. **io_uring 内核版本要求**：需要 Linux 5.6+，5.19+ 才稳定。需 fallback 到 epoll（monoio 已支持）。
2. **跨 shard 事务**：本设计 MVP 不支持跨 shard ACID，仅保证单 shard 线性一致。若需要事务，需 2PC 协议。
3. **再均衡**：分片数固定，扩容需停机迁移。可用虚拟节点（VNode）做软分片，但会引入跨线程同步。
4. **协程栈/调度开销**：monoio 已验证 < 200ns 切换开销，对 KV 业务足够。

## 八、MVP 路线图

1. **M1 — 单分片骨架**：`monoio` + `io_uring` + 内存 SkipMap + WAL，跑通 put/get。
2. **M2 — 多分片**：N 个 shard 线程，hash 路由，CPU 绑定。
3. **M3 — SST + 读取路径**：落盘格式、bloom filter、异步读取协程。
4. **M4 — Compaction + 崩溃恢复**：后台协程 compaction、replay WAL。
5. **M5 — 范围扫描 + 跨 shard 合并**：迭代器抽象 + heap merge。
6. **M6 — 基准 + 调优**：与 RocksDB / TiKV 对比，写压测工具。

## 九、为什么不用锁

> "在足够快的核上，序列化就是你想要的并行。"
> —— ScyllaDB 的设计哲学

锁的根本问题是 **CAS 重试和 cache line bounce**。在 NUMA 机器上，跨核 `compare_and_swap` 的代价可以达到本地命中的 50 倍。  
NexusDB 通过 **把并发问题变成消息传递问题**，彻底回避这一类开销：每个协程只在自己的内存里工作，等待 IO 时不占 CPU。

## 十、参考

---

## 十一、外部接入与运维

(本节为快速上手指引; 详细实现见 `CHANGELOG.md` / `AGENTS.md` / 源码注释)

### 启动服务器

```bash
RUST_MIN_STACK=67108864 cargo run --release -- --config config/nexusdb.toml
```

默认监听:
- Binary 协议 (自定义二进制帧): `0.0.0.0:5433`
- RESP2 (Redis 兼容): `0.0.0.0:6379`

### 客户端连接

- **redis-cli**: `redis-cli -p 6379 -a <password> SET k1 v1` (AUTH 可选)
- **memtier_benchmark**: 标准 Redis 压测工具, 全流程可用
- **自家客户端**: `crates/network/src/protocol/binary.rs` 是 codec 参考实现

### 配置 `config/nexusdb.toml`

```toml
[server]
worker_count = 2
redis_addr = "0.0.0.0:6379"      # 空字符串 = 禁用 RESP 门面
redis_password = ""              # 空 = 不启用 AUTH
max_key_bytes = 1024             # KvLimits: key 上限
max_value_bytes = 3000           # KvLimits: value 上限 (key+value ≤ 4060)

[storage]
block_root = "./data"
num_shards = 6
io_backend = "io_uring"          # "stdfs" | "io_uring"
default_db = "default"
default_table = "default"

[log]
level = "info"                    # error|warn|info|debug|trace
dir = "./logs"                    # 空 = 仅 stderr, 不写文件
buffer_kb = 64
flush_interval_ms = 500
stderr = true
```

### 关键不变量 (运维需知)

- **data → meta 顺序**: meta 永远指向已落盘的 chunk; 手动 kill -9 可能丢 in-flight 但不丢已确认的
- **vpid 永不重用**: COW 友好的不变量; 老 pid 保留在 .block 直至 chunk rotate / LRU 驱逐
- **B+Tree 是有序的**: range scan / SQL `WHERE range` 后续将走 prefix-compress 的字节序
- **持久化触发**: chunk 满自动 swap + 周期 10s / 计数 256 写; SIGINT/SIGTERM 会排空落盘后再退出

- Seastar: https://github.com/scylladb/seastar
- monoio: https://github.com/bytedance/monoio
- glommio: https://github.com/DataDog/glommio
- RocksDB: https://github.com/facebook/rocksdb
- io_uring: https://kernel.dk/io_uring.pdf