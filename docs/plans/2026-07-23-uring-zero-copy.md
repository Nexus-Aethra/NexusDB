# io_uring 真零拷贝优化 Plan

> **日期**: 2026-07-23
> **作者**: Trae + wpp
> **目标**: 把当前 buffered io_uring 升级到真零拷贝高性能模式
> **关联**: DESIGN.md §3.4 (调度) / §4.3-§4.5 (storage) /
>            [scheduler/io_ops.rs](file:///home/wpp/nexus/NexusDB/crates/scheduler/src/io_ops.rs) /
>            [storage/pager_io.rs](file:///home/wpp/nexus/NexusDB/crates/storage/src/pager_io.rs)

---

## 1. 背景与现状

### 1.1 当前实现概览

| 维度 | 当前 | 是否最优 |
|---|---|---|
| io_uring opcode | `Read` / `Write`（普通） | ❌ |
| Registered buffers | 无 | ❌ |
| Registered files | 无 (`IOSQE_FIXED_FILE`) | ❌ |
| SQPOLL | 关闭（默认） | ⚠️ |
| Direct I/O | 关闭 | ⚠️ |
| buffer 来源 | 每次 `vec![0u8; 1MB]` | ❌ |
| fd 来源 | 每次 `open()` + `drop(close)` | ❌ |

### 1.2 性能瓶颈（每次 1MB chunk IO 路径）

```text
user:    pager.read(key)
   ↓
user:    OpenOptions::open(path)              ← syscall: open (1)
   ↓
user:    vec![0u8; CHUNK_SIZE]                ← heap alloc 1MB (2)
   ↓
user:    io_ops::read(fd, buf, off)
   ↓
user:    ring.submit()                        ← syscall: io_uring_enter (3)
   ↓
kernel:  Read SQE(fd, buf_ptr, len, off)
   ↓                                    ← pwrite/pread 走 page cache
kernel:  CQE 返回
   ↓
user:    ring.wait() 等 CQE (4)
   ↓
user:    drop(f)                              ← syscall: close (5)
```

每次 IO 至少 3 次 syscall（open + submit + close）+ 1MB alloc/free + SQE memcpy。

### 1.3 优化目标

按收益排序（按实现成本调整）：

1. **fd 复用 + `IOSQE_FIXED_FILE`** — 省 2 次 syscall/IO
2. **buffer 注册池 + `ReadFixed`/`WriteFixed`** — 省 SQE memcpy + 1MB alloc
3. **可选: `O_DIRECT`** — 绕开 page cache
4. **可选: `SQPOLL`** — 省 submit syscall

---

## 2. 总体设计

### 2.1 优化层级

```text
┌─────────────────────────────────────────────────────┐
│ Layer 4: O_DIRECT (可选, 高门槛, 高收益-部分 workload) │
├─────────────────────────────────────────────────────┤
│ Layer 3: SQPOLL (可选, 中门槛, 中收益)                │
├─────────────────────────────────────────────────────┤
│ Layer 2: Registered Buffers + Fixed Read/Write       │
│   - PageBufPool (already exists!) 扩展支持 Fixed idx │
│   - io_ops::read_fixed/write_fixed                    │
├─────────────────────────────────────────────────────┤
│ Layer 1: Registered Files + IOSQE_FIXED_FILE         │
│   - FD 池 (per-shard, 永生)                           │
│   - register_files at Pager open                     │
├─────────────────────────────────────────────────────┤
│ Layer 0: 当前 buffered io_uring (基线)                │
└─────────────────────────────────────────────────────┘
```

### 2.2 实施顺序（推荐）

| 阶段 | 任务 | 依赖 | 风险 |
|---|---|---|---|
| **T18a** | Layer 1: FD 池 + IOSQE_FIXED_FILE | 无 | 低 |
| **T18b** | Layer 2: PageBufPool 升级 + Read/Write Fixed | T18a | 低 |
| **T18c** | Layer 3: SQPOLL 可选 | T18b | 中（CPU 占用 + 兼容性） |
| **T18d** | Layer 4: O_DIRECT 可选 | T18b | 中（受 page_size/alignment 限制） |

> **MVP**: T18a + T18b（收益最高，成本最低，风险最低）
> **可选增强**: T18c + T18d（视 workload profile 再决定）

---

## 3. T18a: FD 池 + IOSQE_FIXED_FILE

### 3.1 目标

消除每次 IO 的 `open` + `close` syscall + fd lookup。

### 3.2 设计

**为什么懒分配**：

预分配所有 (db, file_id) 组合不现实：
- db_name 动态（多 db 架构）
- file_id 动态分配（chunk 满才 rotate，写入路径才知道）
- 启动时无法预测哪些 path 会被访问

**为什么"懒分配 + 永生"而不是"懒分配 + LIFO 栈式 push/pop"**：

`register_files` 是**增量追加**——kernel API 不允许中途 unregister 单个 fd。
所以"用完 push 回栈、超出容量 close 最旧的"思路不适用：
- push 回栈 = `unregister_files(slot)` = kernel 不支持单独 unregister
- 如果真要 LIFO 语义，只能 fallback 到"非 fixed file 路径"（每次 IO 传 fd），那就白做了

**结论**：懒分配 + 永生 + 容量上限是唯一可行的 fixed file 方案。

**核心结构**：

```rust
// scheduler/src/fd_pool.rs (新文件)

/// Per-shard FD 池: 懒分配 + 永生 + 容量上限.
/// 第一次访问某 path 时 open + register_files, 之后永久保留 slot_id.
/// 容量超限报错 (防止 fd 泄漏).
pub struct FdPool {
    /// path → slot_id (cache, O(1) 命中)
    path_to_slot: HashMap<PathBuf, u16>,
    /// slot_id → raw fd (用于 Pager::drop 统一 close)
    slot_to_fd: HashMap<u16, RawFd>,
    /// 单调递增 slot id (0..MAX_FD_PER_SHARD)
    next_slot: u16,
}

const MAX_FD_PER_SHARD: usize = 64;  // 每 shard 上限 64 个 .block file

impl FdPool {
    /// 拿 path 对应的 slot_id. 命中返回; 未命中 open + register.
    pub fn acquire(&mut self, ring: &mut IoUring, path: &Path) -> io::Result<u16>;

    /// Pager::drop 时批量 close 所有 fd (ring 由 OS 清理).
    pub fn close_all(&mut self);
}
```

### 3.3 API 改动

```rust
// scheduler/src/io_ops.rs

/// 新 API: Read with fixed file slot.
pub async fn read_fixed(
    slot: u16,                    // ← IOSQE_FIXED_FILE slot, not fd
    buf: &mut [u8],
    offset: u64,
) -> io::Result<usize> {
    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fixed(slot),   // ← 不传 fd, 用 slot
        buf.as_mut_ptr() as *mut _,
        buf.len() as u32,
    )
    .offset(offset)
    .flags(io_uring::squeue::Flags::FIXED_FILE)  // ← 关键 flag
    .build();
    // ... 同原有 submit_sqe! 路径
}

/// 新 API: Write with fixed file slot.
pub async fn write_fixed(
    slot: u16,
    buf: &[u8],
    offset: u64,
) -> io::Result<usize>;
```

### 3.4 PagerIo 改动

```rust
// storage/src/pager_io.rs

pub struct IoUringBackend {
    /// Per-Pager FD pool (T18a). 注册到当前 ring 时调用 register_files.
    fd_pool: Arc<Mutex<FdPool>>,
    /// 当前 io_uring 实例 (用于 register_files).
    ring: *const IoUring,  // raw, 由 caller (Pager) 持有
}
```

**register 流程**（懒分配，Pager 任意时刻）：

```text
Pager::read/write 触发 IO
   ↓
PagerIo::IoUring.read_chunk(path, off)
   ↓
fd_pool.acquire(ring, path)  ← HashMap 命中? 返回 slot : open + register_files
   ↓
命中: O(1) 返回 slot_id
未命中: OpenOptions::open(path) → fd, register_files([fd]) → slot_id
   ↓
io_ops::read_fixed(slot, buf, off)  ← SQE 用 slot, 不传 fd
   ↓
kernel: IOSQE_FIXED_FILE → 直接查 slot 表拿 fd
```

**首次访问的 syscall 序列**：
```text
1. open(path)        ← 1 次 syscall
2. io_uring_register_files([fd])  ← 1 次 syscall (注册到 ring file table)
3. ring.submit()      ← 1 次 syscall (提交 SQE)
```

**第二次访问同一 path**：
```text
1. ring.submit()      ← 1 次 syscall (fd 已注册, 走 cache)
```

省了 `open` + `register_files` = 2 次 syscall / IO。

### 3.5 测试

- `fd_pool_tests.rs`:
  - `acquire_returns_unique_slot`
  - `acquire_same_path_returns_same_slot` (cache 命中)
  - `acquire_after_open_doesnt_leak_fd` (acquire 1000 次不同 path → 容量超限报错)
- `pager_io_uring_fixed_e2e.rs`:
  - `read_chunk_with_fixed_file_works`（已有 round_trip + 改用 fixed file）

### 3.6 风险点

| 风险 | 缓解 |
|---|---|
| ring 重启后 slot 失效 | Pager::open 时 register，重启时新建 pool |
| 多 Pager 共享同一 ring | 当前架构不允许（每 shard 独立 ring） |
| fd 太多 register_files 失败 | Linux 默认 1024 files，足够；不预分配多 db 全 file_id |
| drop 顺序导致 close 报错 | FdPool drop 时只 close owned fd |

---

## 4. T18b: Registered Buffers + Read/Write Fixed

### 4.1 目标

消除 SQE memcpy（buf_ptr 内核读 addr+len）+ 消除 1MB alloc/free。

### 4.2 设计

**核心结构**：

```rust
// storage/src/page_pool.rs (升级)

/// 注册到 io_uring 的 buffer pool.
/// 每个 buffer 在 ring 里有固定 slot_id, SQE 只传 slot_id 不传 addr.
pub struct RegisteredPageBuf {
    inner: Box<[u8; PAGE_SIZE]>,
    slot_id: u16,
}

impl PageBufPool {
    /// 注册 N 个 16KB buffer 到 ring (T18b).
    /// 返回 page_pool + slot 映射.
    pub fn register(ring: &mut IoUring, capacity: usize) -> io::Result<Self>;

    /// 拿一个 buffer + slot_id.
    pub fn alloc(&self) -> (Box<[u8; PAGE_SIZE]>, u16);

    /// 归还 buffer 到 pool (slot 仍然注册, 不 unregister).
    pub fn recycle(&self, buf: Box<[u8; PAGE_SIZE]>, slot_id: u16);
}
```

### 4.3 API 改动

```rust
// scheduler/src/io_ops.rs

/// 新 API: Read with fixed file + fixed buffer.
pub async fn read_fixed_buf(
    file_slot: u16,
    buf_slot: u16,
    len: u32,
    offset: u64,
) -> io::Result<usize> {
    let entry = io_uring::opcode::ReadFixed::new(
        io_uring::types::Fixed(file_slot),
        buf_slot,                          // ← buffer slot, not addr
        len,
        buf_slot.into(),                   // bvec_index
    )
    .offset(offset)
    .flags(io_uring::squeue::Flags::FIXED_FILE)
    .build();
    // ...
}

/// 同理 write_fixed_buf
```

### 4.4 优势

| 路径 | 开销 |
|---|---|
| 旧（buffered Read） | SQE memcpy (addr+len) + kernel 临时映射 |
| 新（ReadFixed） | SQE 只传 slot_id (2B)，kernel 直接查 buffer table |

SQE 大小：64B（普通）→ 64B（Fixed 没有 size 区别，但内容简化）

### 4.5 PageBufPool 注册时机

```text
Pager::open
   ↓
PageBufPool::register(ring, 16)  ← 注册 16 × 16KB = 256KB 给 ring
   ↓
后续 IO 用 PageBufPool::alloc() 拿 (buf, slot)
   ↓
用完 PageBufPool::recycle() 归还 (slot 保留注册)
```

### 4.6 测试

- `page_buf_pool_registered.rs`:
  - `register_returns_distinct_slots`
  - `alloc_recycle_preserves_slot`
  - `read_fixed_buf_writes_to_pool_buffer`
- `pager_io_fixed_buf_e2e.rs`:
  - `read_chunk_returns_registered_buffer_slot`
  - `read_chunk_lru_cache_uses_registered_buffers`

### 4.7 风险点

| 风险 | 缓解 |
|---|---|
| buffer 数量限制 | ring 启动时 register 一次，容量可控 |
| 跨 shard 共享 pool | 当前每 shard 独立 pool，独立 register |
| 多线程访问 pool | pool 内部用 `Mutex` 保护 slot table |

---

## 5. T18c: SQPOLL (可选)

### 5.1 目标

消除 submit syscall，让 kernel thread 自旋 poll SQ。

### 5.2 适用场景

- 高 IO 吞吐场景（减少 syscall 是关键）
- 不介意多 1 个 CPU 核心常驻
- kernel ≥ 5.11

### 5.3 启用

```rust
// scheduler/src/scheduler.rs

let mut builder = io_uring::IoUring::builder();
builder.setup_sqpoll(1000 /* ms idle */);  // 1s 没活就睡
builder.setup_sqpoll_cpu(0);               // 绑核
let ring = builder.build(IO_URING_ENTRIES)?;
```

### 5.4 风险点

| 风险 | 缓解 |
|---|---|
| 多 1 CPU 占用 | 仅生产环境启用，测试关闭 |
| kernel 兼容 | feature flag 区分 |
| 与现有 submit syscall 冲突 | SQPOLL 模式下 `submit()` 仍是 no-op |

---

## 6. T18d: O_DIRECT (可选)

### 6.1 目标

绕开 page cache，内核→磁盘直接拷贝。

### 6.2 适用场景

- 大块顺序 IO（1MB chunk 完美匹配）
- 读多写少 or 写密集（不重复读）
- page cache 没收益

### 6.3 启用

```rust
let f = OpenOptions::new()
    .read(true)
    .write(true)
    .custom_flags(libc::O_DIRECT)  // ← 关键
    .open(path)?;
```

### 6.4 限制（Linux）

| 限制 | 影响 |
|---|---|
| buffer alignment | 必须 512B 对齐（我们 16KB page OK） |
| IO size alignment | 必须 512B 倍数（我们 1MB chunk OK） |
| 文件 offset | 必须 512B 对齐（chunk offset OK） |
| 性能可能反而下降 | 部分 SSD/NVMe 在 page cache 下更快 |

### 6.5 测试

- `pager_io_direct_e2e.rs`:
  - `read_chunk_o_direct_round_trip`
  - `write_chunk_o_direct_persists`
  - `random_access_o_direct_works`

---

## 7. 兼容性策略

### 7.1 切换接口

```rust
// storage/src/pager_io.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoBackendConfig {
    pub backend: IoBackend,
    pub use_fixed_file: bool,    // ← T18a
    pub use_fixed_buffer: bool,  // ← T18b
    pub sqpoll_ms: u32,          // ← T18c, 0 = disabled
    pub o_direct: bool,          // ← T18d
}

impl Default for IoBackendConfig {
    fn default() -> Self {
        Self {
            backend: IoBackend::StdFs,
            use_fixed_file: false,
            use_fixed_buffer: false,
            sqpoll_ms: 0,
            o_direct: false,
        }
    }
}
```

**OpenOptions 新字段**：
```rust
pub struct OpenOptions {
    // ... 现有字段
    pub io_config: IoBackendConfig,
}
```

### 7.2 回退路径

- T18a 失败 → fall back to 普通 fd (close before submit)
- T18b 失败 → fall back to 普通 buffer + 普通 Read
- T18c 失败 → fall back to 用户态 submit
- T18d 失败 → fall back to 走 page cache

每层都有 graceful fallback，单层失败不影响其他层。

---

## 8. 性能验证

### 8.1 Micro-benchmark

```rust
// benches/uring_zero_copy_bench.rs

- 顺序读 100 个 1MB chunk: buffered vs fixed_file vs fixed_buf
- 随机读 1000 个 16KB page: buffered vs fixed_file vs fixed_buf
- 顺序写 100 个 1MB chunk: buffered vs fixed_file vs fixed_buf
```

### 8.2 E2E 基准

```rust
// benches/nexusdb_uring_bench.rs

- 写 10000 个 KV, 测 throughput (ops/sec)
- 读 10000 个 KV, 测 p50 / p99 latency
- 混合读写 70/30
```

### 8.3 验收标准

| 指标 | 基线（buffered） | T18a 目标 | T18b 目标 |
|---|---|---|---|
| 顺序读 ops/sec | 1x | 1.2x | 1.5x |
| 顺序写 ops/sec | 1x | 1.3x | 2x |
| 单次 IO latency p50 | 1x | 0.85x | 0.7x |
| 单次 IO latency p99 | 1x | 0.9x | 0.8x |
| syscall count / IO | 3 | 1 | 1 |

---

## 9. 子任务清单

### T18a (基础 FD 池)

- [ ] T18a.1: `scheduler/src/fd_pool.rs` — FdPool + register_files
- [ ] T18a.2: `io_ops::read_fixed` / `write_fixed` API
- [ ] T18a.3: `PagerIo::IoUringBackend` 改造持有 FdPool
- [ ] T18a.4: Pager::open 注册所有 block file
- [ ] T18a.5: `fd_pool_tests.rs` (3 测试)
- [ ] T18a.6: `pager_io_uring_fixed_e2e.rs` (round_trip + fixed)
- [ ] T18a.7: `IoBackendConfig::use_fixed_file` flag
- [ ] T18a.8: clippy + workspace 测试通过

### T18b (Registered Buffers)

- [ ] T18b.1: `page_pool.rs` 升级加 `register(ring, capacity)`
- [ ] T18b.2: `io_ops::read_fixed_buf` / `write_fixed_buf` API
- [ ] T18b.3: Pager::read/create 走 fixed buffer
- [ ] T18b.4: `page_buf_pool_registered.rs` (3 测试)
- [ ] T18b.5: `pager_io_fixed_buf_e2e.rs` (2 测试)
- [ ] T18b.6: `IoBackendConfig::use_fixed_buffer` flag
- [ ] T18b.7: clippy + workspace 测试通过

### T18c (SQPOLL)

- [ ] T18c.1: `IoUring::builder()` + `setup_sqpoll`
- [ ] T18c.2: feature flag `uring-sqpoll`
- [ ] T18c.3: Linux 版本探测（≥ 5.11）
- [ ] T18c.4: 测试 kernel 兼容性
- [ ] T18c.5: bench 验证收益

### T18d (O_DIRECT)

- [ ] T18d.1: `OpenOptions::custom_flags(libc::O_DIRECT)`
- [ ] T18d.2: alignment 检查（buffer / offset / size 都 512B 对齐）
- [ ] T18d.3: `pager_io_direct_e2e.rs` (3 测试)
- [ ] T18d.4: feature flag `uring-o-direct`
- [ ] T18d.5: workload profile（决定默认开启 / 关闭）

---

## 10. 设计决策与权衡

### 10.1 为什么 T18a + T18b 必做

- **收益确定性高**：少 syscall + 少 memcpy，CPU-bound 场景直接受益
- **风险低**：io_uring 文档充分，rust binding 稳定
- **兼容性好**：feature flag 关闭时回到基线

### 10.2 为什么 T18c 可选

- **CPU 占用问题**：kernel thread 常驻 1 个核心
- **container 兼容性**：部分 docker / sandbox 限制
- **测试成本高**：需要专门 Linux 环境

### 10.3 为什么 T18d 可选

- **workload 依赖**：顺序 IO 才划算
- **NVMe 已经很快**：page cache 在 NVMe 上收益小
- **alignment 限制**：必须 4KB / 512B 对齐（我们 OK）
- **降级路径**：失败时 fallback 到 buffered

### 10.4 关键设计选择

| 选择 | 理由 |
|---|---|
| **per-shard 独立 pool** | 符合 share-nothing 架构，无锁 |
| **register 一次性 + 永生** | 文件名稳定（`{file_id + 1:06}.block`），无需动态 register |
| **slot id 用 u16** | ring buffer 限制 65535 slots，足够 |
| **PageBufPool 容量 16** | 16 × 16KB = 256KB，匹配当前架构 |
| **fallback 总是走原路径** | feature flag 控制，不破坏现有测试 |

---

## 11. 文档更新

实施完成后需要更新：

- [ ] `DESIGN.md` §4.5 storage io backend 章节 — 补充 fixed file / fixed buffer 描述
- [ ] `CHANGELOG.md` — 记录 T18 实施进度
- [ ] `docs/superpowers/specs/` — 新 spec 文件（如果架构有变更）
- [ ] `crates/scheduler/src/io_ops.rs` — 模块 doc 补充 fixed file/buffer 用法
- [ ] `crates/storage/src/pager_io.rs` — 补充 fallback 路径说明
- [ ] `crates/storage/src/page_pool.rs` — 补充 register 用法

---

## 12. 验收清单

- [ ] T18a: clippy 0 警告, workspace 测试全过, fixed file 路径生效
- [ ] T18b: clippy 0 警告, workspace 测试全过, fixed buffer 路径生效
- [ ] T18c: feature flag 启用后稳定运行（可选）
- [ ] T18d: feature flag 启用后稳定运行（可选）
- [ ] micro-bench: 顺序 IO ≥ 1.2x (T18a), ≥ 1.5x (T18b)
- [ ] e2e bench: KV ops/sec ≥ 1.2x (T18a), ≥ 1.5x (T18b)
- [ ] 文档同步更新
- [ ] commit + push

---

> **下一步**: 确认 T18a + T18b 实施方案，开始 T18a.1（FdPool + register_files）。
> T18c / T18d 待前两个稳定运行后再决定。