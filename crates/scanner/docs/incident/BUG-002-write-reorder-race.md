# BUG-002: 追加块式写路径的乱序覆写竞态

> **状态**: 待修 (Open)
> **严重度**: Critical (数据静默损坏 / 元数据错位, 已在 INCIDENT-001 生产现场复现)
> **组件**: `crates/storage` — `pager.rs` / `pager_backend.rs` / `chunk_writer.rs` / `meta_cache.rs` / `crates/shard_manager/src/shard_thread.rs`
> **关联**: 与 `INCIDENT-001-bad-page-type.md` 同源 —— E:/study 生产事故 (vpid 0 变成 Leaf、page.mate 的 file_id 系统性偏移) 即本类 bug 的现场实例。

---

## 1. 问题陈述 (用户原话)

> 我们的数据写入是**追加块式写**, 但当前**没有写优化器**。写入传进来是**顺序**的, 但写到盘上是**乱序**的。这会导致**先来的一部分定时 flush 或写入任务可能会覆盖后面 swap 任务**。

核心矛盾:

- **追加块式写 (append-chunk write)**: 一个 chunk = 1MB = 64 × 16KB 页。写先在内存 `nowchunks` 里累积, 满 64 页或背压时整体 `swap` 出去 (整体 1MB pwrite), 这是天然"块粒度、乱序友好"的模型。
- **没有写优化器 (write optimizer)**: 缺少一个全局的"写顺序仲裁层"。同一个 chunk 的多个版本 (周期性快照 / swap 全量快照 / COW 复用原位写) 按"谁先落盘谁赢"而非"版本号最大者赢"来收敛, 因此**先发起的、可能更旧的快照可能后落盘并覆盖后发起的、更新的 swap 全量快照**。

结果: 磁盘上出现"旧版本覆盖新版本", 表现为 INCIDENT-001 那种 page.mate 错位、MetaPage 槽被业务 Leaf 数据占用。

---

## 2. 当前写路径全景 (已读代码)

```
PageWriteBatch::submit (主线程, 单 shard 串行)
  └─ 对每个 vpid:
       决定 复用原位 (chunk 仍在 nowchunks) OR COW (alloc 新 pid)
       memcpy 进 nowchunks[key][page_idx]
       if is_chunk_full(key): swap_full_chunk_to_write_queue(key)        → 异步
            └─ take_chunk_box(key)  // 拿走整个 1MB 视图
            └─ chunk_list.invalidate(key)  // 作废旧快照
            └─ (背压: pending+in_flight >= MAX → 同步 pwrite; 否则) enqueue → write_queue.pending

shard 主循环 (每轮):
  a. harvest FlushDone:
       Data(key)      → complete_flush(key)
       Meta(win)      → complete_meta_flush(win)
       CompactRead/Write/ChunkPromotion → ...
  b. take_flush_batches_limited → 推进 pending→in_flight → spawn 协程 write_chunks_batch
  b2. take_meta_flush_batch (闸门: flush_backlog()==0) → spawn 协程 write_mate_windows
  b3. start_compact (闸门: flush_backlog()==0)
  c. maybe_periodic_flush → 仅重置计数 + WAL seal (⚠️ 不再快照/落盘数据)
```

### 关键并发约束现状

| 约束 | 代码位置 | 状态 |
|------|----------|------|
| 同 key 不允许两个快照同时在飞 | `take_flush_batches_limited` 跳过 `in_flight.contains_key(key)` | ✅ 已修 (2026-08-02) |
| swap 时作废 chunk_list 旧快照 | `swap_full_chunk_to_write_queue` → `chunk_list.invalidate` | ✅ 已修 |
| complete_flush 丢弃过时快照 (pending/in_flight 有更新) | `complete_flush` 的 `peek_chunk_pending/in_flight` 检查 | ✅ 已修 |
| complete_flush 仅当新有效页数 ≥ 旧时才替换 | `count_valid_pages` 比较 `new_valid >= old_valid` | ⚠️ 有洞 (见 §4.2) |
| meta 刷盘闸门 = data backlog 为空 | `take_meta_flush_batch`: `!meta_flush_due \|\| flush_backlog()>0 → None` | ✅ 逻辑正确 |
| 同步 flush 前必须 drain in-flight | `Pager::flush` / `meta.flush_dirty` 的 `debug_assert!` | ❌ **release 下被编译掉 (见 §4.1)** |
| 驻留兜底加载源优先级 = 读路径优先级 | `pager_write.rs` submit 的 `pending→completed→in_flight→chunk_list→disk` | ✅ 已修 |

---

## 3. 已修复的部分 (不要重复劳动)

`2026-08-02` 这一批提交已经把" periodic flush 快照 A(4页) vs swap 全量快照 B(64页) " 的经典竞态基本堵死:

1. **swap 立即作废 chunk_list 旧快照** —— 防止 swap 在管线中飞行时, 读路径命中缺页的旧快照。
2. **complete_flush 丢弃过时快照** —— 若 `write_queue` 或 `in_flight` 里有同 key 更新版本, 本次完成的旧快照不入 `chunk_list`。
3. **有效页数闸门** —— 仅当 `new_valid >= old_valid` 才替换 `chunk_list`, 防止中间快照 (4/14/59页) 覆盖全量快照 (64页)。
4. **驻留兜底加载源优先级对齐读路径** —— 修复 `catalog_many_dbs` 回归。
5. **meta 刷盘闸门只看 `flush_backlog()==0`** —— `flush_backlog = in_flight + pending`, 保证 data 全落盘后才动 page.mate。

**结论**: §5.4 描述的" periodic 快照 A 先于 swap 快照 B 落盘" 在 `maybe_periodic_flush` 改为"只 seal WAL、不快照"之后, 入口已被消除。但**用户当前仍观察到乱序覆写**, 说明还有未被这批修复覆盖的路径。

---

## 4. 仍然存在的真实缺口

### 4.1 CRITICAL — `debug_assert!` 在 release 下被编译掉, 同步 flush 可与异步协程并发写同一磁盘区域

`Pager::flush()` 和 `meta_cache::flush_dirty()` 都用 `debug_assert!` 来强制"先 drain 异步 backlog":

```rust
// pager.rs:796
debug_assert!(
    self.in_flight.is_empty(),
    "flush() requires in-flight chunks drained first"
);

// meta_cache.rs:191
debug_assert!(
    !self.in_flight_windows.iter().any(|&b| b),
    "flush_dirty 要求无 in-flight meta 快照 (先 drain 异步 backlog)"
);
```

**问题**: `debug_assert!` 在 `cargo build --release` (及生产 profile, `opt-level` 非零) **完全不生成代码**。于是 release 构建下:

- `Pager::flush()` 被调用 (close / drain / 外部显式 flush) 时, **不等** data 协程完成, 直接对 `nowchunks` 驻留 chunk 做 `io.write_page_chunk` (写同一 `.block` 偏移), 而此时 `in_flight` 里可能正有一份**更新的 swap 全量快照**在飞。两份写 **目标偏移完全相同** → 谁后完成谁赢 → **先发起的旧快照可能后落盘覆盖新 swap 快照**。
- `Pager::flush()` 随后调 `meta.flush_dirty()` 同步写 page.mate; 而 `take_meta_flush_batch` 之前已 spawn 的 meta window 协程仍在飞, 也在写同一 page.mate 偏移。两份 page.mate 写并发 → **torn / 旧窗口覆盖新窗口**。

这正是用户描述的"**先来的一部分…写入任务可能会覆盖后面 swap 任务**"的精确机制。**最危险的是: 这些保护只在 `debug` 构建生效, 而生产是 release**, 等于没保护。

**修复方向**:
- 把 `debug_assert!` 改为 `if !cond { return Err(StorageError::ConcurrentFlush) }`, 或
- `flush()` / `flush_dirty()` 内部先 `block_on` drain 所有 in_flight (data + meta window), 再同步写; 或
- 引入单调递增的 **write sequence / epoch**: 每次 swap/flush 给快照打版本号, 落盘时只接受 `epoch >= chunk_list.epoch` 的写, 用版本号而非时序仲裁。

### 4.2 HIGH — `complete_flush` 的有效页数闸门用 `>=`, 会保留"页数相同但内容更旧"的快照

```rust
// pager_backend.rs:90
let new_valid = count_valid_pages(&bytes);
let should_insert = if let Some(existing) = self.chunk_list.peek(&key.into()) {
    let old_valid = count_valid_pages(&existing);
    new_valid >= old_valid          // ← 相等时保留新到达者
} else { true };
if should_insert {
    self.chunk_list.insert_from_write_queue(key, bytes);
}
```

`>=` 在"有效页数相同 (==)"时, **后到达的赢**。若链路里出现:

- 快照 A (64 页, 版本 v1) 先完成 → 插入 chunk_list。
- 快照 B (64 页, 版本 v1, 但**更早发起、内容更旧**, 例如是 B 的前一次迭代) 后完成 → `new_valid==old_valid` → 被接受, **覆盖掉 v2 的较新内容**。

页数相等但内容落后, 不是"覆盖 swap"而是"覆盖同一 chunk 的更新版本", 同样导致数据回退。修复方向: 用 `(sequence, valid_count)` 二元组比较, 或者直接比较内容 hash / 用 epoch 序号精确判新旧。

### 4.3 MEDIUM — `maybe_periodic_flush` 不再持久化数据, 半满 chunk 仅依赖 WAL 回放

改动后:

```rust
// pager_backend.rs:546
pub async fn maybe_periodic_flush(&mut self) -> io::Result<bool> {
    ...
    if !should_flush { return Ok(false); }
    // 仅重置计数器 + 触发 WAL seal, 不再快照驻留 chunk
    self.write_count_since_flush = 0;
    self.last_flush_time = std::time::Instant::now();
    Ok(true)
}
```

结果: 一个**未满 64 页的驻留 chunk**, 只有在 (a) 后续写满触发 swap, 或 (b) 显式 `flush()` 时, 才会被写盘。在 (a)(b) 之前若崩溃, 该 chunk 内容**仅存在于 `nowchunks` 内存 + WAL**。

这就要求 **WAL 必须包含该 chunk 每个 page 的完整写入**, 且 WAL seal/replay 与内存状态严格一致。若 WAL 段在 seal 边界与内存存在窗口错位, 半满 chunk 的写入会**静默丢失**——这把"乱序"问题转嫁成"依赖 WAL 顺序"的脆弱性。建议: 周期性 flush 至少对驻留 chunk 做一次 `COW` 快照入队 (带版本号), 而不是完全不持久化数据。

### 4.4 LOW — compact dst 与正常 free 池的边界

`pick_dst_for` 只选 `live>0` 的宿主 chunk 作 compact 目标, `pop_free_chunk` 只弹 `live==0`, 二者天然分离; fresh-bump dst 推进 `pid_bump_next` 做预留。当前未见交叉。但 `start_compact` 的 `excluded` 集合 (pending/in_flight/resident/META) 在**选择时**快照, 选择后到 compact 协程完成期间, 若同一 dst 宿主 chunk 因并发 `COW 复用` 被重新写满并 swap, 理论上有极小概率冲突。鉴于 compact 单飞 + `complete_compact` 的 CAS 校验 (`meta.read(vpid) != Some(src_pid)` → 跳过), 现实风险低, 仅记录待观察。

---

## 5. 复现风险矩阵

| 场景 | 触发条件 | 后果 | 是否已保护 |
|------|----------|------|-----------|
| release 下 close 时 data 协程未 drain | `Pager::flush()` + 在飞 data 协程 | 旧快照覆盖新 swap, 磁盘错位 | ❌ (debug_assert 失效) |
| release 下 meta 窗口协程与 `flush_dirty` 并发 | `flush()` + 在飞 meta 窗口 | page.mate torn / 旧窗口覆盖 | ❌ |
| 页数相等内容不同的两版同 key 快照 | 同一 chunk 两次迭代都 64 页 | 新内容被旧内容回退 | ⚠️ `>=` 漏洞 |
| 崩溃时半满 chunk 未满且无显式 flush | WAL seal 边界错位 | 该 chunk 写入静默丢失 | ⚠️ 依赖 WAL 精确性 |

---

## 6. 修复建议 (按优先级)

1. **P0 — 把 flush 的 `debug_assert!` 换成真实运行时守卫**。
   同步 `flush()` / `meta.flush_dirty()` 必须先 drain (block_on) 所有 `in_flight` (data + meta window), 或显式返回 `Err(ConcurrentFlush)` 拒绝在不一致状态刷新。这是消除"release 下无保护"的唯一可靠手段。

2. **P0 — 引入 write epoch / sequence 仲裁**。
   每次 swap / COW 分配 / 显式 flush 递增全局 epoch; 每个 chunk 快照携带其 epoch。`complete_flush` 与 `chunk_list` 的替换条件改为 `epoch > current_epoch` (严格大于), 用版本号而非"到达时序"或"页数"判新旧。彻底消除"旧快照后落盘覆盖新快照"。

3. **P1 — `complete_flush` 闸门改为严格 `>` + epoch 比较**, 删除 `>=` 的内容相等回退路径。

4. **P2 — 周期性 flush 恢复"对驻留 chunk 做带版本号 COW 快照入队"**, 不依赖 WAL 单点保证半满 chunk 的持久性。

5. **P2 — 加锁/复算 `start_compact` excluded 集合** 到 compact 协程完成点, 或让 `complete_compact` 对 dst 宿主也做 CAS。

---

## 7. 验证手段 (建议补充的测试)

- **集成测试 `race_stale_snapshot_wins`**: 构造 chunk K, 先 enqueue 旧快照 A(64页, epoch=v1) 到 in_flight, 再 swap 新快照 B(64页, epoch=v2) 入队; 强制 A 后完成。断言 `chunk_list[K]` 是 B 的内容, 且 `flush_backlog()` 归零后才允许 meta 刷盘。
- **release 构建 CI**: 在 `--release` 下跑 `Pager::flush()` 与在飞协程并发, 断言返回 `Err` 而非静默覆盖。
- **崩溃恢复模糊测试**: 随机在 swap/enqueue/flush 之间 kill, 重启后用 scanner `rescue-json` + `verify` 校验数据一致。

---

*文档结束*
