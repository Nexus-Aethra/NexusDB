# Incident-001: `rebuild_composite_counts` 因 bad page type 崩溃

> **日期**: 2026-08-21
> **报告人**: 扫描器工具链
> **状态**: 根因已定位，引擎已修复（2026-08-02），待救援

---

## 1. 事故现象

邻居组反馈：一个 21MB 的单进程数据库在 kill-9 后无法打开。引擎初始化时报错：

```
[ERROR][shard] shard-0 engine init failed:
  "rebuild_composite_counts: btree error: bad page type at vpid 5:
   expected Leaf/Internal, got Meta"
```

引擎直接退出，用户无法读取数据。

---

## 2. 现场还原

### 2.1 目录布局

```
E:/study/
  shard_0/                              ← L3 外层包装
    default/
      shard_0/                          ← 实际 block 目录
        000001.block        (10 MB)     ← 单 block 文件
        page.mate           (1 MB)      ← vpid→disk 映射
        pid.state           (8 B)       ← 高水位标记
        stats.bin           (18 B)
    shard_0.wal.000180 ~ 000213         ← 34 个 WAL 段
```

扫描器识别为 **L3 布局**（`shard_<M>/<db>/shard_<N>/`）。

### 2.2 关键发现

#### 发现 A: page.mate 的 file_id 系统性偏移

```console
$ map --from-mate-only
vpid  file_id  chunk_idx  page_idx  flags  source
0     0        0          0         0x01   mate
1     0        0          1         0x01   mate
2     0        0          12        0x01   mate
3     0        0          3         0x01   mate
4     0        0          11        0x01   mate
5     0        0          13        0x01   mate    ← 指向空白页
6     0        0          6         0x01   mate
7     0        0          7         0x01   mate
8     0        0          8         0x01   mate
```

**所有 31 个 mate 条目的 file_id 都是 0**，但磁盘上只有 `000001.block`（file_id=1），没有 `000000.block`。

#### 发现 B: vpid 5 映射到空白页

```console
$ xxd -s $((13 * 16384)) -l 32 000001.block
00034000: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00034010: 0000 0000 0000 0000 0000 0000 0000 0000  ................
```

page.mate 说 vpid 5 → chunk=0, page_idx=13，但 page_idx=13 在磁盘上全是零。而 page_idx=0~12 全有 `LCBP` 魔数头，是有效页面。

#### 发现 C: 页面实际内容与 mate 映射不符

| page_idx | 存储的 vpid | page_type | mate 映射 |
|----------|-------------|-----------|-----------|
| 0 | 0 | Leaf | vpid 0 → 0 ✓ |
| 1 | 1 | Leaf | vpid 1 → 1 ✓ |
| 2 | 2 | Leaf | **无 mate 映射** |
| 3 | 3 | Leaf | vpid 3 → 3 ✓ |
| 4 | 4 | Leaf | **无 mate 映射** |
| 5 | 5 | Leaf | **无 mate 映射** |
| 6 | 6 | Leaf | vpid 6 → 6 ✓ |
| 7 | 7 | Leaf | vpid 7 → 7 ✓ |
| 8 | 8 | Leaf | vpid 8 → 8 ✓ |
| 9 | 2 | Leaf | **无 mate 映射** |
| 10 | 5 | Leaf | **无 mate 映射** |
| 11 | 4 | Leaf | vpid 4 → 11 ✓ |
| 12 | 2 | Leaf | vpid 2 → 12 ✓ |
| 13 | (空) | - | vpid 5 → 13 ✗ |

vpid 2 有三个副本（page_idx=2, 9, 12），vpid 5 有两个副本（page_idx=5, 10），但 mate 指向的是第 13 页（空白）。

#### 发现 D: WAL 段大量空段

```console
$ wal-list
shard_0.wal.000180   5905 bytes    ← 唯一有数据的段
shard_0.wal.000181 ~ 000213   0 bytes    ← 33 个空段
```

#### 发现 E: flags=0x08 的 freed 页

```console
9  0  0  14  0x08  mate    ← PID_FREED
10 0  0  15  0x08  mate
11 0  0  16  0x08  mate
...
```

10 个 freed 页条目，说明引擎曾正常执行过 compaction 或 overflow 释放。

---

## 3. 根因分析

### 3.1 直接原因

vpid 5 在 page.mate 中映射到 page_idx=13，但该页在磁盘上是全零。引擎读到零页后，`page_type` 字节为 0x00，无法识别为合法的 Leaf(3)/Internal(2)，被上层当作 Meta(1) 处理，类型检查失败。

### 3.2 根本原因：`maybe_periodic_flush` 中间快照竞态

代码位置：`crates/storage/src/pager_backend.rs`（2026-08-02 修复前）

旧版 `maybe_periodic_flush()` 每 10 秒或 256 次写后，会将 **nowchunks 中尚未填满的 chunk** 快照一份放入 WriteQueue。这个快照参与正常的 flush 管道：落盘 → fsync → 插入 chunk_list → 触发 meta flush。

**竞态时序：**

```
  T1: chunk 只有 4 页数据 (page_idx 0,1,2,3)
       maybe_periodic_flush 快照这 4 页 → WriteQueue
  T2: 4 页快照落盘 (含 fsync)
       complete_flush → 插入 chunk_list
       data backlog 清空 → meta_flush_due = true
  T3: page.mate 落盘，包含指向该快照的条目
  T4: chunk 继续写入，填到 64 页
       swap_full_chunk_to_write_queue → 完整 64 页版本 → WriteQueue
  T5: kill-9 发生，完整版本未落盘
       ─────────────────────────────────────────
  重启后：
       page.mate 指向快照中的 page_idx
       但快照只有 4 页，page_idx 4+ 在磁盘上是零
       vpid 5 恰好分配了 page_idx=13 → 读取到零页 → bad page type
```

### 3.3 为什么 data-before-meta 不变性没有阻止

引擎的 data-before-meta 不变性依赖 `flush_backlog() == 0` 闸门。但旧代码中，4 页快照的落盘完成了 `complete_flush`，使 `in_flight` 和 `pending` 都为空，闸门打开，meta 被允许落盘。**问题不在于闸门失效，而在于快照本身是"不完整"的数据——它被当作完整数据对待了。**

### 3.4 修复（2026-08-02）

有两处修复：

**修复 1: `maybe_periodic_flush` 不再快照驻留 chunk**

```rust
// pager_backend.rs:557
// 仅重置计数器 + 触发 WAL seal, 不再快照驻留 chunk
self.write_count_since_flush = 0;
self.last_flush_time = std::time::Instant::now();
Ok(true)
```

持久性由 WAL (periodic/strict) + swap (满 chunk) + 显式 flush (shutdown) 三重保证，不再需要 periodic 快照。

**修复 2: `complete_flush` 增加过时检测**

```rust
// pager_backend.rs:75-100
// 若同 key 有更新版本在 pending 或 in_flight 中 → 丢弃
// 插入前比较有效页数，防止旧快照覆盖新数据
```

---

## 4. 附录：现场数据全貌

### 4.1 page.mate 完整映射

```
flags=0x01 (ALIVE):  vpid 0,1,2,3,4,5,6,7,8,17,18,19,20,21,22,23,24,25,26,29,30
flags=0x08 (FREED):  vpid 9,10,11,12,13,14,15,16,27,28
```

共 21 个存活条目，10 个已释放条目。

### 4.2 块文件有效页分布

chunk 0（page_idx 0~63）中，page_idx 0~12 有 LCBP 魔数，13~63 全零。说明在最后一次落盘时，该 chunk 只写了 13 页。

### 4.3 WAL 段

34 个段中只有 seq=180 有 5905 字节有效数据，其余 33 个段为空。seq 从 180 开始，说明引擎启动后曾经历过至少 179 次 seal 轮次（可能来自之前的运行）。

---

## 5. 优化建议（讨论中，未实施）

### 5.1 meta+data 原子提交

**提议**: 将 dirty meta window 和 data chunk 绑定为一个 io_uring 链，保证要么都落盘，要么都不落盘。

**分析**: 粒度不匹配。
- data 粒度：1 chunk = 1MB = 64 页
- meta 粒度：1 window = 1MB = 131072 个 vpid 槽位
- 一次 meta flush 可能覆盖多个 data chunk 的条目

如果每 flush 一个 data chunk 就跟一次 meta flush，写放大严重（64 页 data → 1MB meta，99.9% 未改动）。当前闸门机制（`flush_backlog() == 0`）在效果上等价但无写放大。

**结论**: 当前设计合理，无需改动。

### 5.2 WAL seal 阈值 256 → 更大

**提议**: 将 `FLUSH_WRITE_THRESHOLD` 从 256 提高（如 65536），减少 WAL seal 频率。

**分析**:
- WAL seal 开销很小（一次 close + 一次 open）
- seal 产生的空段在重启时被 `decode_records` 直接跳过，无影响
- 阈值不影响持久性窗口（Periodic 模式由 1 秒 fsync 决定，不是写次数）
- 256 次写 ≈ 小 value 几十 KB，大 value 也许几 MB

**结论**: 可以改，但收益有限。seal 不是性能热点。

### 5.3 扫描器需要修复的盲区

**问题**: 扫描器按 page.mate 的 file_id 查找 block 文件，但 mate 中 file_id=0 而实际文件是 `000001.block`（file_id=1）。导致扫描器无法读取任何页面。

**建议**: 在 `Locate::resolve()` 中，当 mate 的 file_id 对应的 block 文件不存在时，尝试 file_id + 1。

---

## 6. 数据可恢复性

数据本身是可恢复的。vpid 5 的实际数据在 page_idx=5 和 page_idx=10 都有副本。只需将 page.mate 中 vpid 5 的 page_idx 从 13 修正为 5（或 10）即可恢复。恢复工具可参考 `nexusdb-scanner` 的 `merge` 命令（key-order 遍历 + WAL 重放）。

---

*文档结束*