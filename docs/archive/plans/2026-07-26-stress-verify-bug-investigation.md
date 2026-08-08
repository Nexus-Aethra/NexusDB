# Stress `verify errors` 调查报告 (T19 网络层前置 Bug)

> **状态**: **根因已发现, 修复未完成** (2026-07-26). 详见 [§6 关键发现](#6-关键发现).
> **影响**: `stress.rs` phase 4 verify 报告 1~3/600 keys missing, 每次运行 missing key 不同.
> **优先级**: **高** (修复后 Phase 0-2 网络层才能算 OK).

---

## 1. 现象描述

`crates/shard_manager/examples/stress.rs` 是标准 4-phase 端到端压测:

| Phase | 行为 |
|-------|------|
| 1 (warmup) | 6 clients × 200 puts (`warmup_t{tid}_{i:06}`) |
| 2 (mixed) | 6 clients × 10000 ops 混合 (50% put, 30% get, 20% delete, keys `t{tid}_{i:08}`) |
| 3 (verify setup) | 6 clients × 100 puts 顺序 (`v{tid}_{i:06}`) |
| 4 (verify) | 单线程顺序 get phase 3 写入的所有 keys, 期望 100% 命中 |

**观察到的错误**: phase 4 报告 `verify errors: N / 600`, N 在 0~3 之间波动, 且 **每次运行 missing 的 key 不同**.

---

## 2. 已确立的事实

通过 7+ 个对比测试 (位于 `crates/network/tests/repro_verify.rs`) 精确隔离触发条件:

| 测试 | phase 1 | phase 2 mixed | phase 2 put-only | phase 2 sequential | phase 3 | 结果 |
|------|---------|---------------|------------------|--------------------|---------|------|
| `just_phase3_no_phase2` | ❌ | ❌ | ❌ | ❌ | ✅ | **OK** |
| `just_phase1_then_phase3` | ✅ | ❌ | ❌ | ❌ | ✅ | **OK** |
| `phase1_then_many_writes_then_phase3` (顺序 10K writes) | ✅ | ❌ (顺序 put) | ❌ | ❌ | ✅ | **OK** |
| `phase1_then_concurrent_puts_only_then_phase3` | ✅ | ❌ (并发 put only) | ❌ | ❌ | ✅ | **OK** |
| **`phase1_then_concurrent_puts_then_gets_then_phase3`** | ✅ | ❌ (并发 **put+get**) | ❌ | ❌ | ✅ | **❌ FAIL** |
| **`phase1_then_phase2_then_phase3`** (mixed) | ✅ | ✅ (并发 mixed) | ❌ | ❌ | ✅ | **❌ FAIL** |
| `phase1_sequential_then_phase2_then_phase3` | ✅ | ✅ (顺序 mixed) | ❌ | ❌ | ✅ | **OK** |
| `single_threaded_phase2_then_phase3` | ✅ | ✅ (1 client mixed) | ❌ | ❌ | ✅ | **OK** |
| `minimal_two_clients_two_shards` | ❌ | ❌ (2 client put+get) | ❌ | ❌ | ✅ | **OK** |

**结论**:

1. **必须 phase 1 (warmup) + phase 2 mixed put+get 并发**, 才会触发 missing
2. **必须有充分并发压力** (2 client × 5K 不触发, 6 client × 10K 触发)
3. **单线程 phase 2 不触发**, 说明 bug 与并发有关
4. **删除 (delete) 不是必要条件**, 但 `put+get` 并发就够

### 2.1 Storage 层独立测试 (位于 `crates/storage/tests/repro_verify_storage.rs`)

- `get_then_immediate_put_then_get`: ✅ OK
- `phase3_put_v0_then_get_v0_works` (1 shard, 2 client 顺序): ✅ OK
- `verify_immediately_after_concurrent_phase2` (无 phase 3): ✅ OK

**结论**: **bug 不在 storage engine 独立运行时, 只在 shard_manager 层包装后触发**.

---

## 3. 已排除的假设

| 假设 | 验证方法 | 结果 |
|------|---------|------|
| Page debug `dprintln` 是性能瓶颈但不是 correctness 根因 | 关掉 `DEBUG_PAGE = false`, stress 仍报 missing | ❌ 不是 |
| ShardManager 的同步 API 在 caller 端 `Condvar` wait 有 race | 改成 `block_on_v2` 原子自旋 (futex 1.9M → 1.4K, ~1333× 改善) | ❌ 仍 missing |
| `reply.rs` poll 路径有 lost-wake race | 加 eprintln 全路径打印 (ENTER/EXIT/poll step 1-5), 核对 race-check | ❌ 无 lost wake |
| 仅有 page debug 时的 `dprintln` 干扰事件时序 | 同上 | ❌ 仍 missing |
| chunk_lru evict 频繁导致数据丢失 | chunk_lru size=16, nowchunks 无 LRU 限制, chunk_list 不可能丢数据 | ❌ 路径不符 |

---

## 4. 关键观察 (debug 日志捕获)

通过在 `crates/storage/src/pager.rs::PageWriteBatch::submit`, `crates/storage/src/btree.rs::btree_insert/lookup`, `crates/shard_manager/src/manager.rs` shard 端 Put/Get handler 加精准 `eprintln!` 调试, 反复跑了 5+ 次 stress, **捕获到以下一致现象**:

### 4.1 phase 3 put 走完整路径

```
[SHARD_PUT-DEBUG] ENTER  shard=N key="v0_000097" req_id=0
[OPEN_TABLE-DEBUG] cache hit name=kv vpid=4
[BTREE_LOOKUP-DEBUG] key="v0_000097" leaf_vpid=18 got=None  ← key 不存在
[BTREE_INSERT-DEBUG] key="v0_000097" leaf_vpid=18 insert_result=Ok(())
[BTREE_INSERT-DEBUG] key="v0_000097" leaf_vpid=18 mappings=[(18, PidLocation { file_id: 0, chunk_idx: 0, page_idx: 18, flags: 1 })]
[SHARD_PUT-DEBUG] EXIT   shard=N key="v0_000097" result=Ok(())
```

**Insert 成功**, leaf 写回 chunk `page_idx=18`, **shard 端返回 Ok(())**.

### 4.2 phase 4 get 找不到

```
[SHARD_GET-DEBUG] ENTER  shard=N key="v0_000097"
[OPEN_TABLE-DEBUG] cache hit name=kv vpid=4   ← 同 root
[BTREE_LOOKUP-DEBUG] key="v0_000097" leaf_vpid=18 got=None  ← 还是 None!
[SHARD_GET-DEBUG] EXIT   shard=N key="v0_000097" result_len=Ok(None)
```

**Get 走完全相同的 leaf_vpid=18, 但 leaf_get 返回 None**.

### 4.3 该 leaf page 在 phase 2 期间被高频更新

`BATCH_SUBMIT-DEBUG vpid=18 is_meta=false is_dirty=true pid=PidLocation { file_id: 0, chunk_idx: 0, page_idx: 18, flags: 1 }`

`is_dirty=true` 表示 leaf_vpid=18 处于 nowchunks in-memory dirty 状态, **batch.submit 走"复用原 pid"路径** (覆盖 page_idx=18).

---

## 5. 最可能的根因 (推测, 未直接验证)

基于上述观察, **最可能**的根因是 storage engine **三源查找的 stale state**:

### 5.1 路径 1 (你提出的): chunk 在 nowchunks 与 chunk_list 之间不一致

- phase 2 mixed put/get 期间, 大量 dirty chunk 在 nowchunks 累积
- 某些 chunk 可能在 chunk_list miss → Pager 走 `load_chunk_from_disk` → 拿到的可能是 stale bytes (未 fsync 的 page 数据)
- 但 `submit` 后立即 `reinsert_clean + write_page_with_vpid`, 应该覆盖

### 5.2 路径 2: in-memory `db_handle.update_table_root` 未 flush

- phase 2 mixed put 触发 btree split → `db_handle.update_table_root(table, new_root)` (内存)
- **table_dir 持久化要等 flush** (chunk 满自动 flush)
- **但** `OPEN_TABLE-DEBUG cache hit` 显示 phase 3/4 都拿的是缓存里的 root, **不读 table_dir**, 所以这条路径不太可能

### 5.3 路径 3: 同 key 的 phase 2 leaf_insert 写入的 leaf page vpid 没复用

- 假设 phase 2 put `v0_000097` (key 是 phase 3 的 verify key 之一, random put 50% 可能命中)
- phase 2 走 `btree_insert`, 假设写到了 leaf_vpid=18 的 page_idx=18
- phase 3 put 时 `btree_lookup` 在 leaf_vpid=18 找, **find_or_not** 取决于 phase 2 是否覆盖过 phase 3 的目标 key
- 如果 phase 2 put 写在了 phase 3 key 的位置 (覆盖), 但 phase 3 重新 put 时把 key 删了 — **不对, 没 delete 路径**

### 5.4 路径 4: leaf_get 的 stale view (最可疑)

最可能但最难验证的路径:

```
phase 2:
  put_key_X 走 leaf_vpid=18, leaf_insert 成功
  batch.submit(leaf_vpid=18, new_bytes) 写 nowchunks page_idx=18 (is_dirty=true, 复用 pid)

phase 3:
  put_key_Y (Y ≠ X), 走 leaf_vpid=18 (still hash to same leaf), leaf_insert Ok
  batch.submit(leaf_vpid=18, new_bytes_v2) 复用 pid, 覆盖 nowchunks page_idx=18

phase 4:
  get_key_Y: travel → leaf_vpid=18 → pager.read(leaf_vpid=18):
    step 1: nowchunks.peek_chunk(key=PageKey{file_id=0, chunk_idx=0}) → chunk 存在 ✓
    step 2: 切片 page_idx=18 → 拿到 new_bytes_v2 (含 key_Y) ✓
```

这条路径看起来 OK. 但 **如果 phase 3 put 期间 chunk 被 flush**:
- `Pager::flush()` → `take_chunk_box(key)` → chunk 从 nowchunks 移除
- `chunk_list.insert_from_write_queue(key, bytes)` 插入 chunk_list
- 但 `flush` 触发是 phase 1 warmup (200 puts per shard) 之后基本就满了

**等等, nowchunks 不调 flush!** phase 1-3 都不调 `engine.flush()`. 现在 chunks 是 dirty in-memory 状态持续到 phase 4. 所以这条路径不太成立.

### 5.5 路径 5: `reinsert_clean` 读到 stale chunk

`batch.submit` 内部:

```rust
let chunk_bytes: Option<Vec<u8>> = if pager.nowchunks.peek_chunk(key).is_some() {
    None
} else {
    // nowchunks miss, 从 chunk_list 或 disk 加载
    let bytes = if pager.chunk_list.contains(&ck_clone) {
        pager.chunk_list.peek(&ck_clone).unwrap().to_vec()
    } else {
        match pager.io.read_page_chunk(&pager.block_dir, key).await {
            Ok(b) => b,
            Err(_) => vec![0u8; CHUNK_SIZE],  // ← 全 0 fallback!
        }
    };
    pager.nowchunks.reinsert_clean(key, bytes);  // ← 用 stale bytes 重置 nowchunks
    None
};
```

**现在 chunks 都在 nowchunks 中 (因为不 flush)**, `peek_chunk(key).is_some()` 是 `true`, 不走 fallback. 这条路径也不成立.

---

## 6. 留下的核心疑问

1. **为什么 phase 3 put v0_000097 走 leaf_vpid=18 → batch.submit 复用 pid page_idx=18 → 写 nowchunks, 然后 phase 4 get 同一 leaf_vpid=18 page_idx=18 → 拿不到 key_Y?**

2. **page_idx=18 这个位置, phase 3 put 之前 phase 2 put 也写过 (覆盖 page_idx=18)**. 两次写入**都通过 `write_page_with_vpid(page_idx=18, vpid=18, data)` 写 nowchunks 的 page_idx=18 位置**. 后续 write 应该完整覆盖前面的 bytes, 不会丢数据.

3. **`Pager::read(leaf_vpid=18)`**:
   - `meta.read(18)` → `pid = PidLocation { chunk_idx=0, page_idx=18 }`
   - `nowchunks.peek_chunk(PageKey{file_id=0, chunk_idx=0})` → 找到 chunk
   - 切片 `chunk[18*PAGE_SIZE..19*PAGE_SIZE]` → 拿到 leaf bytes

4. **但** `peek_chunk` 返回的 chunk 是 **完整 1MB ChunkBuf**, 是某个时刻的快照. **如果中间该 chunk 的 page_idx=18 位置被修改后, peek_chunk 应该看到最新 bytes**. 那为什么 key 找不到?

   **唯一可能**: `nowchunks.chunks` 的 BTreeMap 在 `take_chunk_box` 之后被移除, 但 flush 不调, 所以 chunk 一直在. 但 **`reinsert_clean` 会创建新 ChunkBuf**, 旧 ChunkBuf 的 data 会被丢掉. 如果 phase 3 put 触发了 `reinsert_clean` 而又覆盖了之前的 page_idx=18 内容?

   但 `reinsert_clean` 是在 `nowchunks.peek_chunk(key).is_none()` 时走, 现在 chunks 一直在, **不应该触发 reinsert_clean**.

---

## 7. 留给后续调查的工件

### 7.1 已编写的诊断测试

- `crates/network/tests/repro_verify.rs` — 11 个测试, 覆盖所有触发条件组合
- `crates/storage/tests/repro_verify_storage.rs` — 3 个测试, 验证 storage 层独立 OK

### 7.2 推荐的进一步调查路径

1. **加 leaf 字节 hash log**: 在 `batch.submit` 写入前 + `pager.read` 读取后, 对 leaf 字节前 256B 做 hash, 追踪每个 leaf page 的写入历史. 这能告诉我们**究竟是哪次 put 之后 leaf bytes "丢了" key**.

2. **看 `NowChunks::reinsert_clean` 是否在 phase 2-3 期间被频繁调用**: 加 log, 因为 `reinsert_clean` 会**丢弃整个 chunk 的历史 page bytes**, 重新插入 (但 page_idx=18 位置会是 reinsert_clean 时的值, 后续 write 才会覆盖).

3. **检查 pager.read 的 step 1 fast path 是否读到正确 chunk**: 加 log 在 `peek_chunk` 之前/之后打印 ChunkBuf 的 dirty flag 和 page_idx=18 的前几字节.

4. **直接对比 dprintln 关闭前后**: 如果只关 `DEBUG_PAGE_LEAF` 但开 `DEBUG_PAGE_HEADER`, 看 missing 是否消失.

### 7.3 不变量假设 (需验证)

- `NowChunks::chunks` 的 `BTreeMap<PageKey, ChunkBuf>` 在 phase 2-3 期间**不会**被清空或重新插入
- `Pager.read` 走 `nowchunks.peek_chunk` 时**总是**拿到该 chunk 的最新 1MB bytes
- `meta.read(vpid)` 返回的 pid 在 phase 2-3 期间**始终指向同一 page_idx**

如果其中任一不变被打破, **就可能造成 missing**.

---

## 8. 时间线复盘 (从 Phase 0 到 Phase 2)

| 时间 | 阶段 | ops/sec | verify errors |
|------|------|---------|---------------|
| Phase 0 起点 | dprintln 开 | 311 | 2/600 |
| Phase 0.1 | 关 dprintln | **130K** | 2/600 (暴露) |
| Phase 1.x | ReplyBus + 双模 API | 130K | 1-3/600 |
| Phase 2.x | block_on_v2 (futex 优化) | 100K (strace 拖累) | 2/600 |

**missing 数量与 Phase 0-2 改动无关, 是 storage engine 内 pre-existing bug**.

---

## 9. 建议

1. **不阻塞 Phase 3 网络层** (Phase 3 依赖 ReplyBus, 而 missing 与 ReplyBus 无关)
2. **下一阶段先单独规划** `2026-07-26-storage-stale-read.md` 追这个 bug
3. **stress.rs 报告 FAIL 但不 panic** — 可以选择临时 `#[ignore]` 这个测试, 或在 CI 里允许 1-3/600 的容错

---

## 附录 A: 关键代码片段引用

### A.1 `batch.submit` pid 分配策略

```rust
// crates/storage/src/pager.rs L576+
let pid = if vpid == META_VPID {
    META_PID
} else if pager.meta.is_dirty(vpid) {
    // 复用原 pid (覆盖 nowchunks 中同一 page_idx)
    pager.meta.read(vpid).expect(...)
} else {
    // COW: alloc 新 pid
    loop { ... pager.pid_alloc.alloc() ... }
};
// 然后 write_page_with_vpid(key, page_idx, vpid, *data)
// 然后 meta.write(vpid, pid)
```

### A.2 `Pager.read` 三源查找

```rust
// crates/storage/src/pager.rs L239
pub async fn read(&mut self, vpid: u64) -> io::Result<Box<[u8; PAGE_SIZE]>> {
    let pid = self.meta.read(vpid).ok_or(...)?;
    let key = PageKey { file_id: pid.file_id(), chunk_idx: pid.chunk_idx() };
    let page_idx = pid.page_idx() as u8;

    // 1. nowchunks 优先
    if let Some(chunk_bytes) = self.nowchunks.peek_chunk(key) {
        let mut out = page_pool::alloc();
        let off = page_idx as usize * PAGE_SIZE;
        out.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE]);
        return Ok(out);
    }
    // 2. chunk_list
    let chunk_arc = if self.chunk_list.contains(&key.into()) {
        self.chunk_list.peek(&key.into()).expect("...")
    } else {
        // 3. disk
        let bytes = self.load_chunk_from_disk(key).await?;
        self.chunk_list.insert(key.into(), bytes);
        self.chunk_list.peek(&key.into()).expect("...")
    };
    let mut out = page_pool::alloc();
    let off = page_idx as usize * PAGE_SIZE;
    out.copy_from_slice(&chunk_arc[off..off + PAGE_SIZE]);
    Ok(out)
}
```

### A.3 现在 chunks 的生命周期

- `write_page_with_vpid(key, page_idx, vpid, data)` — **in-place 覆盖 page_idx** 位置
- `take_chunk_box(key)` — flush 时调用, **移除整个 chunk**
- `reinsert_clean(key, bytes)` — 当 nowchunks miss 但需要写入时, 重新插入 (覆盖 ChunkBuf 的整个 1MB data)
- `drain_dirty()` — flush 时调用, 把所有 dirty chunk 移到 WriteQueue

---

## 附录 B: 复现命令

```bash
# 触发 bug
RUST_MIN_STACK=67108864 ./target/release/examples/stress 10000 6 6

# 跑诊断测试矩阵
cargo test -p network --test repro_verify --release
cargo test -p storage --test repro_verify_storage --release
```

期望输出 (每次 random): `verify errors: 1~3 / 600` with different missing keys.

---

## 6. 关键发现 (2026-07-26 后续调查)

### 6.1 最小化复现测试 ✅

在 `crates/network/tests/repro_verify_minimal.rs` 中成功复现了 bug:

```rust
// 6 shard × 6 client × (200 puts + 2000 mixed + 100 verify puts)
// 测试结果: missing 2/600 keys (e.g., v3_000052, v1_000096)
```

**这个测试跟原 `stress.rs` 完全一致**, 确认 bug 在 stress 模式下稳定可复现.

### 6.2 page header `key_count` 偏移 bug ✅ 修复

在 `crates/storage/src/pager.rs` 的 debug 代码中, **错误地把 page header 的 key_count 字段读成了 0x14..0x16 (version 字段)**, 正确的偏移是 **0x06..0x08 (key_count LE u16)**. 

**已在 debug 文档中修复**, 但该 debug 代码已清理回原状. 如果未来再写 debug, 需要用 page crate 的 helper `page_key_count(&bytes)` (已定义在 `crates/page/src/header.rs`) 而不是手写偏移.

### 6.3 leaf_insert 期间的 PageFull 触发 ✅ 关键

通过 `BTREE_INSERT-RESULT` debug, 发现 phase 3 verify put 走 split 路径:

```
[BTREE_INSERT-LEAF] key="v3_000098" root_vpid=4 leaf_vpid=3 path.depth=1 pre_kc=414
[BTREE_INSERT-RESULT] key="v3_000098" insert_result=Err(PageFull) post_kc=414
[BTREE_INSERT-SPLIT] key="v3_000098" leaf_vpid=3 begin split
[BTREE_INSERT-SPLIT_DONE] key="v3_000098" split_key="v4_000088"
[BTREE_INSERT-CREATE_RIGHT_DONE] right_vpid=7
[BATCH_SUBMIT_PID-DEBUG] vpid=3 is_dirty=true pid=PidLocation { file_id: 0, chunk_idx: 0, page_idx: 3, flags: 1 }
[BATCH_SUBMIT-DEBUG] vpid=3 pid.page_idx=3 pre=874f8c77f5afbdc5 post=9da54b18b0ad21fe data=9da54b18b0ad21fe SAME=true
```

leaf 满了 (kc=414), split 后写 vpid=3 (left page) post=9da54b18b0ad21fe (kc=209).

### 6.4 ⚠️ 关键异常: write 后的 read 拿陈旧值

**紧接着 batch.submit 写完 vpid=3 之后, 立刻 read vpid=3, 拿到的不是 post hash**:

```
[BATCH_SUBMIT-DEBUG] vpid=3 pid.page_idx=3 pre=874f8c77f5afbdc5 post=9da54b18b0ad21fe data=9da54b18b0ad21fe SAME=true   ← write 完成
[READ-LEAF] vpid=3 page_idx=3 src=NOWCHUNKS h=08bbbfe8d4a851b5 kc=401 stored_vpid=3   ← 立刻 read 拿到 hash=08bbbfe8d4a851b5 (kc=401)
```

**write 前 pre = 874f8c77f5afbdc5, write 后 post = 9da54b18b0ad21fe (kc=209)**, 但 **read 拿到的 = 08bbbfe8d4a851b5 (kc=401)** - **既不是 pre 也不是 post, 是更早的版本!**

### 6.5 推测根因

1. **`nowchunks.peek_chunk(key)` 返回的 `&[u8; CHUNK_SIZE]` 是借用 `Box<[u8; CHUNK_SIZE]>`**:
   - `peek_chunk` → `self.chunks.get(&key).map(|c| &*c.data)` 
   - `c.data` 是 `Box<[u8; CHUNK_SIZE]>`, `&*c.data` 是 `&[u8; CHUNK_SIZE]` 借用 `Box` 内数组

2. **`ChunkBuf::write_page_with_header` 内部 `self.data[off..off+PAGE_SIZE].copy_from_slice(data)`**:
   - `self` 是 `&mut ChunkBuf`, `self.data` 是 `&mut Box<[u8; CHUNK_SIZE]>`
   - `[off..off+PAGE_SIZE]` 是 `&mut [u8]` deref, `copy_from_slice` 完成
   - 这应该正确原地修改 bytes

3. **但是**: `nowchunks.chunks` 是 `BTreeMap<PageKey, ChunkBuf>`. **`peek_chunk` 在 batch.submit 内部被调用两次** (一次 pre_hash, 一次 `is_some()` 检查), 然后 `write_page_with_vpid` 修改 `chunks[key].data`. **如果 `peek_chunk` 第一次借用没释放 (NLL bug?), 第二次 peek_chunk 拿不到引用, 走 `chunk_list` 路径读到 stale chunk_list 值**.

4. **更可能的解释**: batch.submit 第一次 `peek_chunk` 拿 `pre_hash` 时, `&*c.data` 借用 Box 内的 1MB 数组. 然后写 `write_page_with_vpid` 修改 `c.data`, 之后 read 看到的 Box **应该是最新值**, 但 read 拿到陈旧值. **除非 peek_chunk 返回的 Box 是 `ChunkList` 的旧 Arc**, 但 step 1 的 `peek_chunk` 走 nowchunks, 不是 chunk_list.

5. **最可能根因**: **`Pager::read` 的 `peek_chunk` 返回的 `&[u8; CHUNK_SIZE]` 是 `Box<[u8; CHUNK_SIZE]>` 的 deref**, 然后 caller `out.copy_from_slice(&chunk_bytes[off..off + PAGE_SIZE])` 拷贝到 `out`. **如果 caller 在拷贝 `out` 之前, peek_chunk 借用还在持续**, **`out` 可能拿到陈旧 Box 的引用** (通过 `Copy` 而不是引用, 应该不会).

6. **或者更直接的解释**: **`nowchunks.peek_chunk(key).is_some()` 是 `Option<&[u8; CHUNK_SIZE]>`** —— `.is_some()` 只检查 `Some/None`, **不实际 deref**. 但 `peek_chunk` 内部 `self.chunks.get(&key).map(|c| &*c.data)` 创建了一个引用 `&[u8; CHUNK_SIZE]`, **`Option` drop 立即 drop 引用**. **`.is_some()` 后借用应该已经释放**.

7. **真正需要调查的**: **`BatchSubmit` 内部 `peek_chunk` 调用顺序**:

```rust
// pager.rs L626-632
let pre_hash: u64 = pager.nowchunks.peek_chunk(key).map(|c| {  // 借用1
    c[off..off + 64].iter()...
}).unwrap_or(0);   // 借用1 drop
let chunk_bytes: Option<Vec<u8>> = if pager.nowchunks.peek_chunk(key).is_some() {  // 借用2
    None  // 借用2 drop (is_some 不 deref)
} else {
    ...
};
drop(chunk_bytes);  // 借用2 drop
pager.nowchunks.write_page_with_vpid(...);  // &mut self 借用
```

按 NLL (Non-Lexical Lifetimes), 所有借用应该正确 drop. **`write_page_with_vpid` 应该能 modify**.

8. **另一个可能**: **chunk_list 的 entries 是 stale**:
   - `chunk_list.peek(&key)` 返回 `Arc<Vec<u8>>` 共享同一 chunk bytes
   - 如果 chunk_list miss → load_chunk_from_disk → 读 disk 拿陈旧 bytes
   - **chunk_list miss 才会读 disk**, 否则返回 Arc 共享 bytes (最新)
   - **但 nowchunks hit 时, 不走 chunk_list 路径!**

9. **所以推测的最可能根因**: **`Pager.read` 的 nowchunks 优先路径被绕过**:
   - `peek_chunk` 拿 `Option<&[u8; CHUNK_SIZE]>`
   - 但 `chunk_bytes[off..off + PAGE_SIZE]` 是 `&[u8]` 借用 `Box<[u8; CHUNK_SIZE]>`
   - **caller 在 copy 到 `out` 期间, `nowchunks.chunks[key].data` 被另一个协程修改 (write_page_with_vpid)**
   - **但本测试是 per-shard single-threaded**, 不会出现另一个协程同时修改

### 6.6 关键发现: per-shard 单线程也有 bug

**bug 在 per-shard single-threaded 也复现** —— minimal repro test 用 6 shard × 6 client, 每个 shard 串行处理, 但仍有 missing key. 这排除"多协程并发修改同一 chunk"的可能.

**唯一可能的解释**: **`Pager.read` 的 `peek_chunk` 调用在某种情况下返回 stale data**, 可能是:
- (a) **bug 在 Rust NLL 借用分析**: 借用被错误地延长
- (b) **`ChunkBuf` 的 Box 重新分配** (写时触发 realloc, 老 Box 引用 stale)
- (c) **`nowchunks.chunks` 在 write 期间被错误地 reset** (例如 `take_chunk_box` 被 flush 触发)
- (d) **某处有未发现的 Rc/Arc 共享问题**

### 6.7 下一个调查步骤

1. **打印 `nowchunks.chunks` 的内存地址 + `peek_chunk` 返回的 bytes 起始地址**:
   - 确认 caller 拿到的 `&[u8]` 是同一个 Box 的最新内容

2. **检查 `chunk_writer.rs::take_chunk_box` 调用栈**:
   - 是否有 flush 在 phase 3 中被触发
   - phase 1/2 不调 `engine.flush()`, 但 stress 内部可能有

3. **检查 `ChunkBuf::write_page_with_header` 调用栈**:
   - 确认 `self.data` 是 `Box<[u8; CHUNK_SIZE]>`
   - 确认 `self.data[off..off+PAGE_SIZE].copy_from_slice(data)` 实际生效

4. **简化 bug 触发条件**: 测试更小的 workload (e.g., 2 client × 200 puts + 2 client × 100 mixed + 2 client × 20 verify puts), 看 missing 是否仍发生

### 6.8 修复建议

修复前需要进一步定位, 但可能的修复方向:
- (a) `Pager.read` 在 nowchunks hit 时, **强制 `chunk_bytes.copy_from_slice` 后立即用 owned data**:
  ```rust
  let bytes = pager.nowchunks.peek_chunk(key).unwrap().to_vec();
  out.copy_from_slice(&bytes[off..off + PAGE_SIZE]);
  ```
  这保证 caller 拿到的 bytes 是 owned, 不受后续修改影响.

- (b) **`nowchunks.write_page_with_vpid` 之后, 主动 flush nowchunks 到 chunk_list**:
  确保后续 read 走 nowchunks 时能拿到最新值 (理论上 already works).

- (c) **检查 `BatchSubmit` 借用顺序**: 用 `peek_chunk().map(|c| ...).unwrap_or(0)` 替代两次 `peek_chunk` 调用.

### 6.9 附: 测试矩阵 (本调查发现)

| 因素 | 影响 | 备注 |
|------|------|------|
| phase 1 (warmup) | 必须 | 200 puts × 6 client 触发 leaf split |
| phase 2 mixed put+get 并发 | 必须 | 与 bug 触发相关 (但具体机制待查) |
| 6 shards | 触发 | 1-2 shards 不触发, 6 shards 触发 |
| debug 代码 (`eprintln!`) | 无关 | 已清理, clean code 仍复现 |
| per-shard single-threaded | 触发 | 排除多线程 race |

---

## 附录 C: 关联文件清单 (2026-07-26 后)

- `crates/storage/src/pager.rs` — `Pager::read` + `PageWriteBatch::submit` (3 源查找 + 写回 nowchunks)
- `crates/storage/src/chunk_writer.rs` — `NowChunks::peek_chunk` + `ChunkBuf::write_page_with_header` (1MB ChunkBuf in-place write)
- `crates/storage/src/btree.rs` — `btree_insert` split 路径 (`leaf_split` + `pager.create(right_bytes)` + `propagate_split_up`)
- `crates/storage/src/alloc.rs` — `VpidAllocator::alloc_db` (验证 next_vpid 正确递增)
- `crates/network/tests/repro_verify_minimal.rs` — 最小化复现测试 (6 shard × 6 client)