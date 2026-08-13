# 嵌入式 KV API 执行计划

## 目标

将 NexusDB 从仅独立服务扩展为可被 Rust 项目直接依赖的库：同一套 shard、存储格式和 WAL，
不启动网络监听即可选择数据库/表并执行同步或异步 `set`、`get`、`del`。

## P1：最小稳定门面

1. `[x]` 新增根 `nexusdb` library target，公开 `NexusDb`、`Database`、`Table` 和 `EmbeddedOptions`。
2. `NexusDb::open` 复用 `ShardManager::open`，默认 `StdFs`，不启动 network/log server。
3. `create_database` / `database`、`create_table` / `table` 分别处理显式创建和轻量选择。
4. Table 提供同步与 runtime-agnostic async `set/get/del`；嵌入 API 自动处理内部 value type tag，
   与 RESP/Binary 数据互通。
5. `[x]` `close(self)` 要求先释放选择句柄，随后调用 `ShardManager::close`，保证 shard/WAL 收尾。

验收：单元测试覆盖多 shard、库表选择、同步与异步读写、删除和显式关闭；Linux 全量编译不启动
网络监听。

## P2：生产化补全

1. `[x]` 为选库增加存在性校验与幂等 `ensure_database` API；表选择保持轻量，持久化表
   则通过显式 `create_table` 在所有 shard 创建。
2. `[x]` 增加按 shard 聚合的 `set_many/get_many`。
3. `[x]` 定义结构化 `EmbeddedError`，区分关闭句柄和底层引擎错误。
4. `[x]` 添加 `examples/embedded_kv.rs`、同步/异步测试，以及显式关闭后重新打开同一
   数据目录的恢复测试。

## 非目标

- P1 不暴露 SQL/RESP 协议解析或网络 server。
- P1 不自行绑定 Tokio；async 方法只返回 Rust Future，调用方选择运行时。
- P1 不改变 B-tree、WAL 或 shard 调度行为。

---

## P3：Scan + Typed 扩展（2026-08-13 第二阶段）

P1/P2 落地后的实际业务反馈集中在两类诉求：

1. "我有一组 name，要拿到每个 name 对应的 id" —— 业务侧不想先 `list()` 再 `get_many()` 两步走，希望单次往返拿 `(name, typed_value)`。
2. "key 是按字典序排的，我想拿 `[a, c)` 区间" —— 时序/分桶/分页场景。

底层 `storage::registry::table_scan_prefix` / `table_scan_range` 早就存在（SQL
索引和复合结构都用过），但嵌入式 API 没透出。本阶段把它接到 `Table` 上。

### 实施项

1. `BatchOp::ScanKeys { db, table, start, end, prefix, limit, with_values }`
   + `BatchResult::Keys(Vec<Vec<u8>>)` / `KeysWithValues(Vec<(Vec<u8>, Vec<u8>)>)`。
   - 范围闭开 `[start, end)` (BTree 字节序)，start/end 各自可空
   - prefix 与 start/end 独立，三者可任意组合
   - `with_values` = false 只返 key；= true 返 (key, stored_value_with_tag)
2. `exec_scan_keys` (shard 端)：走 `table_scan_prefix` 跨 leaf 游标，callback 里
   剥 `[S][varint klen][user_key]` 物理前缀拿 user_key；`end` 命中即 `Break` 早停；
   `prefix`/`start` post-filter。
3. `ShardManager::scan / scan_with_values` 跨 shard fan-out + 各 shard 局部有序
   + 全局 sort + `HashSet` 去重 (路由变更兜底)；同步走 `block_on_v2`。
4. `ShardManager::scan_async / scan_with_values_async / batch_ops_async` 三个
   async 原语，跨 shard 并发 `await` + 归并，runtime 无关。
5. `Table` 公开：
   - 同步：`list / list_prefix / list_limit / list_range / list_range_prefix`
     + `list_typed / list_typed_limit / list_typed_range / list_typed_range_prefix`
     + `get_typed`
   - async：上述各方法 `*_async` 对应 + `set_many_async / get_many_async /
     get_many_typed_async / get_typed_async`
6. `enum TypedValue { Raw/Int/Float/Float32/Str/Doc/Unknown }` + `as_i64 / as_f64 /
   as_bytes / type_name / raw_bytes` 强类型 unwrap
7. `decode_typed` 容错：未知 tag / 长度异常 / 空 stored → `Unknown` 不 panic
8. `EmbeddedError::Scan(String)` 错误变体
9. 文档：`docs/EMBEDDED-KV.md` 重写加 scan + typed 节；`README.md` Embedded
   Library 节扩写；`CHANGELOG.md` F84 条目；本计划追加 P3 节

### 验收

- `cargo test -p NexusDB --lib` 11/11 passed (含 scan/typed/async 8 个新测试)
- workspace 全量 862 → 867 (+5 NexusDB lib)，其它 crate 零回归
- `examples/embedded_scan.rs` (同步) + `examples/embedded_scan_async.rs`
  (异步) `cargo run --example` 实机通
- 范围闭开边界用例：`[bb, d)` over `[a,b,c,d,e]` 返 `[c]`；`[c, c)` 返 `[]`；
  start 不存在正确跳过；end 命中即停 (BTree 序保证)

### 范围与限制 (v1)

- **范围/前缀/limit 全 callback post-filter**：物理 `start` 需 `[S][varint klen]`
  但 user 提供的 start varint 不确定，难精确构造；用 callback 过滤换正确性
  (几十到几百条 cost 忽略；10w+ 留 v2 加 smart physical start)
- **不跨 hash/set/list/zset**：scan 只走 `[KIND_STRING]`，故意避免与
  HKEYS/SMEMBERS/LRANGE/ZRANGE 冲突
- **混合类型**：嵌入式 `set` 写 TAG_RAW，混合类型只能借助 network 层 INCR；
  嵌入式直写 int/float 留 v2
- **async list 不分页 / 不流式**：一次拿 `Vec<Vec<u8>>`；分页用
  `list_range(start, end, limit)` 多次调用
- **零新外部依赖**：所有用到的底层 API (`table_scan_prefix` / `value_num` /
  `BatchOp` / `PendingReply`) 早已存在，仅接线 + 文档
