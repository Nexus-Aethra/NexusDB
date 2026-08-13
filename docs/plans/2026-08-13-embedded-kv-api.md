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
