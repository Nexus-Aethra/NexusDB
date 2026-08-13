# RESP ↔ SQL Schema Adapter 开发计划

## 目标

让同一张 **SQL mode** 表可被 SQL 和 RESP 双向、即时地访问，而不是把 SQL
row 复制或投影为另一份 Redis Hash 数据。RESP 请求编译为已有的 schema-aware
row/index 操作，因此类型、约束、二级索引、WAL 和 shard 路由只有一份真相源。

示例：

```text
HGET    user:5 name
HMGET   user:5 name email
HGETALL user:5
HSET    user:5 name Alice age 30
HINCRBY user:5 login_count 1
DEL     user:5
```

等价于针对 `user` 的按主键查询或单行更新。SQL 写入立即可见于 RESP，
RESP 写入立即可见于 SQL；不引入同步任务、影子 Hash 或双写。

## 当前边界

- RESP 已将 `table:key` 的首个冒号前缀路由到表；例如 `user:5` 会路由到
  `user`，余下 key 为 `5`。
- SQL row 使用 `TAG_ROW` 和 `TableSchema` 编码；通用 RESP Hash 使用独立
  keyspace。schema 表上的普通 `HSET` 当前返回 `WRONGTYPE`，这是正确的隔离。
- 行按主键 hash 分 shard；主键是唯一支持单行 RESP 命令的定位方式。

## 语义与兼容性

### 表模式

表模式沿用已有的持久化边界，并增加显式开关：

```text
Kv   : 无 schema 的现有 Redis String/Hash/List/Set/ZSet 表。
Sql  : 有 TableSchema 的 row 表；可显式启用 RESP row adapter。
```

模式必须由 DDL/表元数据声明，不能根据一次 RESP 命令猜测。现有无 schema 表
迁移为 `Kv`；现有 SQL schema 表初始为 `Sql` 但 RESP adapter 默认关闭，需显式
启用，避免改变已有 `WRONGTYPE` 契约。

### RESP key grammar

SQL adapter 的 v1 key 为：

```text
<table>:<primary-key-literal>
```

- 首个 `:` 分隔 table 与主键字面量；其余字节都属于主键字面量，因此字符串主键
  可以包含 `:`。
- 主键列由 `TableSchema.pk_col` 唯一确定。literal 按该列类型转换，绝不做 SQL
  文本拼接，也不提供非主键检索语法。
- 类型不匹配必须返回明确 RESP error，例如 `id BIGINT` 表上的
  `HGET user:not-a-number name` 返回整数类型错误；不得退化为 Hash miss 或扫描。
- `HGET user:5 name` 只返回一行的一列；`HGETALL` 按 schema 列顺序返回
  `(column, value)`。SQL NULL 在 RESP Hash 视图中等价字段不存在：单字段读取为
  RESP nil，`HGETALL/HKEYS/HVALS/HSCAN/HLEN/HRANDFIELD` 均跳过该列。

### 主键定位规则

| 定位方式 | 单行 HGET/HSET | 路由与执行 |
|---|---|---|
| schema 主键 | 支持 | canonical PK 后直接路由到一个 shard |
| 非主键索引 | 无单行语法 | 交给多行 `HQUERY` |

`HSET/HMSET` 只允许按 schema 主键定位一行。更新主键、生成列或受保护列必须
拒绝；不存在行时，只有调用提供所有必填列或可由默认值补齐时才可执行 UPSERT。
`HDEL table:pk field...` 映射为 shard 内原子 `RowUnset`：仅允许 nullable 的
非主键列，并将字段设为 SQL `NULL`。返回值是实际从非 NULL 变为 NULL 的字段数；
重复字段按 Redis 语义只计一次。`DEL table:pk` 仍映射为 schema-aware RowDelete。

### 多行与普通索引

不改变 Redis `HGET` 的单 bulk-string 返回类型。新增 Nexus 扩展命令：

```text
HQUERY user WHERE age = 20 FIELDS id name LIMIT 100
HQUERY user WHERE score BETWEEN 60 90 FIELDS id name
```

它复用 SQL planner 的索引选择、范围扫描、跨 shard gather、投影和 LIMIT；不把
多行结果偷偷塞进 `HGET` 或让 `HSET` 对多行产生意外 UPDATE。

## 实施阶段

### P0：元数据与协议护栏

状态：已完成（2026-08-12）。

1. 在 SQL `TableSchema` 中持久化 `resp_row_adapter_enabled`，升级格式版本并保持
   旧 schema 解码兼容；无 schema 表继续天然是 Kv mode。
2. 扩展 SQL DDL：创建/修改表模式的显式语法；KV mode 禁止 SQL schema row，SQL
   mode 保留当前 schema 约束。
3. 建立 RESP schema cache，复用 SQL 的 DDL epoch 失效机制；cache miss 使用异步
   `GetSchemaOp` 后恢复原 RESP 请求，禁止 worker 同步等待。
4. 在 RESP dispatch 中先完成既有 `table:key` 路由，再识别 SQL adapter key。
   adapter 未启用时，完全保持原 RESP 行为。

验收：旧 KV/Hash/SQL 回归零变化；重启后表模式与 adapter 开关保持一致；RESP
schema cache 在 DDL 后不使用旧 schema。

### P1：主键单行读写

状态：已完成（2026-08-12）。已支持主键直连语法的
`HGET/HMGET/HGETALL/HKEYS/HVALS/HLEN/HEXISTS/HSCAN`、`HSET/HMSET` 与 `DEL`、
`HINCRBY` 与 `HINCRBYFLOAT`：
读操作经异步 schema 探测后直接使用 `RowGet`，写操作在目标 shard 用一次
`RowPatchUpsert` 原子完成，均有 ColValue→RESP 文本渲染及 KV 回退。不存在 row 的
HSET/HMSET 会按默认值补全并接受 schema 校验，无法构成完整 row 时明确报错。数值自增使用 shard 内 `RowIncr`，不经过
worker 读改写。多 key DEL 已支持 SQL/KV 混合聚合；字段级 HDEL 与 HSETNX 已分别
通过原子 RowUnset/RowSetNx 支持。HSETNX 仅会在现有 row 的目标列为 NULL 时写入；
不存在 row 时明确报错，不构造绕过约束的部分 row。

1. 新增 typed `BatchOp`：`RowRespGet`、`RowRespGetManyFields`、`RowRespPatch`、
   `RowRespDelete`，禁止将 RESP 内容二次拼为 SQL。
2. 复用现有 column literal conversion、row decode/encode、索引维护与约束检查；
   增加 row projection/render helper，将 `ColValue` 渲染为 RESP bulk-string。
3. 支持 `HGET`、`HMGET`、`HGETALL`、`HSET/HMSET`、`HINCRBY`、`DEL` 的主键路径。
4. `RowRespPatch` 在同一 row shard 内一次完成 read/validate/index-update/write，
   保持 HSET 的多 field 原子性。

验收：SQL→RESP、RESP→SQL 双向可见；INT/DECIMAL/BOOL/时间/JSON/UUID/NULL 渲染
正确；主键字面量类型错误明确拒绝；UNIQUE/NOT NULL/外键与二级索引在 RESP 更新后保持一致。

### P2：HQUERY 多行入口

状态：已完成 v1（2026-08-13）。

1. 定义严格、有限的 RESP 参数语法：单表、AND 合取、Eq/范围、FIELDS、LIMIT。
2. 直接构造现有 SQL planner AST/计划，不解析自由 SQL 文本。
3. 复用 `ScanFiltered`、IndexHint/KeySetHint 和 gather 完成点；RESP 数组返回行数组。
4. v1 限制结果数、投影列数和 predicate 数；JOIN、子查询、OR 以后续版本支持。

实现语法为 HQUERY table WHERE col op value [AND ...] FIELDS col... LIMIT n。op
限 =/>/>=/</<=，LIMIT 为 1..10000，条件最多 8 个、投影最多 32 列。执行复用
SqlStmt::Select、既有索引规划和跨 shard gather；响应是按 FIELDS 顺序的 RESP
二维数组，SQL NULL 为 RESP nil。

验收：普通索引 Eq/范围查询可走索引；无索引查询被拒绝或要求显式 scan 开关；跨 shard
结果、LIMIT 与 SQL 对拍一致。

## 测试矩阵

- 协议：RESP2 原生命令、pipeline FIFO、AUTH/SELECT 后的 current db 隔离。
- 互操作：SQL INSERT/UPDATE/DELETE 后 RESP 读取；RESP patch/delete 后 MySQL 与
  PostgreSQL 查询、预处理结果和索引扫描一致。
- 正确性：schema 变更、NULL、默认值、主键类型转换失败、非法列、重启/WAL 恢复。
- 分片：PK 单 shard、HQUERY fan-out。
- 性能：分别测 PK HGET/HSET 与 HQUERY；与 SQL PK 点查和
  原生 KV Hash 基线对照，确保 adapter 不影响 Kv mode 热路径。

## 非目标

- 不将 SQL row 镜像为 Redis Hash，也不允许两个编码同时成为真相源。
- 不让 HGET/HSET 对任何非主键索引隐式返回/更新多行。
- P0-P1 不支持复合主键、JOIN、任意 SQL 文本或 Redis Lua/事务语义映射。
