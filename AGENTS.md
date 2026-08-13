# NexusDB — Agent Handoff Notes

> 给接手这个项目的 agent / 协作者. 读完这份文件你就知道现在到哪、下一步怎么走.
>
> **配套文档**:
> - [`CHANGELOG.md`](./CHANGELOG.md) — 修复历史 (F1-F41) + 测试进度快照 + gotchas + 测试文件清单
> - [`DESIGN.md`](./DESIGN.md) — 项目总设计 (10 节)
> - [`docs/README.md`](./docs/README.md) — 文档导航（活跃计划、事故报告和归档）
> - [`docs/bug-report-btree-split-routing.md`](./docs/bug-report-btree-split-routing.md) — stress 丢 key 根因调查报告

## 项目是什么

NexusDB: 面向写密集/低延迟/高并发的**独立单机数据库服务** (2026-07-25 从嵌入式引擎定位演进), Rust 2024.
- 设计哲学: Share-Nothing + Per-Core Thread + io_uring + 自实现协程调度器
- 长期目标: 多协议统一接入 (Redis ✅ / PostgreSQL / MySQL / Mongo 待实施) + 数据互联 (统一记录编码, value type tag 已预留)
- 子 crate:
  - `crates/scheduler` — 单线程协程调度器 + io_uring 桥 (✅ 完成)
  - `crates/page` — LCB-Tree 页操作: 叶子/非叶子节点 + checkpoint + 前缀压缩 (✅ 完成)
  - `crates/storage` — 物理持久化层: vpid→pid 映射 / chunk LRU / nowchunks / 崩溃恢复 / 多 db 多表 catalog / **自动持久化 + 异步 chunk 落盘 (F41)** (✅)
  - `crates/network` — 网络层: acceptor + epoll worker + **双协议门面 (Binary + RESP2/Redis 兼容含 AUTH)** + KvLimits 校验 + value type tag (✅)
  - `crates/shard_manager` — 多 shard 控制器 + hash 路由 + 2PC + **TaskInbox/TaskReplyBus 直连架构** (✅)
  - `crates/config` (TOML) / `crates/logging` (nlog, io_uring 协程融合 logger) (✅)
  - 根 binary `src/main.rs` — 服务器入口: `nexusdb --config nexusdb.toml`, Binary(5433, 内部) + RESP(6379) + MySQL wire(5434) + PostgreSQL wire(5435) + REST HTTP(6778) 五监听, 信号优雅退出

## 当前进度

### 2026-08-01 会话十九-D 总览 (F83 TLS 传输加密 — rustls STARTTLS, 安全 P0 收官, 细节见 CHANGELOG)

- **SQL 双门面 TLS** (opt-in)。唯一新增外部 crate: `rustls 0.23` + **ring 后端** (避 aws-lc cmake); 手写 PEM 解析 (tls.rs, 复用 crypto::base64_decode)。
- 传输层最小侵入: `ConnState.tls: Option<Box<ServerConnection>>` + start_tls; recv (read_tls→process→冲刷→读明文) / send_bytes (writer→write_tls 泵) TLS 分支; 沿用 spin-flush **不引 EPOLLOUT** (v1); 协议解析层无感知。
- STARTTLS: PG SSLRequest→'S'+升级 (未配→'N' 回退); MySQL `build_handshake_v10_caps` 宣告 CLIENT_SSL + 短包检测 SSLRequest→升级 (未配→1043)。config tls_cert/tls_key + main.rs 注入 SQL/PG (RESP/HTTP/Binary None)。
- 实机: psycopg3 sslmode=require **ssl_in_use=True** SCRAM-over-TLS OK / sslmode=disable 明文回退; mysql-connector ssl_disabled=False OK / True 明文。全量 **862/862** (明文零回归) clippy 0。
- **P0 (数据类型 F80/F81 + 安全 F82/F83) 全部完成**。边界: opt-in 不配证书=纯明文零成本; 无客户端证书认证/无 channel binding/spin-flush 无写缓冲。

### 2026-08-01 会话十九-C 总览 (F82 认证升级 — PG SCRAM-SHA-256 + MySQL caching_sha2, 细节见 CHANGELOG)

- **安全 P0 认证** (不含 TLS=F83)。零依赖手写密码学 `protocol/crypto.rs`: SHA-256/HMAC-SHA256/PBKDF2/base64 (RFC 向量过) + CSPRNG (`/dev/urandom`)。不引 rustls/sha2 (留 F83 才需 rustls)。
- **PG SCRAM-SHA-256** (RFC 5802): 消除明文口令。pg.rs SASL 帧 (code 10/11/12) + ScramState + scram_server_first/verify_final (从明文 sql_password 现场派生, proof 验证); worker pg_phase SASL 两步交换 + ConnState.pg_scram。破坏性: PG 认证 cleartext→SCRAM。
- **MySQL caching_sha2 fast-auth** (additive 非破坏): caching_sha2_password_ok (服务端知明文直接验证, 免 RSA/TLS) + fast_auth_success(0x01 0x03); 失败/其他保留 AuthSwitch→native 兜底; mysql_gen_salt 改 CSPRNG。
- 实机 (sql_password=s3cret): psycopg3 SCRAM 登录+查询 OK/错口令拒; mysql-connector 默认+显式 caching_sha2 均 OK/错口令 1045。全量 **862/862** clippy 0。
- 边界: 单一 sql_password (无 per-user); 无 channel binding; **传输加密 TLS 仍缺 → F83** (最后阶段)。

### 2026-08-01 会话十九-B 总览 (F81 DECIMAL 定点小数 — P0 数据类型第二阶段, 细节见 CHANGELOG)

- **金额精确类型 DECIMAL** (唯一动 row/keyspace 结构的类型)。承载双源: **`ColType::Decimal{precision,scale}`** (scale 入类型→转换/渲染/DDL 零签名改动, 避免 Column 加字段) + **`ColValue::Decimal(i128,u8)`** (值自带 scale, 变长区 16B i128 承载, 不动 row 8B 定长假设)。schema FMT_VER 3→4 (兼容 v1-3)。
- keyspace: IVAL_DECIMAL(0x03) + encode_i128_ordered(符号翻转BE 16B) + 17B 索引值; worker: parse_decimal/render_decimal + sql_to_col/sql_cmp/cmp_colvalue/sql_order_cmp/Accum::SumDec/pk/index/json 全加 Decimal 臂 + Accum::new 传真实 out_ty; sql.rs parse DECIMAL(p,s); 协议 NEWDECIMAL(246)/NUMERIC(1700) 定点文本 (mysql 文本+二进制均 lenenc)。
- 实机: psycopg3 + mysql-connector (文本+预处理) 均返回原生 `Decimal`, 精度不丢, SUM 精确; 全量 **858/858** clippy 0。
- 边界: i128 精度<=38; AVG→f64 回退; SUM 溢出报错; SQL 字面量经 f64 最短文本(常见精确), ORM 字符串参数完全精确; 超位截断不四舍五入。

### 2026-08-01 会话十九总览 (F80 数据类型扩展 P0-1 — BOOL/DATE/TIME/TIMESTAMP/JSON/UUID, 细节见 CHANGELOG)

- **生产可用性 P0** (数据类型与安全 4 阶段计划: F80 类型-整数/字节系 → F81 DECIMAL → F82 认证 → F83 TLS)。本轮 F80:
  - `ColType` 加 6 变体; **BOOL/DATE/TIME/TIMESTAMP 以 i64 承载** (复用 8B 定长槽 + encode_idx; 时间统一 i64 微秒 UTC 裸值); **JSON/UUID 以 Bytes 承载**; ColValue 不新增变体 (语义由列 ColType 决定)。
  - 解析: 抽 `parse_col_type` (create/alter 共用); `value()` 认 TRUE/FALSE + `DATE|TIMESTAMP '...'` 前缀; WHERE RHS ColRef 守卫排除字面量关键字。
  - worker: 自包含日期数学 (days_from_civil/civil_from_days) + parse/render_{date,time,timestamp}/uuid; `coerce_cmp_lit` 在 eval_cond_leaf 按列类型强转 WHERE 时间/布尔字面量。
  - 渲染**按 (ColType,ColValue)**: pg text_cell(ty,v) + type_oid; mysql 文本 + **二进制协议 encode_bin_date/datetime/time** (预处理结果集); mysql_type / coltype_sql_name / SHOW CREATE 加类型名。
- 实机: e2e mysql_types + storage 单元 f80_new_types_roundtrip; **psycopg3** 原生 True/date/datetime/dict/UUID, **mysql-connector** 文本+预处理均原生 datetime; 全量 **856/856** clippy 0。
- 边界: 时间无时区 (UTC v1); JSON 文本存储不建路径索引/单行 <64KB; 破坏性: DESCRIBE BOOLEAN 列 bigint→boolean。

### 2026-07-31 会话十八总览 (F78 表达式聚合 / F79 ALTER — ORM P2 完结, 细节见 CHANGELOG)

- **ORM P2** (15/17→17/17, P0/P1/P2 全完成):
  - **F78 表达式聚合** SUM(a+b)/COUNT(v-1)/AVG(x/2): tokenizer +−*/; ScalarExpr AST + 递归下降; SelectItem::Agg.col→arg:Option<ScalarExpr>; worker BoundExpr + eval_bound_expr 逐行求值嗂 Accum (除零/非数/溢出→NULL)
  - **F79 ALTER TABLE ADD COLUMN**: storage 多版本行解码 (TableSchema.version_ncols + FMT_VER3 + read_col 按行首版本字节取列数, 超出列补 NULL, 零数据重写); parse_alter; dispatch 基于旧 schema 合成新 schema 广播 SetSchemaOp + ddl_epoch递增
- 实机: e2e + ORM 17/17 + mysql/pg 一致; 全量 854/854 (45s) clippy 0
- 边界: F78 仅 +−*/无函数嵌套; F79 仅 ADD 可空列 (DROP/MODIFY/NOT NULL 拒, version u8 上限 255)
- **ORM 对接 P0/P1/P2 全部完成** (列别名/限定列/db.table/LIMIT n,m/DISTINCT/COUNT(DISTINCT)/表达式聚合/ALTER ADD COLUMN + create_all)

### 2026-07-31 会话十七总览 (F77: DISTINCT 补全 ORM P1, 细节见 CHANGELOG)

- **ORM P1** (13/17→15/17): `SELECT DISTINCT` (解析期 desugar→GROUP BY, 零新字段零 worker 新逻辑) + `COUNT(DISTINCT col)` (SelectItem::Agg/AggItemKind 加 distinct + 新 Accum::CountDistinct 去重集)
- 两项均在聚合完成点 gather 后全局去重 (encode_col_key 与 GROUP BY 组键同源), 无跨 shard 改动
- 拒绝: DISTINCT * / DISTINCT+聚合/GROUP BY/JOIN/派生表/系统表; SUM/AVG/MIN/MAX(DISTINCT)
- 剩余 P2: SUM(表达式) / ALTER TABLE
- 实机: e2e mysql_distinct + ORM 15/17 + mysql/pg 一致; 全量 851/851 (52s) clippy 0
- **❗lld 链接器使所有 cargo 命令 (含 build) 都需 `TMPDIR=$PWD/target/nxtmp`** (只读 /tmp 下 lld 建临时文件会 SIGABRT)

### 2026-07-31 会话十六总览 (F76: ORM 对接 P0 + 回归提速, 细节见 CHANGELOG)

- **ORM 对接 P0** (以 SQLAlchemy 实机探测驱动, 0/17→13/17): 列别名 AS / 单表限定列 `表.列` / `db.table` (含反引号) / `LIMIT offset,count`; 额外解 create_all 阻断 (DESCRIBE不存在表→MySQL 1146; 表级 PRIMARY KEY/UNIQUE/KEY/FOREIGN KEY/CONSTRAINT + 吃 AUTO_INCREMENT/DEFAULT)
- 均在 protocol/sql.rs 解析层 + worker.rs 投影渲染 (out_names 三路: alias>label>列名); 零存储/调度改动
- 剩余缺口 (P1/P2): DISTINCT / COUNT(DISTINCT) / SUM(表达式) / ALTER TABLE
- **回归提速 (已配置)**: rust-lld 链接器 (.cargo/config.toml + .linker/ld.lld, 零安装) 使重链接 ~1s; cargo-nextest (并行+进度+超时, .config/nextest.toml) 全量 896 测试 ~40s
  - ⚠️ `.config/` 与 `.cargo/` 均被 gitignore (本地配置). nextest.toml 的 heavy 组必须 `max-threads=1`: e2e 各起多 shard StorageEngine, 全量并行时引擎并发初始化耗尽有界资源 → 2PC 协调器 hang (two_pc_e2e 360s 超时复现; 串行后连续全量 896 passed 零超时)
- **❗测试栈 (已根治)**: 曾因 `ChunkBuf::new` 的 `Box::new([0u8; 1MiB])` 在栈上构造 1MB 临时数组, 深层 async 链爆 8MB 默认线程栈 → SIGABRT (list_ops_tests 复现). 已修复: 大缓冲全部堆分配 (`vec![0u8; CHUNK_SIZE]` / `page_pool::alloc_zeroed`), 默认栈即可, 无需 RUST_MIN_STACK
- **❗跑测试必加 `TMPDIR=$PWD/target/nxtmp`**: 沙箱 /tmp 只读不稳定, e2e 写临时库到 /tmp 会导致并发下引擎初始化失败/页损坏→hang (非 io_uring/非代码 bug)

### 2026-07-31 会话十五总览 (F73/F74/F75, 细节见 CHANGELOG)

- **子查询后续三件套** (Phase 3 已知遗留收尾):
  - **F73 大 IN**: 阈值提升 65536 (按叶子类型: EXISTS 无限/scalar >1 报/IN 65536); IN 集合 sort_in_set 排序去重 + eval_cond_leaf 同型 >64 binary_search
  - **F74 关联 EXISTS 去相关**: `SqlValue::ColRef` 解析期收, decorrelate 改写单等值 EXISTS/NOT EXISTS → 非关联 IN/NOT IN, 执行层零新机制; 不可去相关形态报错
  - **F75 派生表参与 JOIN**: SelectJoin 加 from_inner; 内层物化预填 tables[0] (prefilled, proj 全列), JOIN 状态机跳过其 gather; JOIN 右侧派生表 v1 拒
- 实机: **mysql-connector + psycopg3 跨协议对拍 19×2 全 PASS**; 回归全绿 + clippy 0
- gotcha: is_join_ahead 遇 RParen 即停 (防内层误视外层 JOIN); F73 二分依赖集合已排序; F75 prefilled 表 proj 强制全列 identity
- **子查询能力基本完备**: 非关联 WHERE (F71) + FROM 派生表 (F72) + 大 IN (F73) + 单等值关联 EXISTS (F74) + 派生表 JOIN (F75); 剩余 = 关联 IN/标量、多重相关、JOIN 右侧派生表

### 2026-07-31 会话十四总览 (F72, 细节见 CHANGELOG)

- **FROM 派生表** (Phase 3 收尾): `SELECT ... FROM (SELECT ...) t [WHERE/ORDER/LIMIT/OFFSET]`。方案 = 零 TableSource ripple — 独立 `SqlStmt::SelectDerived` 变体 (学 F67 隔离先例), 单表 Select 路径零改动
- 执行: 内层复用 F71 完成点拦截 (SqlSelectAgg → Fire::DerivedDone(MatResult); SqlRowCtx → derived_capture_rowctx 自合成列定义); 外层 finish_derived/derived_render 在 worker 内存过滤/投影/排序/截断 (保留内层真实列类型)
- 内层 = 任意单表 SELECT (含聚合/GROUP BY, 输出列名 = label 如 `SUM(v)`); 外层支持 `t.x`/裸列 + OR/NOT + 孤 COUNT(*)
- 实机: **mysql-connector + psycopg3 跨协议对拍 30/30** (含 F71 全部用例 pg 欠账补验); 回归全绿 + clippy 0
- **Phase 3 完结**: 非关联 WHERE 子查询 (F71) + FROM 派生表 (F72) 均交付; 已知后续 = 关联子查询 / 大 IN 半连接 (复用 F70 键集合点查) / 派生表参与 JOIN
- 边界: 派生表不参与 JOIN; 外层无 GROUP BY/HAVING/聚合投影 (孤 COUNT(*) 除外); 物化内存上限 JOIN_MAX_ROWS

### 2026-07-31 会话十三总览 (F71, 细节见 CHANGELOG)

- **非关联 WHERE 子查询** (Phase 3 第一部分): IN/NOT IN + 标量 + EXISTS/NOT EXISTS。方案 = 内层先跑完→折叠成字面量/恒真恒假→外层走完全现有路径
- 载体: `SqlValue::Subquery(Box<SqlStmt>)` (与 Param 同构“执行前必解”占位) — 保 `Pred<Cond>` 类型不变, 下游 plan/eval/shard 零改动
- 编排: SubqCtx 顺序状态机 (仿 SqlUniqueIns/PendingSql); materialize 拆分 render_select_agg/render_agg_groups; 拦截 SqlSelectAgg 与 SqlRowCtx 两完成路径 materialize 而非渲染; 折叠后重跑外层
- 实机: mysql 驱动 IN/NOT IN/标量(含 MAX/pk-point)/EXISTS/空集/多行报错/关联拒 全正确; 回归全绿 + clippy 0
- **已知限制**: 仅非关联; 大 IN >1024 阈值拦截引导改 JOIN; 内层限单表; **FROM 派生表未含** (需 TableSource enum 波及, 独立后续)
- gotcha: 内层可走 SqlSelectAgg 或 SqlRowCtx, 两处都需拦截; EXISTS 恒真=And([]) 恒假=Not(And([])); collect/fold 同 DFS 序

### 2026-07-31 会话十二总览 (F70, 细节见 CHANGELOG)

- **JOIN gather 索引点查优化** (纯性能): probe 侧表 gather 时用前序表 ON 等值键值集合下推为索引点查, shard 只回匹配行而非全表扫。实机: 有索引 JOIN 16ms→2.5ms (~6.3x)
- K1 KeySetHint + index_multi_point_local (逐键等值点查+bloom短路+去重+table_get_many 批量回表); table_scan_filtered_local 行来源优先级 key_set>index_hint>全扫
- K3 sql_join_keyset_hint 决策 (worker.rs); sql_join_broadcast 命中时优先键集合不再用 index_hint
- **启用条件**: 单列等值 ON + INNER/LEFT(右表) + 新表 join 列有索引 + 键集合<=1024; RIGHT/FULL/CROSS/多列/无索引退回全扫 (无劣化)
- **不改语义**: 现有 JOIN e2e 全过; tables[idx].rows 变子集对 INNER/LEFT 是精确子集, finish 零改动。回归全绿+clippy 0
- 剩余开销: JOIN 固有两轮串行 gather + 6 shard fan-out 往返 (非全扫问题)

### 2026-07-31 会话十一总览 (F69, 细节见 CHANGELOG)

- **OR/NOT/括号 谓词表达式树**: WHERE 从 AND-only `Vec<Cond>` 升为泛型 `Pred<C>`(Leaf/And/Or/Not), 覆盖单表 SELECT/DELETE/UPDATE/HAVING 与 JOIN 全路径
- 解析: parse_where 改递归下降 (OR<AND<NOT<primary, 括号复用 LParen); Cond 原样作 Pred::Leaf; BETWEEN/LIKE desugar → And(leaves)
- 求值: eval_pred 递归 (5 调用点), JOIN eval_join_pred, HAVING eval_having_pred, 系统表 eval_pred_sysq
- **核心机制 as_conjuncts()**: 纯 leaf 合取 → 平铺列表, 索引界推导/下推/bloom 原 AND 优化不变; 含 OR/NOT → None → sql_plan_select FullScan 回退 + 空下推, 完成点递归残余保正确 (列名校验用 leaves())
- 实机: mysql/pg 双驱动 OR/NOT/嵌套/DELETE·UPDATE·JOIN·HAVING OR 全正确跨协议一致; 回归全绿 + clippy 0
- **已知限制**: 含 OR/NOT 不走索引 (全扫+残余); OR 不下推 shard; NOT 二值简化 (NULL 比较 false)
- **分阶段路线**: Phase 3 = 子查询 (FROM 派生表/IN·EXISTS/标量, 另计划)

### 2026-07-31 会话十总览 (F68, 细节见 CHANGELOG)

- **JOIN 族完备化 Phase 1**: F67 两表 → **N 表左深 + 多条件 ON + RIGHT/FULL/CROSS/USING + 索引驱动 gather**. 仍 worker 完成点、零新增跨线程
- AST: `SelectJoin{from, joins: Vec<JoinClause>}` + OnPred(Eq/Cmp) + JoinKind(+Right/Full/Cross); 执行: SqlJoinCtx 逐表 gather → 左深迭代 hash join (宽行折叠, col_offset 定位, 外连接 null 扩展)
- 索引驱动: ScanFiltered 加 index_hint (storage IndexHint); WHERE 命中索引列 Eq/范围 → 索引范围扫缩候选 (过度近似 + 残余 preds 精确)
- 实机: mysql/pg 双驱动 3 表/RIGHT/FULL/CROSS/USING/多 ON/索引 全正确; 回归全绿 + clippy 0
- **分阶段路线**: Phase 2 = OR (WHERE 升级谓词表达式树, 全查询路径通用); Phase 3 = 子查询 (FROM 派生表/IN·EXISTS/标量)
- **已知限制**: WHERE 仍 AND-only; ON 需至少一等值; 不走索引嵌套循环; JOIN_MAX_ROWS 262144 上限; USING 列在 `*` 不合并
- gotcha: USING/未限定 ON 操作数用 sql_join_resolve_on 按 join 位置限作用域; JOIN 结果同键多行顺序非确定, e2e 断言需完整 ORDER BY

### 2026-07-31 会话九总览 (F67, 细节见 CHANGELOG)

- **两表 hash JOIN** (worker 完成点): `A [INNER|LEFT] JOIN B ON a.x=b.y` — JOIN 逻辑全在 worker, shard 只本地单表扫+谓词/投影下推, fan-in 后 build/probe (右建表、左探测). **零新增跨线程** (无 shard↔shard, 不碰 Scheduler 契约); gather 复用 SqlSelectAgg fan-in 模板
- 下推: 左表谓词恒下推, 右表 INNER 下推/LEFT 留 worker 残余; finish 总重应用全 WHERE (下推仅优化不影响正确性); 投影只回引用列 (ProjRows)
- 新协议: `BatchOp::ScanFiltered` + `BatchResult::ProjRows` + `ScanPred/PredOp` (定于 storage::sql_rows 避分层, request.rs re-export); 解析用独立 `SqlStmt::SelectJoin` 隔离单表路径
- 实机: mysql-connector + psycopg3 INNER/LEFT/下推/重名列/`*` 全正确跨协议一致; 回归全绿 + clippy 0
- **已知限制**: v1 仅两表单 equi ON; 多表/多 ON/RIGHT/FULL/CROSS/USING/子查询/OR 不支持; JOIN 输入全扫不走索引; 单侧 gather 256K 行上限
- gotcha: LEFT 的右表谓词绝不能下推 (null 扩展前误删); 固定右建表左探测保 LEFT 驱动順序

### 2026-07-31 会话八总览 (F66, 细节见 CHANGELOG)

- **系统表虚拟化** (GUI/ORM 反射): worker 层拦截 `information_schema.*` / `pg_catalog.*` / `SHOW` → 从活元数据合成虚拟表, 复用 SELECT 完成点 (过滤/投影/排序/三门面渲染). 数据源: DbDirView 列 db + CatalogDump BatchOp (任意单 shard 列当前 db 全表+schema, schema 每 shard 全副本)
- 支持: information_schema (tables/columns/key_column_usage/schemata); pg_catalog flat 单表 (pg_namespace/pg_class/pg_attribute); SHOW [FULL] TABLES/COLUMNS + SHOW CREATE TABLE (重建 MySQL DDL) + SHOW DATABASES; 反引号标识符; `SELECT @@var` stub (parse_prepared tokenize 前拦, '@' 不过 tokenizer)
- 实机: SQLAlchemy 2.x + PyMySQL `inspect()` 全链路通 (get_table_names 走 SHOW FULL TABLES, get_columns 走 SHOW CREATE TABLE; pk/unique 反射正确). 回归全绿 + clippy 0
- **已知限制**: psql `\d`/`\dt` (pg_catalog 多表 JOIN) 不完整 (无 JOIN, 留后); v1 仅反射 current_db; 系统表只读
- gotcha: CatalogDump locator table 为空 → ensure_table 报 btree 空键; 无表名元 op 跳过 ensure_table

### 2026-07-31 会话七总览 (F65, 细节见 CHANGELOG)

- **全局跨 shard UNIQUE**: opt-in `GLOBAL UNIQUE` 列 — 唯一值按 hash 路由到 email-shard 占坑 (持久化物理行 `[U][iid][enc_val]`→`[state][txn_id][pk]`, 行本身即 prepare 记录自带 WAL); worker 顺序状态机 Reserve→Verify→Write→Confirm; pk-shard 行为真相源, 冲突时回查行懒校对 (删后重插自愈); 不复用 DDL 2PC 协调器
- 边界: 事务内写/UPDATE 全局唯一列/多行 INSERT → v1 拒绝; 普通 UNIQUE 仍本 shard best-effort
- 验收: 旗舰"不同 pk 同 email 必拒 1062"; 实机 mysql-connector IntegrityError + psycopg3 UniqueViolation 跨协议一致; 顺手修 SQLSTATE 映射 (ORM 异常分类); 回归全绿 + clippy 0

### 2026-07-31 会话六总览 (F64, 细节见 CHANGELOG)

- **首次端到端正确性检验**: 真实驱动 (mysql-connector + psycopg3) 订单系统工作流组合压全功能 + 跨协议一致性, 20 项全过. 发现并修复: (a) 事务内 UPDATE 的 RYOW (resolve_ryow 重放同 pk 缓冲 op, NeedBase 读盘+overlay); (b) duplicate key errno 1105→1062 (ER_DUP_ENTRY)
- 新增回归 mysql_txn_ryow_update; 确认 UNIQUE 跨 shard 漏检为文档化 gap (单 shard 正常)
- 回归 net 全绿 + storage+sm+cfg 537/0 + clippy 0

### 2026-07-31 会话五总览 (F63, 细节见 CHANGELOG)

- **GROUP BY 聚合族**: SELECT 投影扩展为列/聚合函数 (COUNT/SUM/AVG/MIN/MAX) + GROUP BY 多列 + HAVING + 裸聚合 (全表单桶); worker 完成点纯内存分桶 (Accum 累加器, NULL 忽略, 空集 SUM/AVG→NULL, 分桶上限 64K), shard/存储/协议零改动; 合成结果集复用 sql_rows_bytes 三门面统一
- 边界: 不做表达式聚合/DISTINCT/GROUP_CONCAT/别名 AS/窗口函数; shard 端部分聚合下推留 v2 (当前全量收行)
- 验收: 实机 mysql-connector + psycopg3 GROUP BY/HAVING/AVG/ORDER BY 聚合列全通; 回归 829/0 + clippy 0

### 2026-07-31 会话四总览 (F62, 细节见 CHANGELOG)

- **事务 v2 多隔离级别**: SET [SESSION] TRANSACTION / BEGIN 尾缀四级语法 (RU→RC, RR→Serializable 归并); SERIALIZABLE = OCC backward validation (pk 点查记 read_set crc32 指纹, TxnApply 预检重读比对, 冲突 40001/1213); READ ONLY (25006/1792); SAVEPOINT/ROLLBACK TO/RELEASE (ops 水位截断, E 态 ROLLBACK TO 恢复 — SQLAlchemy 标准路径); 仍零锁零 MVCC 零调度器改造
- 边界: 不防幻读 (行级 OCC, 扫描读不进 read-set); RR=SER 等价; 真快照读留后
- 验收: 实机 psycopg3 SerializationFailure 类型化捕获+重试 + SQLAlchemy savepoint 序列全通; 回归 827/0 + clippy 0

### 2026-07-31 会话三总览 (F61, 细节见 CHANGELOG)

- **事务 v1**: BEGIN/COMMIT/ROLLBACK 双协议 (MySQL+PG+HTTP SQL) — conn 层 write_set 缓冲 (shard/调度器零事务状态), COMMIT 按 shard 分组 TxnApply 原子批 (先验后写 + 无条件 wal_barrier — 回复到达即持久); RC 隔离 + pk 点查 RYOW; PG I/T/E 状态字节 + 25P02, MySQL IN_TRANS 位 (resp_complete 单点注入)
- 边界: 跨 shard commit best-effort (单 shard 严格); RYOW 仅 pk 点查; DDL 事务中拒; Serializable (read-set 验证)/快照读 (COW 视图) 留 v2
- 验收: 实机 psycopg3 默认事务模式 + mysql-connector commit/rollback 全通; COMMIT 后立即 kill -9 → 20/20; 回归 824/0 + clippy 0; String 316K 无回退

### 2026-07-31 会话二总览 (F60, 细节见 CHANGELOG)

- **WAL 预写日志**: `storage.wal_mode = off | periodic (默认, 每秒 fsync, 窗口 ~1s) | strict (回复前 fsync + 组提交, crash 零丢失)`; per-shard 段文件 `{block_root}/shard_N.wal.{seq}`, 插在 put/delete_physical 收敛点记结果态 (重放幂等), 刷盘快照时 seal / meta 全落盘后删段; DDL 不进 WAL → 成功后强制 flush
- 验收: strict 写完立即 kill -9 → 50/50 全恢复; 性能 off 234K / periodic 231K (-1.6%) / strict 63K@8.1ms; 回归 821/0 + clippy 0
- **gotcha 作废更新**: crash 测试不再需等 10s 刷盘 (strict 立即可杀 / periodic 等 >1s); wal_mode=off 时旧 gotcha 仍适用

### 2026-07-31 会话总览 (F59, 细节见 CHANGELOG)

- **ORM 性能专项 — SQL 门面多 worker 化**: `sql_worker_count` 配置 (MySQL+PG 门面, 默认 1); **单 SQL worker 前提正式解除** — schema 缓存 per-worker 零锁 + 进程级 DDL epoch 失效 (DROP +1, 每语句一次 load); routes bloom/created_here 进程级 `SqlSharedRoutes` (IndexBloom 原子位图化 fetch_or 无锁; per-worker 会假阴性漏行). 热路径零锁 (1 epoch load + bloom 原子读)
- **归因**: prepared 服务端净差仅 0.90x (上轮 0.62x 是 Python 客户端开销); 单 worker 饱和 ~135K → **4 worker 16 连接 254K qps (2.5x)**
- **gotcha**: NetworkServerConfig.sql_shared 必填 — 同集群全部 SQL 门面必须同一实例 (跨门面一致性), e2e 各测试独立实例 (全局 OnceLock 会串台致假阴性)
- 回归 632/0 + clippy 0; 实机 4 worker 双驱动全通; String 234K 无回退

### 2026-07-30 会话四总览 (F58, 细节见 CHANGELOG)

- **预处理语句 (ORM 接轨)**: SQL 层 `?`/`$n` → `SqlValue::Param` + `bind_params` AST 绑定 (拒文本代入, 零注入面); MySQL COM_STMT_PREPARE/EXECUTE/CLOSE (二进制参数 + **二进制结果集**, prepare 报 num_columns=0); PG 扩展查询协议 (Parse/Bind/Describe/Execute/Sync 批次 = 单 seq, 前缀在 resp_complete 单点拼接)
- **gotcha**: 弱类型文本参数要同时放宽 sql_to_col **和 sql_cmp** (漏一边 = 残余过滤静默滤光); mysql-connector 握手后必发 `SET @@session...` ('@' 需在 tokenize 前吞); prepared 吞吐低于文本 (0.62x, 客户端编码开销) — 价值在安全与生态
- 实测: **mysql-connector (prepared=True) + psycopg3 (扩展协议) 双驱动全通**; 回归 164/0 + clippy 0; asyncpg (Flush 依赖) 不保证

### 2026-07-30 会话三总览 (F57, 细节见 CHANGELOG)

- **REST 门面 (6778)**: 零依赖手写 HTTP/1.1 (`protocol/http.rs`; 增量解析/keep-alive/chunked 拒 501) + CORS (进程级 OnceLock 配置 + OPTIONS preflight) + Bearer 鉴权 (复用 auth_password 通道, /metrics /v1/status 白名单); KV `GET/PUT/DELETE /v1/kv/{table}/{key}` (tag 感知 JSON, 与 RESP 数值互通) + SQL `POST /v1/sql` (共内核第四门面, sql_*_bytes 加 Http 分支); serde_json 为唯一新依赖
- **可观测性**: `/metrics` Prometheus + `/v1/status` + `/v1/debug/sql-cache`; 进程级 AtomicU64 relaxed 打点 (RESP dispatch 一次 fetch_add, String 基线无回退)
- **Binary 5433 降级为内部协议** (README; 代码零改动, 测试/压测工具仍用)
- 测试快照: net+sm+cfg 161/0 (新增 http_e2e 3), clippy 0; curl 实机 + 四协议互联 (redis↔REST↔mysql) 全通; REST 基线 KV ~10.4K / SQL pk ~11.3K rps

### 2026-07-30 会话二总览 (F56, 细节见 CHANGELOG)

- **SQL 补全 + PG 门面**: DELETE/UPDATE SET/多行 INSERT/DROP TABLE (pk 单 shard 原子, 索引条件两阶段收 pk 非原子); 全表扫 (无索引 fallback)/ORDER BY/OFFSET/COUNT(\*)/IN/BETWEEN/!=/LIKE 前缀 (BETWEEN·LIKE 解析期 desugar 成范围); 方言别名 + USE/DESCRIBE; **PostgreSQL wire 门面 5435** (psql 直连, cleartext auth, 与 MySQL 门面共内核 — 渲染收敛 `sql_{err,ok,rows}_bytes(proto,..)`)
- **gotcha (真客户端 stub 债)**: mysql cli 的 USE 走 COM_INIT_DB 不走 COM_QUERY; USE 后自动发 `SELECT DATABASE()`; 登录 database 字段要在 AuthSwitch 二段后应用; psql dbname 缺省 = user 名, default 隐式库需特判
- 测试快照: **800 passed / 0 failed** (新增 pg_e2e 3 + sql_e2e 扩至 7), clippy 0; psql 16 + mysql 8.4 交叉读写实机全通

### 2026-07-30 会话总览 (F50-F55, 细节见 CHANGELOG)

- **F50 SQL 索引基建**: schema (`[$]` 行 + 常驻镜像) + row 编码 (`TAG_ROW` null bitmap + 变长偏移) + 索引行 `[I][iid][保序值][PK]` (字符串转义终结符, **不用长度前缀**); **本地二级索引** — 索引行与 row 同 shard (按 PK 路由), **禁止两跳** (IndexScan 广播 → shard 内扫+回表闭环)
- **F51 SQL INSERT/SELECT**: 零依赖解析器 + worker 查询规划 (pk 点查单发 / 索引广播 + 界下推 + 全条件残余过滤 / limit 条件下推)
- **F52 MySQL wire 门面**: 5434 端口 mysql cli 直连, `mysql_native_password` + AuthSwitch 兜底 (手写 SHA1), COM_QUERY/结果集; config `sql_addr`/`sql_password`
- **F53 双层布隆剪枝**: shard 本地 bloom (开库重建, 免 BTree travel) + worker 路由缓存 (`created_here` 表零任务短路). **gotcha: 两层都必须只增不减 — 精确 map+LRU 驱逐重积 = 假阴性漏行**
- **F54/F55 性能**: 回表批量化 (LeafGuide 复用) + 投影/覆盖索引 (免回表 3.3x) + 复合写批量化 (SADD 3x) + UNIQUE 索引 (约束先行 + 等值早停 60µs)
- **gotcha: crash 测试 kill 前必须等 >10s 刷盘窗口**; repro_verify_storage 间歇 hang (杀掉重跑即可); **UNIQUE 跨 shard 漏检** (探测仅本 shard, 记录 gap)
- 测试快照: **79 suites / 784 passed / 0 failed**, clippy 0; SQL: 覆盖 eq 5.7K / unique 点查 36.7K / pk 43K qps

### 2026-07-29 会话总览 (F48, 细节见 CHANGELOG)

- **F48 RESP 分库分表**: 分表 = key 冒号前缀 `table:key` (协议无状态, `push_task` 单点重写 + `BatchOp::table_key_mut()`; 表名限 `[A-Za-z0-9_.-]{1,64}`, 非法前缀整 key 落 default 表); 分库 = `SELECT n` 经 `DbDirView` (resolver name↔DbId 内存镜像, 只含真实已建库) 翻译成 db name, `ConnState.current_db` per-connection; **惰性建表** = shard 数据面 op 前 `ensure_table` (本地建, 免 2PC). **gotcha: 建库仍是重资源不自动建 (`precreate_dbs` 配置预建); list_tables 各 shard 视图可能不一致 (惰性建表固有)**
- 顺手重构: `BatchOp::locator()` 单源提取, 净删 ~200 行三份重复路由 match
- 测试快照: **75 suites / 739 passed / 0 failed**, clippy 0; memtier 189K (无回退)

### 2026-07-28 会话总览 (F45-F47, 细节见 CHANGELOG)

- **F45 复合数据结构体系**: Hash/Set/List/ZSet 全命令落地 — 统一 key 编码 (`keyspace.rs`, `[kind][sub][varint klen][key][suffix]`, 编码只在存储边界) + 范围扫描 (`leaf_scan_from`/`btree_scan`, LeafGuide 跨 leaf 续) + ZSet 双索引 (保序 f64) + List 保序 i64 idx
- **F46 统一类型 meta** `[#][klen][key]`→`[kind][count]`: 类型检查 5→2 探测; SET 覆盖复合 key 自动 purge (无孤儿行); GET miss 1 探测全类型 WRONGTYPE; 开库 `rebuild_composite_counts` 修 crash 计数漂移. **gotcha: 一个 key 至多一行类型 meta, 由所有写入口维护互斥**
- **F47 命令面补全**: ZCOUNT/ZMSCORE/ZPOP* + SMISMEMBER/SINTERCARD/SPOP·SRANDMEMBER count + HSTRLEN/HRANDFIELD; **List 中段操作** (LREM/LTRIM/LPOS/LINSERT, **放弃 idx 连续假设** — LINDEX 扫描序 O(n), 搬行先物化 value 防溢出链误释放); *STORE (worker 二阶段聚合, 非原子记 gap); **Geo** (`storage/geo.rs` 52-bit geohash 作 ZSet score, GEOADD 解析期转 ZAdd); **Bitmap** (SETBIT shard RMW, 读类 worker 位运算)
- 遗留: BITOP/SMOVE/LMOVE/阻塞类/ZSTORE weights/TTL/Stream/HyperLogLog
- 测试快照: **75 suites / 736 passed / 0 failed**, clippy 0; memtier 184-203K (热路径无回退)

### 2026-07-27 会话总览 (F42-F44, 细节见 CHANGELOG)

- **⭐ F42 GC 静默数据丢失修复 (最重要)**: compact 判活曾用 page header vpid 自描述, 但 Internal 页该字段是 first_child → 误判死页 → 高压写后早期 key 静默丢失 (GET nil 不报错). 修复: 判活以 meta 平坦数组全扫为 SoT. **gotcha: compact/GC 判活禁止依赖页头自描述**. 排查探针 `NLOG_GC_DEBUG=1` 保留
- **F43 热路径 9 项优化** (同机 A/B +19%): page_pool 归还闭环 + travel_to_leaf_ro (免 path 分配) + table_put 单 travel + BatchOp Arc<str> + Put.value 预置 tag 免二次拷贝 + 解析游标化. **约定: `Request::Put.value` / `BatchOp::Put.val` 统一 `[TAG_RAW][payload]` 布局 (decode 时预置)**
- **F44 String 命令集**: MGET/MSET (跨 shard 分组聚合 + `LeafGuide` 区间复用批量, `ShardTask.group` 字段) + INCR/DECR/INCRBY/DECRBY/APPEND/SETNX (shard 端原子 RMW) + EXISTS/STRLEN/TYPE; 大 value 溢出页 (~1MB, 13B 描述符, PID_FREED 墓碑防复活)
- **区间 travel 基建**: `internal_child_with_bounds` → `travel_to_leaf_guided` → `LeafGuide [lower, upper)` — range scan / cursor 的直接前置
- 测试快照: **75 suites / 708 passed / 0 failed**, clippy 0

### 2026-07-25/26 会话总览 (F33-F41, 细节见 CHANGELOG)

- **三个关键正确性修复**:
  - F33 btree_insert split 条件路由 (stress 丢 key 根因)
  - F39 pollster 死锁 → `block_on_io` (IoUring 下 shard 永久 futex 睡死)
  - F40 leaf_update 段首 shared=0 损坏 (memtier 长前缀 key 覆盖写必现)
- **独立服务架构** (F36): worker(epoll) → TaskInbox → shard → TaskReplyBus → worker, 零 client 线程; 旧同步 API 保留给测试
- **自动持久化 + 异步落盘** (F35/F41): chunk 满 swap → FlushJob 协程 io_uring 写盘 (与内存写并发); MAX_INFLIGHT=8 超限退化同步 (背压); 周期 10s / 256 写触发; meta 仅在 backlog 排空后刷
- **多协议门面** (F38): RESP2 全链路 (redis-cli/memtier 验证), AUTH / pipeline FIFO 重排 / KvLimits / type tag
- **成品化** (F37): config + nlog + main 服务器化
- **性能快照** (memtier 2t×10c pipe16, io_uring, 真实持久化): 读混合 1:10 = **1.06M ops/s**; 写重 1:1 = **153K ops/s** (p99 16.7ms); 同机 Redis AOF everysec 对照 1.83M / 1.51M
- **下一步 (按 ROI)**: 读路径 PageIndex 缓存 + 零拷贝 → WAL (消 16KB 页写放大) → shard 自包含网络 (消 worker↔shard 两跳 handoff, 读向 1M+)

### ShardManager crate (✅ T13 + T14 完成, T15 async API 待实施)

**T14 (2026-07-22): 2PC 跨 shard 协调 + 同步 API** ✅
- `TwoPhaseCoordinator` 状态机 (`coordinator.rs` ~330 LOC)
- 6 个 2PC 消息: Prepare/Commit/Abort × {Db, Table}
- `ShardManager::create_db/create_table` 走 2PC
- 8 个 2PC e2e 测试, 15 个 lib 单元测试
- **测试 0 failed, clippy 0 警告**
- 同步 API 性能影响已识别: 主线程串行化 (T15 解决)

**T13 (2026-07-22): 基础架构** ✅
- 多 shard 控制器, hash 路由
- per-shard 线程 + Scheduler + StorageEngine
- `Rc<RefCell<Option<StorageEngine>>>` 共享 engine
- 同步 API: put/get/delete

**T15 (待实施: async API + pipeline)**: 网络层已搭建 (NetworkServer), 但 ShardManager 内部仍是同步 API.
- 当前 network crate 的 worker 用同步 `ShardManager::put/get/delete` (阻塞)
- 未来: ShardManager 加 `put_async` / `get_async` / `delete_async` 返回 Future
- 配合 ReplyBus 实现异步 waker 通知, 解决主线程串行化

### 当前能力盘点

**已支持** (T1-T17 + F32-F47):
- **服务化**: `nexusdb --config nexusdb.toml` 启动, Binary(5433) + RESP/Redis(6379) 双协议监听, redis-cli/memtier 可直接使用, SIGINT/SIGTERM 优雅退出 (退出前排空异步落盘 + final flush)
- **元数据**: open/close/flush; create_db/drop_db/open_db/list_dbs/use_db; create_table/drop_table/open_table/list_tables (2PC 跨 shard)
- **KV 数据**: table_put / table_get / table_delete (含覆盖写 leaf_update); 大 value 溢出页 (≤1MB)
- **Redis 数据结构**: String (含范围/RMW/批量) + Hash + Set (含代数/*STORE) + List (含中段操作) + ZSet (双索引, 含 *STORE) + Geo (复用 ZSet) + Bitmap (复用 String); 统一类型 meta + 全类型 WRONGTYPE + crash 计数重建
- **持久化**: 多 db 物理隔离 (`{block_root}/{db_name}/shard_{N}/`); reopen recover; **自动持久化** (chunk 满 swap + 周期 10s/256 写); **异步 chunk 落盘** (FlushJob 协程 + 有界背压 MAX_INFLIGHT=8); data→meta 刷盘顺序不变量
- **异步**: 全 async; 自实现协程调度器 + io_uring 后端 (服务器默认 io_uring)
- **多 shard**: hash 路由 + TaskInbox/TaskReplyBus 直连 (worker→shard→worker, 零 client 线程); 跨 key 命令 worker 端分组聚合
- **分库分表 (RESP)**: `SELECT n` 选库 (DbNameResolver id↔name 翻译, per-connection; `precreate_dbs` 预建) + key 冒号前缀 `table:key` 选表 (无状态, 非法前缀落 default 表) + shard 数据面惰性建表
- **协议层**: RESP2 命令面覆盖五大结构 + Geo/Bitmap (清单见 README); 自家二进制协议; **SQL 双门面 (MySQL wire 5434 + PostgreSQL wire 5435, 共内核)** — CREATE/INSERT 多行/SELECT (投影·ORDER BY·COUNT·IN·BETWEEN·LIKE·全表扫)/UPDATE/DELETE/DROP/USE/DESCRIBE; KvLimits (key≤1024/value≤1MB); value type tag (数值原生二进制)
- **SQL 索引**: schema/row 编码 + 本地二级索引 (与 row 同 shard, 禁两跳) + 双层布隆剪枝 (shard 本地 + worker 路由) + 覆盖索引/UNIQUE 早停; 查询规划在 worker (pk 单发/索引广播/全表扫 fallback/残余过滤)
- **测试**: workspace 800 passed / 0 failed; clippy 0 警告

**还没支持** (下一步 gap):
- **TTL/过期** (EXPIRE/TTL/PERSIST + SET 的 EX/PX/NX/XX) — 明确后置的生命周期机制
- **跨 key 原子命令**: BITOP/SMOVE/LMOVE/RPOPLPUSH/阻塞类 BLPOP·BRPOP; MSETNX/Set 代数/*STORE 跨 shard 非原子 (已记 gap)
- **Transaction** — ✅ F61 v1 + F62 v2 已交付 (conn 层缓冲 + commit 原子批; RC/SERIALIZABLE 双档 + OCC 验证 + SAVEPOINT + READ ONLY; 跨 shard best-effort, 幻读防护/快照读留后)
- **Snapshot** — 事务内一致性读 (COW + meta_cache 天然支持, 实现成本低; **不需要 MVCC** 见 §3.3.2 设计决策)
- ~~WAL~~ — ✅ F60 已交付 (三档可配; 注: "消 16KB 页写放大"的 WAL-as-主存储变体未做, 当前 WAL 是附加日志非替代写路径)
- **Stream / HyperLogLog** — 最后两个 Redis 类型 (前者工程量大, 后者小众)
- **PG/MySQL/Mongo 门面** — 前置: 统一记录编码 (保序 key 编码已有 + 表级 schema)
- **shard 自包含网络** (ScyllaDB 模式) — 消 worker↔shard 两跳 handoff 的终局方案

**⭐ 不需要 MVCC 的设计决策** (见 `docs/superpowers/plans/2026-07-18-storage-crate.md` §3.3.2):
- meta_cache 跟随 COW, 写 vpid 只改映射不改数据
- 单线程 runtime + `&mut Pager` 强制串行, 无真并发
- COW 已天然保留历史 page, 未来 Snapshot API 只需 clone meta_cache 视图
- 优势: 零额外存储 (无 version chain), 零 GC (无 version 清理), 零冲突 (Pager 仍串行)

### Storage crate T17 (全 async 重构 + io_uring 集成, 2026-07-21) ✅ **完成**

**T17 范围:**
- T16: PagerIo 抽象层 (StdFs / IoUring Backend 枚举, 通过 `OpenOptions.io_backend` 选)
- T17: Pager / StorageEngine / Registry / TableDirectory / BTree 全部改 async
- 异步测试运行器 (`tests/common/mod.rs::run_async`)
- 栈大小修复 (RUST_MIN_STACK=64MB 启动, 因 storage async fn 内联后 poll frame 含多个 16KB page buffer)
- 386 tests passed (含 19 个新 io_backend / async 测试), 0 failed

### Network crate (✅ Phase 1-4 完成, 2026-07-24)

**Phase 1-4 范围:**
- Protocol trait + BinaryProtocol 实现 (二进制帧 codec)
- Acceptor (非阻塞 accept loop, RoundRobin/Random/Sticky LB)
- WorkerPool (N worker thread, 每个 conn 独立 OS thread)
- NetworkServer 顶层组装 (acceptor + worker pool + 优雅关闭)
- ReplyBus (crossbeam unbounded channel, 实现 ReplySink trait)
- 压力测试工具 (network_stress: 4 阶段, 多 client 多 shard 压测)
- Pager read 路径加固: 四源查找 (nowchunks → WriteQueue → chunk_list → disk)

**missing key 排查 (仍在进行):**
- 高并发下 ~0.2% key 丢失, 已在 storage 层独立复现
- 单线程永不触发, 仅在多 client 并发时出现
- 已实施的修复: Pager read 路径加 WriteQueue 检索
- 待深入: BTree insert 并发 get 的 stale leaf page 问题

**当前测试状态:** Page 131 + Storage **386 passed, 0 failed**, clippy 0 警告.
Workspace: ~549 passed, 0 failed (不含慢 repro 测试).

### Storage crate T12 (ShardManager 集成, 2026-07-20) ✅ **全部 21 子任务完成**

**已完成 (21/21 子任务):**
- T12.1-T12.3: types.rs DbId + MetaKey + IoBackend 基础 ✅
- T12.4-T12.5: MetaCache v2 (LFU + per-db mate), 17→18 测试迁移 ✅
- T12.6: MetaCache 加 DbId 维度 (+13 测试, +evict bug 修复) ✅
- T12.7-T12.8: VpidAllocator + PidAllocator + FreePageQueue per-db (+10 测试) ✅
- T12.9: ChunkList ChunkKey 加 DbId (+5 测试) ✅
- T12.10: ChunkWriter per-(db, file_id) paths (+3 测试) ✅
- T12.12: Pager::new + recover 路径加 block_root + shard_id (+16 测试) ✅
- T12.13: recover 扫描 `{block_root}/{db_name}/shard_N/*.block` ✅
- T12.14: MetaPage 集成 DbNameResolver (+Resolver 段 + COW 修复) ✅
- T12.15: OpenOptions 加 block_root + shard_id ✅ (在 T12.12 提前完成)
- T12.16: StorageEngine 加 current_db 多 db 上下文 (+5 测试) ✅
- T12.17: OpenOptions 加 db_name 参数 + DbRegistry 真实多 db 物理路径 ✅
- T12.18-21: 多 db 物理隔离 e2e (9 测试) + catalog_consistency 重写 + clippy/fmt 收尾 ✅

详细修复历史 (F1-F29) 见 [`CHANGELOG.md`](./CHANGELOG.md).

### Storage crate T1-T11 (✅ 完成)

| # | 任务 | 状态 |
|---|---|---|
| T1 | Workspace + storage scaffold + types.rs | ✅ DONE |
| T2 | MetaCache: 两层数组 (10MB + 10×1MB Index) + LRU-最近邻 | ✅ DONE |
| T3 | VpidAllocator + PidAllocator + FreePageQueue | ✅ DONE |
| T4 | 三层架构: NowChunks + WriteQueue + ChunkWriter | ✅ DONE |
| T5 | ChunkList: 1MB chunk 读 LRU 缓存 (只读不可修改) | ✅ DONE |
| T6 | Pager: read + create + PageWriteBatch + chunk_lock + TravelTree | ✅ DONE |
| T7 | recover: 扫描 block_dir + MetaCache union 语义 | ✅ DONE |
| T8 | StorageEngine facade: open/put/get/flush/close | ✅ DONE |
| T9 | MetaPage: db_name → table_dir_root_vpid BTree | ✅ DONE |
| T10 | TableDirectory: table_name → table_root_vpid BTree (移除 *mut Pager 修复 aliasing UB) | ✅ DONE |
| T11 | DbRegistry: 多 db/多表 API + 镜像 cache | ✅ DONE |

### Scheduler crate (✅ 完成 T1-T10, T11 clippy/fmt polish 暂停)

11 任务 plan: `docs/superpowers/plans/2026-07-17-scheduler-crate.md`.

### ShardManager crate (✅ T13 + T14 完成, T15 async API 待实施)

**T13 (基础架构)**:
- 多 shard 控制器: N 个独立 shard 线程 + Scheduler + StorageEngine
- hash 路由: `(db_name, table_name, key)` 三元组 hash
- 同步 API: put/get/delete/create_db/create_table
- 共享 engine: `Rc<RefCell<Option<StorageEngine>>>`

**T14 (2PC 跨 shard 协调)**:
- `TwoPhaseCoordinator` 状态机: begin_txn → on_prepare_ack/fail → on_commit/abort_ack
- 6 个 2PC 消息: Prepare/Commit/Abort × {Db, Table}
- Abort 是 best-effort: reverse op = drop_db/drop_table
- Coordinator 用 `RefCell` 包装, 让 `&self` 方法能访问

**T15 (待实施: async API + pipeline)**:
- 解决 T14 同步 API 的主线程串行化问题
- 给网络层 (Tokio/Axum) 用

### Page crate (✅ 完成 Phase 1-7 + dump 工具)

7 phases: ItemPtr / PageIndex / push_back / pre_split·merge / leaf CRUD / internal CRUD / 清理旧代码 + dump.rs.

---

## Windows 可移植性 (2026-08-13)

当前 Windows 跑通的是 P1 MVP: `std::net::TcpListener` + 每连接一个 `std::thread` 阻塞 IO,
见 `docs/plans/2026-08-13-windows-portability.md` + `docs/plans/2026-08-13-windows-iocp.md`。

- **Linux 主推路径不动**: `server.rs` / `worker/` / `scheduler/` 全部 Linux-only, 性能
  与 memtier 数字都是 Linux 路径的结果, **不能套用到 Windows**。
- **Windows 路径**: `crates/network/src/runtime_iocp.rs` (cfg `target_os = "windows"`,
  ~250 LoC std::net + per-conn thread); `portable.rs` 仍是 Linux 之外的 fallback。
- **依赖**: Cargo.toml 加 `windows-sys = "0.61"` (Windows-only) 仅用于
  `SetConsoleCtrlHandler`。
- **CLI**: `nexusdb [--config <path>] [--version]`, 缺省 config 自动用 `stdfs` (不
  会因 `io_uring` 拒绝而崩)。
- **协议范围**: Windows 启动只 bind Binary (5433) + RESP (6380)。SQL/PG/HTTP/TLS
  仍是 Linux 路径, 启动 cfg-隔离跳过。
- **已知缺口**: `INCR/HSET/LPUSH/SADD/ZADD/DBSIZE/INFO/CLIENT LIST` 在 dispatch 树上
  还没接 (portable.rs 也一样), 是协议层本身的事, 不是 Windows runtime 缺。

### Windows IOCP 集成 gotchas (再启用前必看)

1. **`#[repr(C)]` 是硬约束**: `OverlappedData` 必须 `#[repr(C)]` 且第一个字段是
   `overlapped: OVERLAPPED`, 否则 Rust `repr(Rust)` 会 reorder 字段, GQCS 拿到的
   ptr 强转 `*mut OverlappedData` 后拿到错位数据。调试时打印 `data_ptr` vs
   `overlapped as *mut _` 验证 `offset == 0`。
2. **`AcceptEx` 在 Win10/11 overlapped listener + 没 client 时同步返回 TRUE with
   bytes=0**: OS 投递完成事件到 IOCP, 但 child socket 仍是 pre-alloc 状态。
   - 症状: GQCS 立刻拿到 `key=ACCEPT_KEY` 事件; `SO_UPDATE_ACCEPT_CONTEXT` 失败
     10057 `WSAENOTCONN`; `WSARecv` 在 child 上失败同样 10057;
     `closesocket(child) + re-arm` 触发 OS 复用 handle 编号 → 死循环。
   - 解决: 改用 `wepoll` (kernel-bridged epoll) 或 winsock catalog; 或者直接
     继续用 std::net blocking path (P1 MVP)。
3. **`windows-sys = "0.61"`**:
   - `ACCEPTEX` 不存在, 是 `LPFN_ACCEPTEX` (type alias `Option<unsafe extern "system" fn(...)>`)
   - `setsockopt` 第 4 参是 `PSTR` (`*const u8`), 不是 `*const c_void`
   - `WSASocketW` 必须传 `WSA_FLAG_OVERLAPPED` 才能配合 IOCP
4. **Listener `set_nonblocking(true)` 状态继承到 child socket**: acceptor 需要靠它
   轮询 `stop` atomic, 但 winsock 会继承给 child. child 的 read 在无数据时返回
   `WSAEWOULDBLOCK` (10035) 或 `WSAETIMEDOUT`, **必须** retry + 短 sleep, **不能**
   `return Err`, 否则 client 看到 "An existing connection was forcibly closed"。
5. **优雅停止**: `NetworkServer::shutdown` 顺序: 设 `stop` atomic → acceptor 退出
   (listener 关闭, accept 立即返回错误) → `stream.shutdown(Shutdown::Both)` 唤醒
   阻塞的 conn thread → join 所有 conn → `Arc::try_unwrap(mgr)` + `mgr.close()` flush
   WAL。**不能**反过来先 close mgr 再等 conn 退出, 否则 WAL 没 final flush。
6. **`redis-server` 6379 在 win 自带**: `Redis.Redis` winget 包安装的 redis-server
   跑在 SYSTEM 账户, 没 admin 杀不掉; 测试用 6380 避让。

---

## 关键设计原则 (实施时记住)

### 调度器 / IO

- **Scheduler 多线程契约**:
  1. 每个 shard 线程自己 NEW 一个 Scheduler (独立 io_uring), 永久 run() loop
  2. spawn / drive / JoinHandle::poll 全在同一线程
  3. 跨 shard 通信用 mpsc channel (不用 JoinHandle 跨线程)
  4. 违反任一条 → JoinInner::UnsafeCell 跨线程 race → 永久 hang

- **协程 = Rust `async fn` + Future** (不是栈式协程)
- **Waker 全部自实现**, 不依赖 monoio 的 Reactor
- **不引入 tokio / crossbeam / monoio**: 全部走 `scheduler::io_ops::{read, write, fsync}`
- **Future 自取 CQE** via `peek_cqe_by_user_data`, 不走 SharedResult 中转

### Storage crate (T12 阶段, 实施时遵守)

- **三层地址空间**: vpid (u64, 永不重用-COW 友好) → pid (file_id + chunk_idx + page_idx + flags) → byte offset
- **PidLocation 必须 `#[repr(C, packed)]`** 8B (MetaCache 一项 8B 槽)
- **写顺序**: page data → .block → vpid log → .block fsync → dirty .mate window → page.mate fsync (data→meta, 不可调换)
- **vpid 永不重用**: 一旦分配不被回收, COW 由 meta_cache 完成
- **chunk 满 64 pages 触发 rotate**: PidAllocator 返回 None, ChunkWriter 切新 chunk/file
- **Page 二层访问**: `read_page` borrow 零拷贝 / `take_page_for_write` COW 复制
- **PageWriteBatch 必走**: leaf/internal/root split / merge / drop_table 必走 batch (MAX_BATCH_BYTES=256KB, 跨 batch 原子性 caller 自保)
- **chunk_lock owner**: 必须 batch::submit + meta_cache.write 都完成才释放 (持有期 = 隐式 pin chunk)
- **TravelTree RAII**: TravelTreeGuard drop 自动 unregister, 不允许手动
- **recover 第一版用 page header 自描述**: 不解析 vpid log 格式 (T11 polish 时再加)

### Page crate

- **哨兵总是 item 0**: shared=0, key_unshared_len=0
- **key_count 包含哨兵**: 真实 keys 数 = key_count - 1
- **每个 cp 段首 shared=0** (create_from_cp 时验证)
- **段大小 ≤ MAX_PER_CHECKPOINT (32)**: 超了就 split; **≥ MIN_PER_CHECKPOINT (8)**: 少了就 merge (哨兵段例外)
- **只有 k+1 需要重写 shared_prefix_len**: push_back 后紧邻 item 的 prev_key 变了
- **删除后也要重写 k+1**: `leaf_delete` / `internal_delete` 物理删除后, 原来 k+1 的 prev_key 从 target 变成 target-1, 必须用新 prev_key 重新编码
- **删完别越界**: 清理空段后 target_seg_idx 可能失效, 用 `effective_seg_idx = min(target_seg_idx, segments.len()-1)`

### Catalog (T9-T11, 已确认版)

- **MetaPage 硬编码 chunk 0 page 0**: 整个 catalog 树的根, 启动第一个读
- **MetaPage 用 BTreeMap 镜像 + 整页重写 flush**: db 数量少时整页重写性能可接受
- **TableDirectory 单 leaf page BTree**: 复用 page crate leaf, 每个 db < ~200 table, 超需 internal page (留 polish)
- **DbRegistry write-through cache**: HashMap 是 BTree 的镜像, cache 永不超前
- **多 db, 每 db 多表**: db_name + table_name 复合 key, 不同 db 完全隔离

### T12 ShardManager (新增)

- **三层物理隔离**: `block_root/{db_name}/shard_{N}/{*.block, page.mate}` (db 物理隔离 + shard 物理隔离 + block 文件隔离)
- **pid/vpid per-db 命名空间**: 不同 db 的 vpid 0 物理上不同 (独立 .block)
- **DbId(u32) 内部唯一标识**: 替代 String (4B Copy vs 24B + heap alloc)
- **DbNameResolver**: name ↔ id 双向映射, 持久化到 MetaPage
- **MetaCache v2 = LFU + per-db page.mate**: 抛弃 sliding window, freq tracking + 衰减 (抗陈旧热点) + soft/hard cap 动态伸缩
- **compat 策略**: 所有现有 caller 用 compat API (走 db=0) 保持 zero regression

### 三层并发控制 (T6 实施后正交)

1. **chunk_lock** — 字节层, 同 chunk 内串行读 page 字节
2. **travel_key_path + travel_tree** — tree 逻辑层, split 传播时更新栈路径
3. **fresh root_vpid** — 全局入口层, 每次新 travel 拿最新 root

### 2026-07-25/26 增量设计原则 (F33-F41)

**异步 I/O (核心修正)**:
- **❌ 不能在 shard 线程用 `pollster::block_on`** 跑 IoUring 后端的 async — IoUring 下 `io_ops::fsync` 首次 poll 提交 SQE 后 Pending, pollster park 线程; 而 CQE 收割在**下次 poll 的 CQ 扫描**里 — 线程睡死后无人再 poll → 永久死锁. 现象: PING 通、SET 卡死. 用 `block_on_io` (重 poll, Pending 后 spin/yield), poll 内部自带 CQ 收割
- **⭐ flush 不能在 shard 主循环内 `block_on_io` 串行 await** —— 磁盘 IO 应**所有权转移**给独立协程 (`spawn_on`, FlushJob 零 Pager 借用), 与内存写入完全并发; 主循环每轮 `drive_until_idle` 推进收割. 磁盘 IO 满时自然降速 (有界背压, MAX_INFLIGHT_CHUNKS 超限退化同步)
- **`flush()` 契约**: caller 必须先排空 in-flight (debug_assert), 否则同 key 并发写同 offset
- **完成顺序**: shard 端先 push reply_bus 再 `reply.send` (避免 client 醒来读到缺条目的 sink)

**协议层**:
- **value type tag**: 写入 `[tag u8][payload]`, 读时按 tag 解; 空值/未知 tag 容错按 RAW 返回 (兼容早期未打 tag 数据). 多协议数据互联统一编码
- **KvLimits 上限依据**: page 编码路径全用 `[0u8; 4096]` 栈缓冲, 单 item 硬上限; config 校验 `max_key + max_value <= 4060`. 超限在 worker parse 后进 shard 前拦截, 返协议 error
- **RESP FIFO 重排**: RESP 无 req_id, per-conn 递增 seq 作 req_id; 回复经 BTreeMap 严格按序; 本地命令 (PING/AUTH/超限 error) 也占 seq 保证 pipeline 顺序
- **同 key 去重 (异步落盘)**: in-flight 中的 key 跳过新一轮 take, 避免两个协程并发写同 offset 乱序
- **TCP_NODELAY**: server accept 后必设, 否则 pipeline 小回复被 Nagle + delayed-ACK 拖到 40ms (p50 0.26→0.66ms)

**通信层**:
- **drain 丢唤醒竞态修复**: 先 `store(0, Release)` 再 `pop` —— store 前的 push 必被本轮 pop 到; store 后的 push 看到 0 重新写 eventfd. (inbox + task_inbox + reply_bus 均修复)
- **EPOLLT 边缘触发易丢事件**: 改水平触发 (默认), 稳健优先
- **worker FIFO 重排 + BinTreeMap** 是 RESP 正确性的基石
- **accept → worker 通知用 eventfd 精确唤醒**, 避免 worker 1ms epoll 空轮询

**catalog 修复** (F33, stress 丢 key 根因):
- **btree_insert split 后必须按 key 路由**: `if key > split_key { right } else { left }`. 旧代码无条件插 right 假设触发 key 一定 > split_key, 对非顺序插入 (新 key 落在原页 max 之前) 错位
- MetaCache 零槽 phantom entry: pread_slot_from_mate 读到全零返回 None, 不缓存

---

## 关键文档路径

| 路径 | 内容 |
|---|---|
| `DESIGN.md` | 项目总设计 (10 节) — 必读 §3.4 (Per-Shard 调度器), §4.2.3 (Page/Item 设计), §4.3-§4.7 (Storage) |
| `CHANGELOG.md` | 修复历史 (F1-F41) + 测试进度快照 + gotchas + 测试清单 (接手后首选查阅) |
| `docs/bug-report-btree-split-routing.md` | stress 丢 key 根因调查报告 (F33) |
| `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md` | scheduler crate 设计 |
| `docs/superpowers/plans/2026-07-17-scheduler-crate.md` | scheduler crate 11 任务实施 plan |
| `docs/superpowers/plans/2026-07-17-page-item-revision.md` | page crate 增量式 prefix-compress 方案 |
| `docs/superpowers/plans/2026-07-18-storage-crate.md` | storage crate T1-T11 实施 plan |
| `docs/superpowers/plans/2026-07-20-shard-manager.md` | storage T12 + ShardManager plan (21/21 子任务完成) |
| `docs/superpowers/plans/2026-07-25-async-network-stack.md` | async network stack plan (Phase 1-5, 15 任务) |
| `docs/superpowers/plans/2026-07-26-stress-verify-bug-investigation.md` | stress verify bug 排查时间线 (F32 阶段) |
| `crates/page/src/dump.rs` | 调试工具: 解析输出 page 结构 |
| `crates/logging/src/lib.rs` | nlog 模块, 含 io_uring 协程融合 logger 设计说明 |
| `scripts/smoke.toml` + `scripts/smoke_client.py` | 服务器端到端 smoke 测试 (含 redis-cli 验证步骤) |

---

## 提 issue / 改 plan 时

- 设计的总入口是 `DESIGN.md §3.4` (调度) / `§4.2-§4.3` (page) / `§4.3-§4.7` (storage)
- plan 里所有数字 (POOL_SIZE, BATCH_SIZE, MIN/MAX_PER_CHECKPOINT, MATE_CACHE_SIZE, INDEX_SIZE 等) 都从这些章节来
- 改 plan 请同步改 spec, 改 spec 请同步改 plan

---

> 如果你从外部接手, 先读:
> 1. 这份文件 (5 分钟)
> 2. `DESIGN.md §3.4` (15 分钟)
> 3. `docs/superpowers/specs/2026-07-17-scheduler-crate-design.md` (30 分钟)
> 4. `docs/superpowers/plans/2026-07-17-page-item-revision.md` (15 分钟)
> 5. `docs/superpowers/plans/2026-07-18-storage-crate.md` (15 分钟) — T9-T11 catalog 设计
> 6. `docs/superpowers/plans/2026-07-20-shard-manager.md` — T12 ShardManager 计划
> 7. `docs/superpowers/plans/2026-07-25-async-network-stack.md` — async network stack 计划
> 8. `crates/page/src/dump.rs` — 调试工具 (排查问题时很有用)
> 9. `CHANGELOG.md` — 当需要看修复历史 / 测试进度 / gotchas 时按需查阅
