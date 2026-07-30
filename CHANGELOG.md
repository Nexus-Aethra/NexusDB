# NexusDB — Changelog & Hindsight

> 详细修复历史 + 测试进度快照 + 环境 gotchas + 测试文件清单.
> 本文件由 `AGENTS.md` 拆分而来 (2026-07-20), AGENTS.md 只保留项目入口与设计原则摘要.
>
> 完整测试状态快照历史索引: 7-24 / 7-20 / 7-19 三个旧快照完整保留于 `git log CHANGELOG.md`
> 任意历史版本; 与本快照差异仅在测试计数 (随会话累积), 测试文件清单同步见代码目录.

**逆序时间线 (最新在上).**

---

## 2026-07-31 会话 (F59: ORM 性能专项 — SQL 门面多 worker 化)

### 交付总览 (计划 A-D)

| # | 交付 | 文件 |
|---|------|------|
| A | Rust 客户端归因: prepared 服务端净差 **0.90x** (上轮 0.62x 系 Python 客户端开销) → 预规划缓存 gate 未过不做; 单 worker 饱和 ~135K qps (8 连接起平) | `sql_e2e.rs` (bench_* ignored) |
| B1 | IndexBloom 原子化: `Vec<u64>` → `Vec<AtomicU64>` (fetch_or AcqRel / load Acquire), `&self` 写 | `storage/index_bloom.rs` |
| B2 | 缓存分层 (用户方案): **schema per-worker 零锁** + 进程级 **DDL epoch** 失效 (AtomicU64, DROP +1, 每语句一次 load 比对陈旧即清) ; **routes/created_here 进程级** `SqlSharedRoutes` (per-worker 必假阴性 — INSERT 分散多 worker) | `network/worker.rs`, `server.rs`, `lib.rs` |
| B3 | `sql_worker_count` 配置 (默认 1) 应用 MySQL+PG 门面; HTTP 保持 1 | `config`, `main.rs` |
| C | COM_STMT_EXECUTE 借用重构省一次 AST 深拷贝 (bind_params 单拷贝) | `worker.rs` |

### 关键设计

- **热路径零锁**: 每语句 = 1 次 epoch load + bloom 原子读; routes RwLock 仅保护 map 结构 (读取克隆 Arc 锁外操作), created_here 读锁 DDL 低频
- **NetworkServerConfig.sql_shared 必填注入** (同 ShardManager 集群的全部 SQL 门面必须同一实例 — 跨门面 INSERT/SELECT 一致性; e2e 各测试独立实例防串台); 拒绝全局 OnceLock (测试进程内多集群共享会让 created_here 误判存量表 → 假阴性)
- **单 SQL worker 前提正式解除** (W 轮红线语义平移到进程级: 只增禁驱逐 / created_here 门槛 / 回退广播 shard bloom 兜底)
- epoch 只在 DROP 递增 (CREATE 新表不作废旧缓存); DROP+重建换 schema 的旧 schema 解码错行为被 epoch 阻断 (多 worker e2e 覆盖)

### 验收

- **并发吞吐**: 16 连接 pk 点查 1 worker 100K → **4 worker 254K qps (2.5x)**; 单连接延迟不变
- 多 worker e2e: 2 worker 跨连接 CREATE→INSERT 分散→等值 SELECT 完整 (per-worker bloom 必挂的场景) + DROP/重建 epoch 失效跨 worker 正确
- 实机 `sql_worker_count = 4`: mysql-connector prepared + psycopg3 双驱动全通 (跨门面共享路由缓存)
- 回归: net+sm+cfg 165 + storage 467 = **632/0**, clippy 0; String mixed 234K 无回退 (bloom 原子化零感知)

---

## 2026-07-30 会话四 (F58: 预处理语句 — MySQL COM_STMT_* + PG 扩展查询协议)

### 交付总览 (计划 P1-P4)

| # | 交付 | 文件 |
|---|------|------|
| P1 | SQL 层参数化: `?`/`$n` 占位符 → `SqlValue::Param` + `bind_params` AST 模板深拷贝绑定 | `sql.rs` (parse_prepared 新入口), `worker.rs` (sql_to_col/sql_cmp 弱类型放宽) |
| P2 | MySQL COM_STMT_PREPARE/EXECUTE/CLOSE/RESET: 二进制参数解码 + **二进制结果集** | `mysql.rs`, `worker.rs` (mysql_stmts 注册表 + mysql_binary seq 标记) |
| P3 | PG 扩展查询协议: Parse/Bind/Describe/Execute/Close/Sync | `pg.rs`, `worker.rs` (PgBatch 批次 + resp_complete 前缀单点拼接) |
| P4 | 验收: e2e 手写双协议客户端 + **真实驱动实测** (mysql-connector prepared / psycopg3) | `sql_e2e.rs`, `pg_e2e.rs` |

### 关键设计

- **AST 模板 + 绑定** (拒绝文本代入重解析 — 零转义面/注入面): Param 泄漏到执行层由 sql_to_col 防御报错; ?/$n 混用报错; LIMIT/OFFSET 位置不支持占位 (语法位, 记录)
- **弱类型转换**: PG 文本参数一律 SqlValue::Str, 目标列 I64/F64 时文本解析 (sql_to_col **和 sql_cmp 两处都要放宽** — 只改前者会导致残余过滤静默滤掉全部行, e2e 抓到); 二进制参数按 Parse 声明 OID 解码 (int2/4/8, float4/8, bool, text/varchar/bytea)
- **MySQL**: prepare 回 num_columns=0 (列定义延迟到 execute 结果集自描述, 免 prepare 期 schema 异步化); execute 结果集 = 二进制协议行 (0x00 头 + NULL bitmap 位偏移+2 + LONGLONG/DOUBLE LE + lenenc) — 渲染分流 = `sql_rows_bytes(proto, binary, ..)` 加 bool 维度, seq 级 `mysql_binary` 标记 (各渲染点 remove, agg drained 时清防泄漏)
- **PG 扩展协议批次 = 单 seq**: Parse..Sync 累积 (PgBatch: prefix 字节 + bound 语句 + error skip-to-Sync), Sync 触发 dispatch; **前缀 ([ParseComplete][BindComplete][ParamDesc][NoData]...) 在 resp_complete 单点拼接** — 结果主体 (T+D+C+Z) 复用 simple query 渲染零改动; Describe(statement) 回 t+NoData (列描述由结果流 RowDescription 满足 psycopg3/node-postgres flow)
- **驱动噪声**: `SET @@session...` 含 '@' 在 tokenize 前整吞 (mysql-connector 握手后必发, 实测暴露)

### 实测

- **mysql-connector-python (use_pure, prepared=True)**: 建表/参数化 INSERT (含 NULL)/SELECT (二进制结果集 + NULL bitmap)/UPDATE/DELETE 全通; C 扩展实现连接失败为驱动侧问题 (errmsg 属性 bug), 纯 Python 协议路径全对
- **psycopg3 (默认扩展协议)**: %s 参数化 INSERT/SELECT/COUNT 全通; 与 MySQL 门面同表互读 ✓
- **prepared vs 文本性能 (诚实)**: 单连接点查 text 30.5K vs prepared 19K qps (0.62x) — 客户端二进制编码开销 > 服务端省 parse (手写解析器本就 <10µs); prepared 的价值 = 注入安全 + ORM 生态兼容, 非吞吐
- 回归: net+sm+cfg 164/0 (新增 mysql_prepared_statements + pg_extended_query e2e), clippy 0; asyncpg (Flush 依赖) 明确不保证 (记录)

---

## 2026-07-30 会话三 (F57: REST 门面 HTTP/1.1 + CORS + 可观测性)

### 交付总览 (计划 H1-H5)

| # | 交付 | 文件 |
|---|------|------|
| H1 | 零依赖 HTTP/1.1 基建: 增量解析/keep-alive/CORS preflight/Bearer 鉴权 | `protocol/http.rs` (新), `worker.rs`, `config`, `main.rs` (第五监听 **6778**, 用户拍板避开 8080) |
| H2 | KV REST: GET/PUT/DELETE `/v1/kv/{table}/{key}?db=` (tag 感知 JSON, 与 RESP 互通) | `worker.rs` (http_ctx 簿记 + 回包渲染钩子) |
| H3 | SQL REST: POST `/v1/sql` — 共内核**第四门面** (渲染分流 sql_*_bytes 加 Http 分支) | `worker.rs` |
| H4 | 可观测性: `/metrics` (Prometheus) + `/v1/status` + `/v1/debug/sql-cache`; 进程级 AtomicU64 指标 | `lib.rs` (metrics/http_config 模块), `worker.rs` 打点 |
| H5 | 验收: http_e2e 3 测试 + curl 实机 + 四协议互联 + 基线 | `tests/http_e2e.rs` (新) |

### 关键设计

- **手写 HTTP/1.1** (不引 hyper/axum/tokio; serde_json 为唯一新依赖): 增量解析 (不完整 None 续读), 头 16KB/431 + body 1MB/413 上限, **chunked 拒 501** (仅 Content-Length, 记录); keep-alive pipeline 复用 seq 重排 (每请求一 seq); Connection: close → close_after_flush (pending 出完再关)
- **CORS**: `http_cors_origin` 进程级 OnceLock (单 HTTP server 语义, 免 NetworkServerConfig 破坏面); OPTIONS preflight 就地 204 全套头
- **鉴权**: `http_token` 复用 WorkerConfig.auth_password 通道 = Bearer token; `/metrics` `/v1/status` 白名单免鉴权 (监控惯例)
- **KV tag 互通**: PUT value JSON number → encode_i64/f64 (数值原生二进制), string → TAG_RAW — 与 RESP 完全同源; GET 按 tag 渲染 JSON number/string, 非法 UTF-8 → base64 + encoding 标记
- **SQL 第四门面零内核改动**: sql_err_bytes Http 分支按消息映射 400/409/500 JSON; Binary 降级"内部协议" (README, 零代码改动)
- **指标**: 静态 AtomicU64 relaxed (HTTP/SQL/KV 计数 + uptime); RESP 热路径仅 dispatch 入口一次 fetch_add, memtier 复测无回退

### 测试快照

- net+sm+cfg **161 passed / 0 failed** (新增 http_e2e 3: 全流程/Bearer/连接语义; http.rs 单测 5), clippy 0; storage 未触碰 (零改动)
- 实机 curl: KV/SQL/preflight (204+Allow-*)/metrics/status 全对; **四协议互联**: redis 写→REST 读 / REST 写→redis 读 / REST 建表→mysql 读, 全一致
- 基线: REST KV GET/PUT ~10.4K rps, SQL pk ~11.3K rps (p50 0.25ms, 4 连接单 worker); String mixed 228K 无回退 (打点零影响)

---

## 2026-07-30 会话二 (F56: SQL 补全 DML/SELECT/方言 + PostgreSQL wire 门面)

### 交付总览 (计划 S1-S5)

| # | 交付 | 文件 |
|---|------|------|
| S1 | DML: DELETE / UPDATE SET / 多行 INSERT / DROP TABLE | `sql.rs`, `worker.rs`, `sql_rows.rs` (row_update/drop_table_sql), `request.rs+manager.rs` (RowUpdate/DropTableOp) |
| S2 | SELECT 扩展: 全表扫 / ORDER BY / OFFSET / COUNT(\*) / IN / BETWEEN / != / LIKE 前缀 | `sql.rs`, `worker.rs`, `keyspace.rs` (split_string), `sql_rows.rs` (table_scan_rows_local), TableScan op |
| S3 | 方言别名 (DOUBLE PRECISION/VARCHAR(n)/BYTEA/BOOLEAN) + USE / DESCRIBE / SET·version() stub | `sql.rs`, `worker.rs` |
| S4 | **PostgreSQL wire 门面 (5435)**: psql 直连 + cleartext 认证 + 渲染 per-proto 分流 | `protocol/pg.rs` (新), `worker.rs`, `config`, `main.rs` |
| S5 | mysql cli 实机暴露修复: COM_INIT_DB 真切库 / 登录 database 字段 / SELECT DATABASE() | `worker.rs`, `sql.rs` |

### 关键设计

- **双门面单内核**: MySQL(5434)/PG(5435) 共用 SqlStmt/规划器/聚合状态机; 渲染收敛为 `sql_err_bytes/sql_ok_bytes/sql_rows_bytes(proto, ..)` 三个 per-proto 编码器 (PG 每回复尾随 ReadyForQuery). **分端口决策**: MySQL 服务端先发言 vs PG 客户端先发言, 共端口需超时嗅探 (延迟税+误判), 不值
- **DML 原子性分级**: pk 等值 → 单 shard 原子 (RowUpdate 在 shard 端读-改-写, 继承 UNIQUE 校验/索引跟随); 索引/扫描条件 → **两阶段** (phase1 复用 SELECT 聚合收全行过滤取 pk — `SqlSelectAgg.dml` 标记, DML 禁早停保证 phase1 全量回齐后才发 phase2, 同 seq 双聚合不并存; phase2 逐 pk 分发 `SqlDmlAgg` 计数 affected). 非原子, 与 *STORE 同级 gap
- **全表扫**: TableScan 广播 op, shard 扫 `[S]` 前缀只收 TAG_ROW (跳过混入 KV 行), pk 批量回读 (LeafGuide); 规划器无索引 fallback (报错路径退役), 无 WHERE 无排序时 limit+offset 下推
- **新算子不改树形**: BETWEEN → Ge+Le, LIKE 'p%' → [p, p+1) 字节范围 (与 starts_with 精确等价; 全 0xFF 退化只留下界) — 均**解析期 desugar**; IN → CmpOp::In (Cond.set), 索引列取 [min,max] 保序编码极值下推 + 残余精确; != 纯残余
- **ORDER BY**: 聚合完成点排序 (多列/DESC; NULL asc 排最后 desc 相反 = PG 默认), OFFSET 排序后截断; 有排序时 shard limit 一律不下推, 无 top-k (记录). 覆盖判定并入排序列
- **DROP TABLE 三层清理**: 引擎 (物理 + schema 镜像/bloom/复合提示 `purge_table_state`) + worker 缓存 (schemas/routes/created_here) — e2e 验证重建同名表零幽灵
- **PG wire 子集**: SSLRequest/GSSENC → 'N' 拒绝回落; StartupMessage database 参数校验切库; cleartext (28P01) — SCRAM/TLS/扩展查询协议 (Parse/Bind) 明确不做; OID 映射 int8/float8/text/bytea; CommandComplete tag = "OK n" (非标准, psql 原样显示)

### 实机验收插曲 (真客户端暴露的三个 stub 债)

- mysql cli 的 `USE x` 走 **COM_INIT_DB** 而非 COM_QUERY — 原 stub 假 OK 不切库 → 真实现 (校验 + conn.current_db)
- USE 后 cli 自动发 `SELECT DATABASE()` → 解析失败断连 → DatabaseStub 单行回显
- 登录报文 `--database` 字段一直被忽略 → 认证通过后应用 (AuthSwitch 二段经 `MysqlState.pending_db` 传递)
- psql 的 dbname 几乎必带 (缺省 = user 名) → default 库名特判 (隐式库不入 resolver)

### 测试快照

- workspace: **net+sm+cfg 153 + storage+page+sched 647 = 800 passed / 0 failed**, clippy 0
- e2e 新增: `pg_e2e.rs` 3 测试 (手写 PG 客户端: SSL 拒绝/auth 成败/全流程/多语句拒绝) + sql_e2e 扩至 7 (DML 全流程/SELECT 扩展/方言工具)
- 实机: **psql 16 + mysql 8.4 同库交叉读写一致**; kill -9 (等 12s 刷盘) 恢复正确; 优雅重启跨门面数据一致
- 性能: pk 57K / DELETE+INSERT 11K / ORDER BY+LIMIT 2.9K / 全表扫 5K 行 ~70 qps·55ms (新基线项); String mixed 229K 无回退

---

## 2026-07-30 会话 (SQL 体系: 索引基建 → MySQL wire 门面 → 双层剪枝 → 三项优化)

### 交付总览

| # | 交付 | 文件 |
|---|------|------|
| F50 | SQL 索引基建: schema/row 编码 + 本地二级索引 + IndexScan 广播 | `storage/schema.rs+row.rs+sql_rows.rs+index_bloom.rs+keyspace.rs+btree.rs`, `shard_manager`, `network` |
| F51 | SQL INSERT/SELECT: 解析器 + worker 查询规划 (pk 点查/索引广播/残余过滤) | `network/protocol/sql.rs`, `network/worker.rs` |
| F52 | MySQL wire 门面 (5434): 握手/mysql_native_password 登录/COM_QUERY/结果集 | `network/protocol/mysql.rs`, `network/worker.rs`, `config`, `main.rs` |
| F53 | 双层布隆剪枝: shard 本地 index bloom + worker 索引路由缓存 | `storage/index_bloom.rs+sql_rows.rs+collections.rs`, `network/worker.rs` |
| F54 | 回表批量化 + schema worker 级缓存 | `storage/sql_rows.rs`, `network/worker.rs` |
| F55 | 三项优化: 投影列/覆盖索引 + 复合写批量化 + UNIQUE 索引/早停 | `sql.rs`, `worker.rs`, `schema.rs`, `sql_rows.rs`, `collections.rs`, `engine.rs` |

### F50: SQL 索引基建

- **schema**: 表内 `[$]` 保留行持久化 + engine 常驻镜像 lazy load (无 schema = 纯 KV 表零回归); ShardManager 控制面 + 数据面 SetSchemaOp 双通道分发
- **row 编码**: `[TAG_ROW=0x07][ver][null bitmap][定长列区][变长偏移][变长数据]`; row 行复用 String 命名空间 `[S][klen][pk]` (pk 点查 = 既有热路径)
- **索引行**: `[I][iid u32 BE][型别字节+保序值][PK]` → 空值 (ZSet score 索引同构); 数值 8B 保序编码, 字符串**转义终结符** (`0x00→0x00FF` + 尾 `0x0000`, memcmp 保序, **不用长度前缀** — 破坏字典序)
- **本地二级索引 (核心决策)**: 索引行与 row 同 shard (按 PK 路由, shard 端 row_put 内部维护) → 写单 shard 原子; **禁止两跳**: IndexScan 广播 → shard 内 "索引扫 + 本地回表" 闭环, worker 只聚合; 范围扫底座 `btree_scan_from` (start ≠ prefix)
- gap: NULL 不入索引 (IS NULL 全表扫); crash 窗口 row 落/索引未落 (回表 miss 跳过兜底)

### F51: SQL INSERT/SELECT (查询规划)

- 手写零依赖解析器 (`sql.rs`): CREATE TABLE / INSERT / SELECT, '单引号' ('' 转义)
- worker 规划: `WHERE pk=` → RowGet 单 shard; 索引列命中 → IndexScan 广播 (界下推, 开界下推闭界 + **全条件残余过滤**兜底); 无索引 → ERR (无全表扫); limit 下推仅当全部条件在选中索引列且 Eq/Ge/Le
- schema conn 级缓存 (F54 升 worker 级), miss 经 GetSchemaOp + sql_pending 挂起续跑; UPDATE = 同 pk INSERT 覆盖 (row_put 自动换索引行)

### F52: MySQL wire 门面 (mysql cli 直连 + auth)

- 5434 端口 (config `sql_addr`/`sql_password`): HandshakeV10 主动发 (splitmix 可打印 salt) → HandshakeResponse41 → `mysql_native_password` (手写 SHA1, RFC 3174 向量单测) + **AuthSwitchRequest 兜底** (8.x caching_sha2 客户端自动切换); 密码错 1045 断连
- COM_QUERY/PING/INIT_DB/QUIT; 老式 EOF 文本结果集 (列定义取 schema, lenenc, NULL=0xFB); 错误码 1064/1054/1105
- 复用 epoll + seq 重排 + SQL 聚合钩子 (`ProtocolKind::Sql`); mysql 8.4 cli 实测全通
- 演进注记: 曾短暂落地自定义行文本协议 (Y2) 作过渡, 同会话内被 MySQL wire 整体替换
- gap: 无 TLS / COM_STMT_* / USE 切库 / 16MB 分片包; RESP 端口回归纯 Redis (SELECT 严格选库, 无 CREATE/INSERT)

### F53: 双层布隆剪枝 (等值查询)

- **shard 本地** (`index_bloom.rs`, 每 shard 每 (db,table,iid) 一个 64K bit 位图, FNV k=2): 等值扫 shard 端 O(1) 拒绝 (免 BTree travel); set_schema 建空 bloom / row_put 喂值 / **开库随 rebuild_composite_counts 扫 [I] 重建** → 重启后剪枝仍生效
- **worker 路由缓存** (`SqlWorkerCache.routes`, per-shard 只增 bloom): INSERT 喂 (value→shard), 等值 SELECT 只向候选 shard 分派, 候选空 → **零任务**直接回空
- **正确性红线 (双层同构)**: 只增不减 (禁精确 map+LRU — 驱逐重积 = 假阴性漏行); worker 层仅 `created_here` 的表启用 (CREATE 时刻零数据 = 空 bloom 完备), 重启/存量表回退广播由 shard 层兜底; 单 SQL worker 前提
- 实测: eq miss 86µs (shard 拒) → 35µs (零任务, 62K qps)

### F54: 回表批量化 + schema worker 级

- `index_scan_local/entries_local` 回表: 逐 pk travel → `table_get_many` (排序 + LeafGuide 区间复用); 等值百行 1.0K→2.4K qps (p50 3.6→1.6ms)
- schema 缓存 conn 级 → worker 级共享 (`Rc<RefCell<SqlWorkerCache>>`, ConnState 持 clone 零签名扩散)

### F55: 三项优化 (投影/覆盖 + 复合写 + UNIQUE)

- **O1 投影+覆盖索引**: `SELECT a, b`; 投影∪条件列 ⊆ {索引列, pk} → `with_rows=false` 免回表, 行值从 (val, pk) 保序编码重建 (worker 与 keyspace 严格同源). 等值百行 3.1K→**5.7K**, 范围 →**6.7K qps (3.3x)**
- **O2 复合写放大**: `kind_of` 探测反转 (先 [#] meta, 已存在 key 2→1 探); HSET/SADD/ZADD 探在+写入批量化 (`get/put_physical_many`). 分散写 HSET 97→**124K**, SADD 76→**232K (3x)**, ZADD 44→**76K**
- **O3 UNIQUE**: `col TYPE UNIQUE` (隐含 NOT NULL, IndexDef.unique 序列化 +1B); row_put **写前**本 shard 探测拒 duplicate key (无半写); worker 等值早停 (首个非空即回复, agg 保留至回包收齐防迟到重复 complete). unique 点查 **36.7K qps / 60µs** (≈pk 级)
- **gap: UNIQUE 跨 shard 漏检** (探测仅本 shard, e2e 实证; 真全局唯一需广播探测/全局索引)

### 本轮 gotchas

- **crash 测试 kill 前必须等 >10s** 周期刷盘窗口, 否则未落盘写丢失会被误判为功能 bug (W3 验收插曲)
- **repro_verify_storage 间歇 hang** (全量套件序列内偶发, 单独跑 12s 过; 与业务改动无关, 杀掉重跑即可)
- mysql cli 分词会剥引号 — SQL 字面量含空格/引号必须走独立端口原文直达 (放弃 RESP 通道 SQL 的根因之一)

### 测试快照

- workspace: **storage 33 suites/467 + net+sm+page+sched 46 suites/317 = 784 passed / 0 failed**, clippy 0
- 实机: mysql cli 8.4 全流程 + kill -9 恢复 (数据/索引/bloom 重建一致); memtier String mixed 217K (历史波动区间 189-313K 内)

---

## 2026-07-29 会话补记 (F49: 纯 String 表跳过复合类型探测, 修性能回退)

- **现象**: mixed 1:1 (4×8 pipeline16) 掉到 ~189-219K (07-28 基线 ~249K)
- **A/B 定位** (临时短路热路径逐项测): 去 SET `purge_composite_if_any` 探测 219K→253K; 再去 GET-miss WRONGTYPE 探测 →292K. **根因是 F45/U2 引入的复合类型探测在纯 String 热路径的固定成本** (每 SET/GET-miss/DEL 各多一次 `[#]key` BTree 点查), 非本轮分库分表改动 (memchr/Arc clone 都极轻)
- **F49 修复**: `StorageEngine` 加**单调提示位** `composite_tables: HashMap<db, HashSet<table>>` — 复合写入口 (hash_set/set_add/zset_add/put_list_meta 等) `mark_composite`, 开库 `rebuild_composite_counts` 扫到 `[#]` 行时重建. 纯 String 表 (`!has_composite`) 的 SET purge / GET-miss WRONGTYPE / DEL 探测**全部跳过** (零额外点查). false positive 无害 (仅多一次探测), 语义完全保留 (有复合类型的表仍正确 WRONGTYPE / purge)
- **效果**: mixed 1:1 回到 ~253-313K (抖动区间, 已达/超 07-28 基线); redis-cli 验证 SET 覆盖 hash 仍正确 purge + WRONGTYPE
- 回归: storage 32 suites/444 + net/sm 15 suites/118 全绿, clippy 0

---

## 2026-07-29 会话 (RESP 分库分表: SELECT id 翻译 + key 冒号前缀选表 + 惰性建表)

### 交付总览

| # | 交付 | 文件 |
|---|------|------|
| F48 | RESP 分库 (SELECT n ↔ DbId 翻译) + 分表 (table:key 冒号前缀) + shard 数据面惰性建表 | `storage/engine.rs+registry.rs`, `shard_manager/request.rs+manager.rs`, `network/worker.rs+resp.rs+server.rs`, `config`, `main.rs` |

### F48: RESP 分库分表

- **选表 (key 冒号前缀, 协议无状态)**: 所有 RESP 命令 key 按**第一个 `:`** 拆 `table:key`; 无冒号 → default 表. 表名限 `[A-Za-z0-9_.-]{1,64}`, 不匹配 (空/二进制/超长前缀) → 整 key 落 default 表 (防二进制 key 撞 `:` 产生垃圾表). 只拆第一个冒号 (`user:1000:profile` → 表 `user`). 实现: **`push_task` 单点重写** (`BatchOp::table_key_mut()`) 覆盖全部单 key 命令; MGET/MSET/MSETNX 分组键 shard → **(shard, table)**; Set 代数/*STORE 源 key 逐个解析 (天然跨表), dst 解析后存 agg; `ConnState.table_cache` 前缀→Arc 复用免热路径分配
- **选库 (SELECT n ↔ DbId)**: 复用存储层既有 `DbNameResolver` (name↔u32, MetaPage 头 1024B 持久化, create_db 2PC 全 shard 同序副本) — **KV 用数字 id, SQL 未来用 name, 协议层统一翻译成 name 传 worker** (BatchOp/路由/引擎零改动). 新增 `ShardManager::DbDirView` (RwLock 双向 map, open 时从 shard 0 拉取 / create_db 后刷新, 只含**真实已创建**的库); `ConnState.current_db` per-connection (断连重置); 越界 → `-ERR DB index is out of range`; 配置 `precreate_dbs = N` 启动预建 db1..dbN. **不自动建库** (库是重资源: 物理目录 + 2PC)
- **惰性建表 (shard 数据面)**: op 执行前 `engine.ensure_table` (registry 缓存命中 = 纯内存查表; miss 则该 shard 本地 create_table, 幂等) — **不走 2PC 控制面**, shard 间物理隔离无需协调. 代价: list_tables 各 shard 视图可能不一致 (RESP 不暴露, 记录在案)
- **顺手重构**: `BatchOp::locator()` 单源 (db,table,key) 提取 — 收敛 manager 两处路由 + worker `hash_route_op` 三份 40+ 变体重复 match (净删 ~200 行); *STORE 二阶段 (db,table) 存 agg 快照 (防 pipeline 中 SELECT 切库后错库)
- 验证: **75 suites / 739 passed / 0 failed** + clippy 0; e2e 新增 resp_t_table_routing (跨表 MGET/MSET/*STORE/边界前缀) + resp_d_select_db (库隔离/越界/断连重置); redis-cli `-n 1/-n 2` 实机隔离 + 冒号分表 + 惰性建表; kill -9 后自动建的表/双库数据/resolver id 映射全恢复; memtier 189K (基线区间内, 无冒号 key 仅 +1 次 memchr)

---

## 2026-07-28 会话 (Redis 数据结构体系: 五大类型 + 统一 meta + Geo/Bitmap)

### 修复/交付总览

| # | 交付 | 文件 |
|---|------|------|
| F45 | 复合数据结构体系: keyspace 编码 + 范围扫描 + Hash/Set/List/ZSet 全命令 | `storage/keyspace.rs+collections.rs+btree.rs`, `page/leaf.rs` (`leaf_scan_from`), `shard_manager`, `network` |
| F46 | 统一类型 meta: SET 覆盖孤儿行泄漏 + 全类型 WRONGTYPE + crash 计数重建 | `storage/keyspace.rs+collections.rs+engine.rs` |
| F47 | 命令面补全: 空洞/List 中段/*STORE/Geospatial/Bitmap | `storage/geo.rs+collections.rs`, `shard_manager`, `network` |

### F45: Redis 复合数据结构体系 (Hash/Set/List/ZSet)

- **统一 key 命名空间编码** (`keyspace.rs`): data 行 `[kind][1][varint klen][key][suffix]` (kind=S/H/L/T/Z); 长度前缀二进制安全消歧 + 顺带解决 `user:1`/`user:10` 前缀包含; 集合子行共享精确前缀天然连续. 编码只在存储边界 (engine/collections), 协议层/路由仍用裸 key
- **范围扫描游标**: `leaf_scan_from` (段内有序遍历) + `btree_scan` (`travel_to_leaf_guided` + `LeafGuide.upper` 跨 leaf 续, 前缀越界早停) — F44 区间 travel 基建的直接变现
- **复合 key 展开** (非胖 value): 每 field/member/元素一行 + meta 行存 count; **ZSet 双索引** member→score (点查) + score→member (`encode_f64_ordered` 保序 8B, 正数翻符号位/负数全翻); List 用保序 i64 idx (XOR 符号位) + head/tail meta
- **RESP 命令**: String 范围类 (GETRANGE/SETRANGE/GETDEL/GETSET/MSETNX) + Hash/Set/List/ZSet 全套; Set 代数 (SINTER/SUNION/SDIFF) worker 端 `SetAlgAgg` 跨 shard 聚合
- **性能回退排查**: GET miss 探全部 4 种 meta 致 127K (基线 218K) → 收窄后由 F46 统一 meta 彻底解决

### F46: 统一类型 meta (`[#][klen][key]` 每 key 唯一行)

- **动机**: 每类型独立 meta 行导致类型检查 5 次点查 + SET 覆盖复合 key 留孤儿行 (空间泄漏) + GET 只探 hash 的 WRONGTYPE 不完整
- **方案**: meta 收敛为 `[#][klen][key]` → value `[kind_byte][count u64]` (List 额外 head/tail) — 1 次探测即知类型; data 行编码不变
- **收益**: `kind_of` 5→2 探测; SET/MSET 写前 `purge_composite_if_any` 清异类旧行 (Redis 语义, 无孤儿); GET miss 1 探测覆盖全类型 WRONGTYPE; `key_delete_any` 1 探测定位 kind 后 purge; 开库 `rebuild_composite_counts` 从 data 行重算 count 修复 crash 漂移
- **性能**: memtier 203K/p99 5.4ms — 不降反升 (复合 op 类型检查减半)

### F47: 命令面补全 (空洞 + List 中段 + *STORE + Geo + Bitmap)

- **C1 空洞**: ZCOUNT/ZMSCORE/ZPOPMIN/ZPOPMAX; SMISMEMBER (新 `BatchResult::IntList`)/SINTERCARD (复用 SetAlgAgg 回 card)/SPOP·SRANDMEMBER count; HSTRLEN (复用 HGet+Strlen 转换, 零新 op)/HRANDFIELD
- **C2 List 中段** (语义改造): **放弃 idx 连续假设** — LINDEX/LSET 改扫描序第 pos 个 (O(n) 符合 Redis), LPOP 容忍空洞收缩重试; LREM (±count)/LTRIM/LPOS (RANK/COUNT)/LINSERT (优先复用空洞 O(1), 无隙搬较小一侧; 搬行必须先物化完整 value 防溢出链被删旧行释放)
- **C3 *STORE**: SINTERSTORE/SUNIONSTORE/SDIFFSTORE + ZINTERSTORE/ZUNIONSTORE (无 weights, SUM); worker 聚合完成点向 dst shard 发 Delete+SAdd/ZAdd 二阶段 (`StoreFinishAgg`, 同 inbox FIFO 保序); 跨 shard 非原子记 gap
- **Geo** (几乎零 shard 层改动): `storage/geo.rs` 52-bit morton geohash (roundtrip <1m) + haversine; GEOADD 解析期直接转 ZAdd (score=geohash, <2^52 f64 精确); GEOPOS/GEODIST 复用 ZMScore、GEOSEARCH 复用 ZRange, worker `GeoCtx` 钩子渲染; GEODIST 京↔沪 1069km (误差 0.2%)
- **Bitmap** (复用 String): SETBIT shard 端 RMW 零扩展 (offset 受 max_value_bytes 保护); GETBIT/BITCOUNT/BITPOS = Get + worker `BitCtx` 位运算 (含全 1 找 0 回越界位等 Redis 边界语义)
- **本轮明确遗留**: BITOP/SMOVE/LMOVE/BLPOP·BRPOP/ZSTORE weights/TTL/Stream/HyperLogLog
- 验证: **75 suites / 736 passed / 0 failed** + clippy 0; redis-cli 全命令实机对齐; kill -9 后 List 中段/STORE dst/Geo/Bitmap 全恢复; memtier 184-203K (基线噪声区间内, 新命令不碰热路径)

---

## 2026-07-27 会话 (String 命令集 + 热路径优化 + ⭐GC 数据丢失修复)

### 修复总览

| # | 修复 | 文件 |
|---|------|------|
| F42 | ⭐ **GC 静默数据丢失**: compact 判活误杀 Internal 页 | `storage/src/pager.rs` (`analyze_compact_read`) |
| F43 | 热路径性能修复 9 项 (同机 A/B: 201K→239K, +19%) | `storage` (page_pool/btree/registry/chunk_writer), `network` (resp/worker/binary), `shard_manager` (request) |
| F44 | String 命令集: MGET/MSET + travel 区间复用 + RMW 命令 | `page/internal.rs`, `storage/btree.rs+registry.rs+engine.rs`, `shard_manager`, `network` |

### F42: ⭐ GC 静默数据丢失 (30s memtier 后早期 key GET 返回 nil)

- **现象**: 少量 key 写入 → 30s 高压写 (~4M key, 大量 compact/drain) → **运行期不重启**直接 GET 早期 key 返回 nil (不报错). `git stash` A/B 证实为既有 bug (非 String 改动引入)
- **根因**: `analyze_compact_read` 判活用 page header vpid 自描述 (`parse_page_vpid` + meta 点查); 但 **Internal 页的 header vpid 字段是 first_child** (page crate 路由约定) → 点查永远对不上 → Internal 页被误判死页: src 侧不搬运 (chunk 释放复用后物理销毁), dst 侧被当死槽覆盖 → 子树路由断, travel 在被覆盖位置撞到 Leaf 提前终止 → 在错误 leaf 找 key → nil
- **修复**: 判活以 **meta 平坦数组全扫为 SoT** (`iter_allocated` 过滤 pid ∈ src/dst chunk 且 PID_ALIVE), 一次遍历同时产出 src 活页表 + dst 死槽表, 零 header 依赖
- **证据**: `NLOG_GC_DEBUG=1` 排查日志 (保留为常备探针) 单轮 30s 压测捕获 **848 条** page_type=2 (Internal) 误判; 修复后原场景 a/b/c 全存活 + kill -9 reopen 完整
- **回归**: `compact_must_migrate_internal_pages` (构造 first_child≠自身 vpid 的 Internal 页驱动多轮 compact)
- **教训**: compact/GC 判活**禁止**依赖页头自描述 —— Internal 页 header vpid 语义被复用; meta 是唯一 SoT

### F43: 热路径性能修复 (审计 9 项, A/B +19%)

- **page_pool 闭环**: pager.read 的 Box 此前从不归还 (池空转, 每 read = malloc+memset+memcpy+free ×16KB) → travel/leaf/submit 消费端 recycle; alloc 免清零 (`new_uninit`)
- **travel_to_leaf_ro**: lookup/update/delete 免 TravelPath 每层 `key.to_vec()`, 且直接返回 leaf 字节省二次 read
- **table_put 单 travel**: `leaf_get_with` 借用窥视旧值 (只物化 13B 溢出描述符) + 原地 leaf_update, 从两次树遍历降为一次
- **BatchOp Arc<str>**: db/table 每 op 两次 String 分配 → 引用计数
- **Put.value 统一 `[TAG_RAW][payload]` 布局**: RESP/Binary decode 物化时预置 1B tag, 删 worker `encode_value` 整值二次拷贝 (1MB value 省 1MB memcpy)
- **解析游标化**: RESP/Binary 循环游标推进 + 末尾一次 drain, 消 pipeline O(n²) memmove
- 其余: write_page_with_vpid 借用传参 / DEL 免 clone 校验 / Binary GET to_vec 记录保留
- **长尾结论** (探针+负载阶梯): fsync 已从主循环消失 (flush 协程 avg 7.6μs), p99 = 饱和排队 (Little's Law 验证 in-flight/吞吐), 非调度病态

### F44: String 命令集 + travel 区间复用

- **区间 travel** (用户提议): `internal_child_with_bounds` 零成本带出左右 separator → `LeafGuide {lower, upper}` 逐层收窄 = leaf 覆盖区间; 批内排序 key `contains` 命中直接复用 leaf 免回 root (实测 500 顺序 key travel < 125 次)
- **MGET/MSET**: worker 按 key hash 分 shard 组 (`ShardTask.group` 回传聚合), shard 内 `table_get_many`/`table_put_many` 区间复用批量执行, 按原始顺序拼回复; MSET 同 key 重复后者覆盖 (稳定排序)
- **table_put_many 防泄漏**: 旧溢出链 leaf 提交成功后才释放 / 新链提交失败回滚; PageFull 退化单 key split 路径 (root 变化跟踪)
- **RMW 命令**: INCR/DECR/INCRBY/DECRBY/APPEND/SETNX shard 端执行 (单线程天然原子); EXISTS 多 key Get 聚合; STRLEN/TYPE Get 语义转换
- 大 value 溢出页 (F41 后续): `max_value_bytes` 3000→1MB, 13B 间接描述符 (0x00 标记与 value_codec tag 空间免冲突), PID_FREED 墓碑防 recover 复活, 覆盖写/删除全链路防泄漏
- 验证: 75 suites / 708 passed / 0 failed + clippy 0; redis-cli 全命令语义对齐; MSET(10 keys) 107-132K cmd/s ≈ 1.1-1.3M key/s

---

## 2026-07-26 会话 (多协议门面 + 两个关键死锁/损坏修复 + 异步落盘)

### 修复总览

| # | 修复 | 文件 |
|---|------|------|
| F38 | 协议层三件套: value type tag + KV 长度限制 + RESP2 (Redis 兼容) 门面 | `network/src/value_codec.rs` (新), `protocol/resp.rs` (新), `protocol/mod.rs`, `worker.rs`, `server.rs`, `config`, `src/main.rs` |
| F39 | ⭐ **pollster 死锁**: IoUring 下 shard 线程永久 futex 睡死 | `shard_manager/src/manager.rs` (`block_on_io`) |
| F40 | ⭐ **leaf_update 段首损坏**: 覆盖写段首 item 破坏 shared=0 不变量 | `page/src/leaf.rs` |
| F41 | 异步 chunk 落盘 + 有界背压 + reply 通知合并 + send_reply 顺序竞态 | `storage/src/pager.rs`, `pager_io.rs`, `chunk_writer.rs`, `shard_manager/src/manager.rs`, `task_reply_bus.rs` |

### F38: 多协议门面 (Redis 兼容)

- **value type tag**: 存储格式 `[tag u8][payload]`, 本阶段全写 `TAG_RAW=0x01`, 预留 I64/F64/STR/DOC 给 SQL/Mongo 门面 (避免存量迁移); worker Put 打 tag, Get 剥 tag
- **KvLimits**: 默认 key≤1024 / value≤3000, parse 后进 shard 前拦截. 上限依据: page 编码路径 `[0u8; 4096]` 栈缓冲 = 单 item 硬上限; config 校验 `max_key + max_value <= 4060`
- **RESP2 门面**: SET/GET/DEL(多key聚合`:N`)/PING/ECHO/AUTH/QUIT/HELLO/SELECT/COMMAND; AUTH 按 Redis 官方语义 (NOAUTH/WRONGPASS/no password is set), worker 本地处理不进 shard
- **FIFO 重排** (RESP 无 req_id): per-conn 递增 seq 作 req_id, 回复经 BTreeMap 重排严格按序; 本地命令也占 seq 保证 pipeline 顺序
- **双协议 server**: main 同时起 Binary(5433) + RESP(6379), worker_id 空间隔离 (`worker_id_base`), `ShardManagerOptions.reply_bus_count` 扩容
- 验证: 真实 redis-cli 全链路 AUTH/SET/GET/DEL/PING 通过

### F39: pollster 死锁 (现象: PING 通、SET 永久卡死)

- **现象**: 服务器启动后每个 shard 在第一次写入后 ~10s (周期刷盘) 准时卡死在 `futex_do_wait`; Ctrl-C 无 "stopped"
- **根因**: shard 线程用 `pollster::block_on` 跑 engine async; IoUring 下 `io_ops::fsync` 首次 poll 提交 SQE 后 Pending, pollster park 线程; 而 CQE 收割在**下次 poll 的 CQ 扫描**里 — 线程睡死后无人再 poll → 永久死锁. buffered write 常在 submit 内同步完成所以 stress 一直没暴露; fsync 被 punt 到 io-wq 必输竞态
- **修复**: `block_on_io()` — Pending 后短 spin/yield 重 poll (poll 内部自带 CQ 收割), 替换 manager.rs 全部 20 处 pollster. 符合 "Future 自取 CQE" 契约

### F40: leaf_update 段首 shared=0 损坏 (memtier 发现)

- **现象**: memtier 首轮即报 `-ERR ... segment head item must have shared=0, got shared=15`
- **根因**: 被覆盖的 key 恰是 checkpoint 段首时, 段内扫描第一个就命中, `prev_ptr` 被初始化为 target 自身 → `prev_key == key` → 重编码 shared=len-1, 破坏段首自包含不变量. 长公共前缀 key (memtier-XXXXXXXX) + 覆盖写必现; 顺序 key 测试难触发
- **修复**: target 在段首 (`prev_ptr.byte_offset() == old_off`) 时 prev_key 视为空; 回归测试 `update_segment_head_keeps_shared_zero`

### F41: 异步 chunk 落盘 + 有界背压 (写吐出 3.5x)

- **问题**: `drive_write_queue`/`maybe_periodic_flush` 在 `block_on_io` 内串行 write+fsync 阻塞 shard 循环 → 写重 p99 40ms
- **方案**: 所有权转移式异步化 — `FlushJob{key, bytes: Rc, dir, io: Rc<PagerIo>}` 零 Pager 借用, shard 线程 `spawn_on` 协程落盘, 主循环每轮收割 (`complete_flush`) + `drive_until_idle` 推进; 磁盘 IO 与内存写完全并发
- **有界背压**: `MAX_INFLIGHT_CHUNKS=8`, 超限时 swap 退化同步落盘 (写入自然降速到磁盘速度, 零死锁风险)
- **正确性钢筋**: 同 key 去重 (不并发写同 offset); 读路径 in-flight 可见 (五源查找); meta 仅在 backlog 排空后刷 (data→meta 不变); flush/close 前 `drain_async_flush` 排空; 失败回 pending 重试
- **附带**: TaskReplyBus 通知合并 (首条写 eventfd, N 条回复 N→1 次 syscall); send_reply 顺序竞态修复 (先推 sink 再唤醒 client)
- **效果** (memtier, io_uring, 真实持久化): 写重 1:1 44K→**153K** ops/s (p99 40→16.7ms); 读混合 1:10 298K→**1.06M** ops/s (同步刷盘停顿之前连读一起卡)

### Benchmark 快照 (同机 Redis 8.6.2 对照, memtier 2t×10c, 32B)

| 场景 | NexusDB | Redis (AOF everysec) |
|---|---|---|
| pipe=16, 1:10 | 1.06M | 1.83M |
| pipe=16, 1:1 写重 | 153K | 1.51M |

差距构成: worker↔shard 两跳 handoff (可解, 终局方案 shard 自包含网络) + 每写 16KB 页 COW 写放大 (WAL 可解) + 有序 B+Tree vs hash (结构性).

---

## 2026-07-25 会话 (btree 路由修复 + 通信层优化 + 独立服务架构 + 成品化)

### 修复/特性总览

| # | 内容 | 文件 |
|---|------|------|
| F33 | ⭐ **btree_insert leaf split 路由错误** (stress phase4 1-3/600 key 丢失的根因): split 后无条件插 right, 非顺序插入下 key < split_key 时错位 → 改条件路由 `key > split_key ? right : left`; 附带 MetaCache 零槽 phantom entry 修复 | `storage/src/btree.rs`, `meta_cache.rs` |
| F34 | shard 通信层: ShardInbox (无锁 ring + eventfd + coalescing) 替换 mpsc; spin-then-park; adaptive spin-poll; Batch API (`batch_ops`, batch=64 时 +98%) | `shard_manager/src/inbox.rs` (新), `reply.rs`, `manager.rs` |
| F35 | NowChunks 自动持久化: chunk 满 (64 page) 自动 swap 入 WriteQueue; 周期(10s)/计数(256写) 刷盘; **退出完整性** (close 排空 + break 后置); ⭐ WriteQueue stale 快照回滚覆盖新数据修复 (`remove_pending`) | `storage/src/pager.rs`, `chunk_writer.rs`, `engine.rs` |
| F36 | 独立服务架构: TaskInbox/TaskReplyBus (network→shard 直连, 零 client 线程模型), worker 重写为 epoll 事件循环, pipeline 支持; ⭐ EPOLLET 丢事件改水平触发; ⭐ server accept 连接补 TCP_NODELAY (Nagle 延迟 13x); inbox drain 丢唤醒竞态修复 (先重置 pending 再 pop); ReplyFuture waker 修复 | `shard_manager/src/task_inbox.rs` (新), `task_reply_bus.rs` (新), `network/src/worker.rs` (重写), `acceptor.rs`, `server.rs` |
| F37 | 成品化: `config` crate (TOML+serde), `nlog` crate (io_uring 协程融合 logger, 无锁前端 + 专用 log 线程 + 量/时间双阈值), main.rs 服务器化 (信号优雅退出); ⭐ scheduler `io_registry.take_result` 误删未完成 entry 修复 | `crates/config/` (新), `crates/logging/` (新), `src/main.rs`, `scheduler/src/io_registry.rs` |

### 其他要点

- io_backend 切换 io_uring (避免等待期内核切换): stress 192K→368K ops/s
- 多处测试 PRNG seed `tid * 0x9E37...` debug 溢出 → `wrapping_mul` (repro_verify×2, stress×2)
- leaf_split 统一为 checkpoint 段边界 bulk memcpy (字节中点选段 + 整段 copy + `force_split_segment_at_mid`), 无双路径分歧
- 网络压测结论: 单连接 ping-pong 是客户端瓶颈, pipeline=16 + TCP_NODELAY 后 16K→53K→61K (12conn×pipe64)

---

## 2026-07-24 会话 (Async Network Stack + missing key 排查)

### F32: network crate — 异步网络栈骨架搭建

| F32 | **Async Network Stack Phase 1-4: 网络栈骨架 + 压力测试 + missing key 排查** | `crates/network/` (新建 crate, 7 个源文件 + 1 example + 4 个测试文件), `crates/scheduler/src/await_predicate.rs` (新建), `crates/scheduler/src/park.rs` (新建), `crates/storage/src/pager.rs` (read 路径加固), `crates/network/examples/network_stress.rs` (新建), `crates/network/tests/repro_verify.rs` (新建, 9 测试), `crates/network/tests/repro_verify_minimal.rs` (新建, 1 测试), `crates/storage/tests/repro_verify_storage.rs` (新建, 3 测试) | |

#### 架构变动

1. **新建 `network` crate** — 7 个模块:
   - `protocol/` — `Protocol` trait + `BinaryProtocol` 实现 (二进制帧: `|total_len:u32|req_id:u64|op:u8|key_len:u16|val_len:u32|key|val|`). max frame 16MB.
   - `acceptor.rs` — 非阻塞 acceptor loop, 支持 RoundRobin/Random/Sticky LB 策略
   - `worker.rs` — WorkerPool: N 个 worker thread, 每个 conn spawn OS thread, 同步 ShardManager API
   - `server.rs` — NetworkServer 顶层组装: acceptor + worker pool + 优雅关闭 (AtomicBool stop + drop inbox)
   - `kv_to_shard.rs` — Application Layer: Request → ShardManager::put/get/delete → Response
   - `reply_bus.rs` — ReplyBus: crossbeam unbounded channel, 实现 `ReplySink` trait, 支持异步 reply 路由 (Phase 1 完成, Phase 6+ 正式接入)

2. **Scheduler crate 扩展**:
   - `await_predicate.rs` — `AwaitPredicate` future: 基于谓词的协程等待, 配合 `park::register_parked` 实现
   - `park.rs` — park/unpark 机制, 全局 slot 存储 waker

3. **Pager read 路径加固** (⭐ 关键修复):
   - `Pager::read()` 和 `read_into()` 现为**四源查找**: `nowchunks → WriteQueue(pending) → WriteQueue(completed) → chunk_list → disk`
   - 新增 `WriteQueue::peek_chunk_pending()` 和 `peek_chunk_completed()` 方法, 让读路径能看见 WriteQueue 中正在落盘或已完成落盘但尚未插入 chunk_list 的数据
   - 修复: 之前读路径只查 nowchunks + chunk_list + disk, 忽略 WriteQueue, 导致 `flush` 过程中 put 后立即 get 可能读到 stale 数据

#### 压力测试工具

- `crates/network/examples/network_stress.rs` — 多 client 多 shard 完整网络层压测:
  - Phase 1: warmup (N clients × 200 put)
  - Phase 2: mixed workload (50/30/20 put/get/delete, N clients × M ops)
  - Phase 3: setup verify keys (N clients × 100 put)
  - Phase 4: verify (重读所有 verify keys)
  - 输出: ops/sec, error count, verification errors

#### 问题排查: missing key bug

**现象**: 高并发压力测试 (6 clients, 6 shards, ~30K total ops) 下, phase 3 写入的 key 在 phase 4 验证时 ~0.2% 返回 `Get(None)`, 即 key 丢失。

**排查过程**:

1. **旧框架 (原生 ShardManager 同步 API)** — 在 T14 同步 API 下运行 stress.rs, 发现 phase 4 有 missing key
2. **新框架 (NetworkServer + TCP)** — 在 network_stress 下运行, 发现 missing key 仍然存在 (~0.2% 错误率)
   - 性能提升: 50K → 143K ops/sec (因 NetworkServer 多 worker 线程并行处理)
   - 但正确性问题未解决
3. **Storage 层独立复现** — 新建 `repro_verify_storage.rs` 在 storage 层直接模拟 phase 2 + phase 3 流程, 排除网络层干扰
   - 发现 `phase3_put_v0_then_get_v0_works` 在高并发下仍然有 missing key
   - 确认 bug 在 storage 层, 不在网络层或 ShardManager 层
4. **最小化复现** — `repro_verify_minimal.rs` 最小化场景: 6 shard × 6 client × phase 1+2+3+4
   - 假设根因: `nowchunks.peek_chunk` 在并发读写交错时可能返回 stale bytes

**关键发现**:
- 单线程场景下永不触发 (包括 `just_phase1_then_phase3_sequential`, `single_threaded_phase2_then_phase3`)
- 仅在多 client 并发时触发
- Phase 1 (warmup) + Phase 2 (mixed) 组合才触发, 单独 phase 2 不行
- 说明问题与**并发写入 + 存储层数据竞争**相关

**已实施的修复**:
- Pager read 路径加 WriteQueue 检索, 确保 flush 过程中的数据对读路径可见

**待深入排查**:
- BTree insert 过程中, 并发 get 可能读到 stale leaf page (nowchunks 中插入后尚未 meta_cache write)
- 多 shard 间 hash 路由的 key 分布可能在某些 shard 上造成热点, 触发 chunk_full → rotate 期间的竞争
- 建议在 `btree_insert` 和 `btree_lookup` 中添加更细粒度的调试日志, 追踪特定 key 的 put/get 时间线

#### 测试状态

基础测试 (page + storage + shard_manager + network fast tests) 全部通过:
```
page:          131 passed ✅
storage:       386+ passed ✅ (不含 repro_verify_storage 慢测试)
shard_manager: 28+ passed ✅
network:       21 passed ✅ (end_to_end/integration_reply_bus/protocol_binary/reply_bus)
workspace:     ~549 passed ✅ (0 failed, 2026-07-22 快照)
```

> **注意**: `repro_verify_storage` (3 测试) 和 `repro_verify` (9 测试) 和 `repro_verify_minimal` (1 测试) 为高并发复现测试, debug 模式下跑非常慢 (~10 分钟), 建议 `cargo test --release` 运行.

#### clippy 状态

全 clean, 0 警告 (page crate 旧 warning 除外).

---

## 2026-07-22 会话 (T14: ShardManager 2PC + 同步 API) — 贡献摘要

**F31 T14 ShardManager 2PC**: 协议消息 `Prepare/Commit/Abort × {Db, Table}` + `TwoPhaseCoordinator` 状态机 (`coordinator.rs` ~330 LOC); `ShardManager::create_db/create_table` 走 2PC 流程; 15 lib + 8 e2e 全过. 同步 API 性能瓶颈 (主线程串行) 识别并交给 T15 async API. 关键测试: `two_pc_metadata_with_cross_shard_routing` (40 key 跨 4 shard) / `_persists_across_reopen` / `_duplicate_triggers_abort` / 另 5 个. 详见 git log F31.

---

## 2026-07-21 会话 (T17 全 async 重构 + ⭐T17b 64x→1x 写放大修复 + T15 多层 BTree + reopen) — 贡献摘要

- **⭐ F30 T17b: Pager::flush 写放大 64x→1x (本阶段最大亮点)**. 原 flush `disk_read(1MB) + merge + disk_write(1MB)` 相对 16KB page write 是 **64x 写放大**; 修复后直接写 nowchunks 1MB. 顺带实现 **vpid 复用** (in-nowchunk 原位覆盖同一 pid, `MetaCache::is_dirty` API). 设计文档 `docs/superpowers/plans/2026-07-18-storage-crate.md` §3.3.1. 8 e2e 多 vpid 行为更新.
- **F29 T17 全 async 重构**: `PagerIo` 抽象 (`StdFs` / `IoUring`); 入口方法全部 async 化; ⭐ **Stack Overflow 修复** — async fn 内联后 16KB 局部累积, 默认 8MB 线程栈不够, **测试用 `RUST_MIN_STACK=67108864` (64MB)** (`tests/common/mod.rs` 文档化). 测试 367→386 (+19).
- **F28 T15.1 chunk_offset 根因**: 文件内偏移误用全局偏移 → file 2 的 page 14 被写到 sparse 末尾, reopen 后 vpid 路由断. 改为 `chunk_idx * CHUNK_SIZE` (文件内偏移), scan_block_file 加 sparse 容错. 同会话完整修复 + T15 7/8 测试由 F28 补齐.
- 详见 git log F28/F29/F30.

---

## 2026-07-20 会话 (T12: ShardManager 集成 — T12.1-T12.21 全部完成) — 里程碑摘要

T12.1-T12.3: types.rs 加 `DbId` type alias + `MetaKey` 复合 key + `IoBackend` enum.
T12.4-F22/F21: ⭐ MetaCache v2 重写 — 抛弃 10MB sliding window, 改 per-shard LFU + BinaryHeap freq tracking + **修复** `evict_if_needed` 用 soft cap 作触发 (旧版 hard cap 漏掉 `soft < len < hard` 区间).
T12.6-F23: MetaCache 加 DbId 维度 + 13 新测试.
T12.7-T12.10-F24: VpidAllocator / PidAllocator / FreePageQueue / ChunkList-Key / ChunkWriter 全部加 DbId 维度 + 17 新单元测试.
T12.12+T12.13-F25: Pager/recover 走 `{block_root}/{db_name}/shard_N/` 路径, 三级 fallback (compat 直接走 block_dir).
T12.14-T12.17-F26: DbNameResolver (MetaPage 1024B 段) + StorageEngine `current_db` + ⭐ MetaPage COW 修复 (META_VPID 走固定 PID).
T12.18-T12.21-F27: ⭐ **关键 bug 修复** — `StorageEngine::open` 路径 tuple 第二项 `db_name` 被硬编码为 `DEFAULT_DB_NAME.to_string()`, 导致多 db 模式 recover 永远走 default 目录; 新增 `multi_db_physical_isolation.rs` 9 e2e.

**收尾口径**: 367 passed, clippy 0, fmt 0. **T12 全部 21 子任务完成**. 详见 git log F20-F27.

---

## 2026-07-19/18/17 会话 (Storage T1-T11 + Page F1-F12 早期) — 里程碑摘要

- **Page crate 早期 (F1-F12)**: LCB-Tree 页头 40B (`LCBP` magic + page_type + vpid @0x18); ItemKind/ItemPtr + PageIndex 段二分 + 段内 next + checkpoint 数组; Item prefix-compress (shared=0 哨兵 + varint len). 关键 bug 修复: F1 pre_split 漏重写 k+1 (段首 shared 错位) / F2 total_delta wrap → checked_add + panic / F3 pre_split 后未 write_back / F4 internal_delete 缺 PageIndex 更新 / F5 dump 调试模块 / F6 internal_push_back cp 边界 seg_idx 错位 / F7 split_delete chaos right_base 快照时机 / F8 多轮 split mid_off 边界偏移 / F11 internal_delete k+1 重写 + effective_seg_idx / F12 apply_pre_merge_steal 测试. 详见 git log F1-F12.
- **Storage T8-T11 (F13-F19)**: T7 recover (page header 自描述 + MetaCache union 语义); T8 NowChunks `vpid_map` + Pager::flush disk-in-memory merge; T9-T10/T11 **⭐ F17 aliasing UB 修复** — `TableDirectory` 移除 `*mut Pager` 字段 (改 `PhantomData<*mut Pager>` 保留 !Send/!Sync); **F18 `.truncate(true)` 导致数据丢失** — clippy auto-fix 给 4 处 `OpenOptions::new().create(true).truncate(true)` 在 reopen 已存在文件时清空, 全部改为 `.truncate(false)`; F19 catalog 一致性 12 个新测试. 详见 git log F13-F19.
- **T9-T11 catalog 设计 (Storage 关键设计决策)**: **MetaPage** (chunk 0 page 0, db_name→table_dir_root_vpid BTree) + **TableDirectory** (table_name→root_vpid 单 leaf BTree, 多 page 升级到多层) + **DbRegistry** write-through cache (HashMap 镜像, cache 永不超前 page.mate). 这三个是后续 T12-T14 多 db 物理隔离与 Network 多协议的数据基石.
- 测试贡献: Page 131 + Storage 282

## 历史索引 (近 4 段完整保留, 其余已全部压缩)

- 环境注意事项 (cargo 镜像 / Rust edition / io_uring 串行 / JoinInner 跨线程 race): 已被各具体段的 gotcha 内化, 无独立段
- 完整测试文件清单 (`crates/{page,storage,network,shard_manager}/tests/` 全文件目录): 跟随 `git log CHANGELOG.md` 取任意历史版本可查, 内容与代码目录同步变化
- 全部 F 编号 (F1-F44) / T 编号 (T1-T17b + T12.1-T12.21) 哈希检索锚保留在本快照中, 不会丢失 (56 lib + 226 integration). 完整实现细节见 `docs/superpowers/plans/2026-07-17-page-item-revision.md` + `docs/superpowers/plans/2026-07-18-storage-crate.md`.

| F12 | **新增 `apply_pre_merge_steal` 4 个单元测试** | `tests/steal_tests.rs` | 覆盖: steal 触发 (left 达 MIN) / left>=MIN 不触发 / 无右邻不触发 / right 太小不触发 |
| F1 | **pre_split_segment 漏重写 k+1: 重编码 mid item 为 shared=0 后, 需用 `mid_full_key`(不是 mid-1) 还原并重编码 k+1** | `index.rs` | 修复 cp 段首 shared!=0 的根本原因 |

---

## 整体测试状态快照

### 2026-07-26 (F38-F41: 多协议门面 + 异步落盘 + 两个关键修复)

```
workspace 全量:            71 suites / 682 passed, 0 failed ✅
clippy:                     0 警告 ✅
新增测试覆盖: 5+8+6 (network) + 3+1 (storage/page) = 23 个 F38-F41 相关测试
Benchmark (memtier, io_uring): 读混合 1.06M ops/s, 写重 153K ops/s p99 16.7ms, stress 10000×6 verify 0/600 PASS
```
(早期 1.06M/153K 数据为 F38-F41 当基线, 已由后续 F42-F44 替代到 240-310K 和 1.1-1.3M key/s. 早期 T12 文字同理, 此处仅保留 7-26 最新基准)

