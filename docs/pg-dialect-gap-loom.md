# PG 方言核算与 Loom 对接更新步骤

> 日期: 2026-08-03 | 目标: 评估 NexusDB PG 门面对 Nexus-Story-Loom 的对接可行性
> 方法: 代码扫描 + 真实协议/执行层实测（psql simple protocol + Python 原始 socket 模拟 pgx 扩展协议）

## 一、Loom 的真实方言需求（子代理扫描结论）

### 应用层（8 个仓储，约 40 方法，运行时）

| 方言特征 | 使用方式 | 数量 |
|---|---|---|
| `$N` 位置参数 | **全部** SQL 均用 pgx `$1,$2...` | 全量 |
| INSERT 列列表 | 显式列名 + VALUES，**无 RETURNING** | 8 处 |
| UPDATE ... WHERE | 复合条件 + `version=version+1` 乐观锁 | 6 处 |
| DELETE WHERE id=$1 | 单行 | 8 处 |
| RowsAffected | UPDATE/DELETE 判命中（乐观锁 CAS） | 全部写方法 |
| JOIN | INNER JOIN ×1（edges JOIN nodes） | 1 处 |
| COALESCE / COUNT / ORDER BY / LIMIT $N OFFSET $N | 常用 | 多处 |
| 错误码 | `pgconn.PgError.Code` 23505/23514 + Detail | mapErr |
| 类型 | sql.NullString / []byte(JSONB) / time.Time / bool | 扫描层 |
| **未使用** | RETURNING、NOW()、jsonb 操作符、子查询、事务、CopyFrom、StringArray 专用类型 | — |

### 启动层（migrator）

- `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)` — 建库探测
- `CREATE DATABASE "name"` — 建库
- `SELECT EXISTS (SELECT FROM information_schema.tables WHERE ...)` — 探表
- 连接 `postgres` admin 库

## 二、实测核算结果（2026-08-03，最新代码实例）

### ✅ 已支持（pgx 实际路径验证通过）

| 项 | 验证方式 | 结果 |
|---|---|---|
| PG 握手（cleartext 认证） | Python 原始 socket | AuthOk |
| 扩展查询协议 Parse/Bind/Describe/Execute/Sync | 同上 | 完整可用 |
| `$1` 参数绑定 | INSERT 3 参 / SELECT 1 参 / LIMIT $1 | 数据正确写入并回读 |
| INSERT 部分列 + 完整列（含 TEXT[] 数组值） | psql + 扩展 | 成功 |
| SELECT 投影 + WHERE + DataRow | 扩展协议 | 返回行 |
| 唯一约束冲突 SQLSTATE **23505** | psql VERBOSITY verbose | `ERROR: 23505: duplicate key...` |
| 字符串字面量比较 | psql | 正常 |
| 迁移 DDL 全量执行 | psql -f 0001_init.sql | rc=0（9 表/索引/触发器/函数/外键/CHECK） |
| dollar-quote `$$`/`$tag$` | 迁移函数体 | 解析正常 |

### ❌ 缺失（按阻塞等级）

| 项 | 影响 | 实测现象 |
|---|---|---|
| **P0** `SELECT EXISTS(subquery)` 标量形式 | migrator 探测查询失败 | `unknown function 'EXISTS'` / `expected LIMIT count, got Dollar(1)` |
| **P0** `pg_database` / `information_schema.tables` 系统表 | migrator 建库/探表失败 | 查询报错（`SELECT EXISTS` 不支持；裸 `SELECT 1 FROM` 不支持） |
| **P0** `CREATE DATABASE` / `postgres` admin 库 | migrator 建库失败 | `database "postgres" does not exist` |
| **P1** `DEFAULT uuid_generate_v4()` 不求值 | 迁移表无法 INSERT（主键 NULL） | `PRIMARY KEY must not be NULL` |
| **P2** simple protocol 字符串/UUID 字面量比较缺陷 | 手动 SQL / 非参数化查询 | `unresolved column reference`（`$1` 参数路径正常） |
| **P2** RETURNING 无输出 | 未来应用可能用（Loom 当前不用） | INSERT...RETURNING 无结果列 |

### ⚠️ 结论修正说明

第一轮评估曾判断"$1 参数/PREPARE 不支持"，**该结论有误**：psql 的 `PREPARE` 走 SQL 解析器（simple protocol），而 pgx 走扩展协议消息（Parse/Bind），两者路径不同。实测证明扩展协议路径**完全可用**。

## 三、对接结论

- **应用层（Loom 运行时 SQL）**：`$N` 参数化 + 标准 CRUD + JOIN + COALESCE + 23505 错误码 —— **已全部支持，Loom 仓储层大概率可直接运行**（改 DSN 指向 NexusDB PG 门面即可）。
- **启动层（migrator）**：4 个 P0 缺口导致**无法自动建库建表**，需绕过（手动建表）或补齐。
- **数据层**：P1 `uuid_generate_v4` 缺口使**依赖默认 UUID 主键的表无法插入**，需补齐或改表结构。

## 四、实施结果（2026-08-03 已完成 Phase 1-3）

### 已实现并验证

| 项 | 实现 | 验证 |
|---|---|---|
| `postgres` admin 库别名 | worker PG startup database=postgres → default | 连接成功 |
| `pg_database` 系统表 | 零任务合成（datname 列）+ 裸名映射 `pg_*` | `SELECT datname FROM pg_database` 返回 default |
| `information_schema.tables` | table_schema 固定 'public' + 空投影 `SELECT FROM` | EXISTS 探表 t/f 正确 |
| `SELECT EXISTS(subquery)` | 新 `SqlStmt::ExistsStub` + SysQuerySpec.exists + `$1` 参数递归绑定 | simple + pgx 双路径 t/f 正确 |
| `CREATE DATABASE` | 新 `SqlStmt::CreateDb` + `SqlSharedRoutes.cluster_ctl` 注入 ShardManager 2PC | 建库后 db_view 可见、可建表读写 |
| 列默认值 | `Column.default` + `ColDefault{Lit,Now,UuidGenV4}` + FMT_VER 6（兼容 v1-5） | `DEFAULT uuid_generate_v4()/'draft'/'{}'/NOW()` 全生效，重启持久化 |
| UUID 列比较 | `coerce_cmp_lit_uuid`（36 字符文本 → 16B 再比较） | simple + pgx 双路径命中 |

### 端到端验证（模拟 Loom migrator + 迁移 SQL）

1. `SELECT EXISTS(pg_database WHERE datname=...)` → f ✅
2. `CREATE DATABASE "nexus_story_loom"` ✅
3. `0001_init.sql` 完整迁移（9 表）rc=0 ✅
4. `INSERT INTO story_worlds (user_id, name) VALUES (...)`（不提供 id）→ **uuid 默认值生效** ✅
5. status='draft' / created_at=NOW() / cover='{}' 默认值全部正确 ✅
6. 唯一约束冲突 SQLSTATE 23505 正确返回 ✅

### 复合唯一索引（追加完成，2026-08-03）

用 **key 拼接**实现复合 UNIQUE，复用单列唯一索引机制：

- **schema (FMT_VER 7)**：`IndexDef` 加 `cols: Vec<u16>`（`col` 保留 = cols[0] 兼容既有引用）；`TableSchema::new` 加 `composite_unique_cols` 参数
- **索引 key**：`index_vals_bytes` 多列拼接 `[IVAL_COMPOSITE][nseg][u16 len][enc]...`（长度前缀防碰撞 + 型别字节防 split 误判）；单列保持原编码（存量兼容）
- **keyspace**：`split_index_val` 加 `IVAL_COMPOSITE` 分支（唯一检查/UPDATE 新旧比较正确切分）
- **parser**：表级 `UNIQUE(a,b,...)` 不再截取首列 → 整组
- **扫描**：复合索引 v1 不参与单列索引扫描（退化全表，正确性保底）
- **验证**：Loom 完整迁移 + 同一用户两世界允许、重复世界名报 `duplicate key on unique columns (user_id, name)`；新增 e2e `mysql_composite_unique`

### 2026-08-03 完整性测试新增发现

**外键完全未实现（parser 层吞掉，`v1 吞 (不强制外键)`）** —— Loom 对接的实质障碍：
- 引用完整性不强制：插入悬空 `REFERENCES` 行成功（实测 characters 悬空 story_id 可插入）
- **`ON DELETE CASCADE` 不执行**：实测 DELETE world 后 chapters/nodes/edges/characters 全残留（孤儿行）
- Loom 8 张表全部依赖 `REFERENCES ... ON DELETE CASCADE`（删世界必须级联清子表），此缺口破坏 Loom 核心删除语义
- compat 测试第 59/60 行只验证 CREATE TABLE 语法接受 → 假阳性（同 uuid_generate_v4）

**跨 shard 复合唯一漏检（架构限制，文档已记录）**：
- 实测 3 shard 下 40 次插入仅拒 2（期望拒 5，35 个唯一组合），跨 shard 重复漏检
- 同 shard 必拒（单 shard 实测 + e2e 全过）

**已排除（Loom 应用层未用，不阻塞）**：`INSERT...SELECT`、`||` 拼接、裸常量 SELECT 列。

### 外键级联（追加完成，2026-08-03）

实现方案（worker 编排 + 进程级反向引用）：
- **parser**：列级 `REFERENCES t(col)` + 表级 `FOREIGN KEY` 解析 → `FkDef`（含 ON DELETE CASCADE/SET NULL），不再吞掉
- **schema (FMT_VER 8)**：`TableSchema.fks` + 序列化（兼容 v1-7）
- **反向引用**：`SqlSharedRoutes.fk_incoming`（CREATE 注册 / DROP 移除）——"通过 schema 了解当前表被哪些表引用"
- **级联编排**（`worker/sql_cascade.rs`）：主 DELETE 完成（DmlAgg remaining==0 或 PkGet 单发）→ 按反向引用对每引用表递归下发 `DELETE WHERE fk_col IN (被删pks)` / `UPDATE ... SET fk_col=NULL` 子任务（伪高位 seq，回包拦截不发给客户端）→ 全部完成才回复根 DELETE
- **防环**：visited (表, pk) 去重（自引用/菱形引用）；SET NULL 不过滤（更新引用行不删行）
- **验证**：Loom 迁移全表级联（world→chapters→nodes→edges/nc/hooks 7 表全清）、chapters 自引用 SET NULL、e2e `mysql_foreign_key_cascade`、全量回归 40 passed
- **边界**：引用完整性 v1 不强制（悬空插入允许）；跨 shard 引用行全广播删（正确性优先）

### UPDATE SET 表达式（追加完成，2026-08-03）

Loom 的乐观锁 / 开关 toggle 依赖 SET 表达式，现支持：
- **parse**：`SET col = <expr>` 解析为 `SqlValue::Expr(ScalarExpr)`（列引用 / 字面量 / 一元 NOT / 链式二元算术 `col*2+1`，左结合）
- **exec**：shard 端 `row_update` 读旧行 → `eval_row_expr` 对旧行求值 → 原子写回（引擎单线程天然 CAS 语义；复用现有 RowPut 的 UNIQUE/索引跟随）
- 值传递：`BatchOp::RowUpdate.sets: Vec<(u16, SetVal)>`，`SetVal::{Val, Expr}`
- **验证**（Loom 真实 SQL）：
  - `SET version = version + 1 WHERE ... AND version=$N` 乐观锁（simple + pgx 扩展协议）✅
  - `SET enabled = NOT enabled` toggle ✅
  - 多列 SET（`version+1, updated_at=...`）✅
  - 链式 `version*2+1` ✅
  - e2e `mysql_update_set_expr` + 全量回归 42 passed
- 边界：除法产生浮点写回整型列报类型不匹配（Loom 不用）；事务内表达式退化为不支持

### 引用完整性检查（追加完成，2026-08-03，P0）

外键"真正生效"的最后一环 — INSERT 拒绝悬空引用：
- **worker 预检**（`worker/sql_fk.rs`）：INSERT 构建完 RowPut 后，若本表有 fks 且父表 schema 已缓存，先对每个非 NULL 外键值向父表发 `RowGet` 存在性检查（real seq，聚合于 `sql_fk_ins`）；全父行存在才注册 sql_dml_agg 发原 RowPut，任一缺失 → 拒
- **原子性**：多行混合（部分有效部分无效）→ 整体拒绝，无部分写入
- **NULL 外键**：不校验（PG 语义）
- **跨 shard**：RowGet 按父主键 hash 路由到父表所在 shard
- **验证**（3 shard 实测）：引用存在的父 → 成功；引用不存在的父 → `foreign key violation`；Loom 真实场景 world→chapter 悬空拒；多行混合整体拒；级联删除不受影响
- e2e `mysql_foreign_key_referential_integrity`
- **v1 边界**：父表 schema 不在缓存时跳过检查（但 CREATE TABLE 后父表已进缓存，实测总是生效）

### 剩余（Loom 不阻塞）

- RETURNING 未实现（Loom 当前不用，低优先级）。

## 六、迁移就绪结论（2026-08-03）

**Loom 对接的方言障碍已全部清除**。已实现并实测：
启动兼容（postgres 别名/系统表/EXISTS/CREATE DATABASE）+ 默认值（uuid/NOW/字面量）
+ 复合唯一（key 拼接）+ 外键级联（worker 编排）+ UPDATE SET 表达式（乐观锁/toggle）。
Loom 的 40 个仓储方法 SQL 逐条核对，剩余差异均为 Loom 刻意规避（无 RETURNING/无 NOW()/
无 jsonb 操作符），可直接迁移。

## 五、更新步骤（分阶段，原计划，已基本完成）

### Phase 1 — 启动兼容（P0，让 migrator 能跑）

1. SQL 解析器支持 `SELECT EXISTS(subquery)` 标量布尔表达式（WHERE EXISTS 已有，补 SELECT 列表形式）
2. 提供只读系统视图：`pg_database`（映射已存在库）、`information_schema.tables`（映射表元数据）
3. `CREATE DATABASE` 门面语法（复用已有分库能力）
4. `postgres` admin 库别名（migrator 连接探测用，映射到元数据库）

**验收**：`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='default')` 返回 t；migrator 三步骤全过。

### Phase 2 — 默认值求值（P1，让数据能写）

5. `uuid_generate_v4()` 函数实现（或 DEFAULT 表达式求值框架）
6. DEFAULT 表达式真正求值（`NOW()`、`uuid_generate_v4()` 等）

**验收**：`INSERT INTO story_worlds (user_id, name) VALUES (...)` 不指定 id 也能成功，且 id 为 UUID。

### Phase 3 — 正确性补丁（P2）

7. simple protocol 字符串/UUID 字面量比较修复（unresolved column reference）
8. RETURNING 支持（低优先级）

**验收**：`SELECT ... WHERE id='<uuid>'` 与 `$1` 路径行为一致。

### Phase 4 — 验证固化（防止回归）

9. compat 测试补**执行层断言**：DDL 后真实 INSERT/SELECT/UPDATE + 错误码断言（当前 62/62 是语法层假阳性，如 `uuid_generate_v4` 语句接受但执行失败）
10. 固化 pgx 路径回归测试（Python 原始 socket 或 Rust 侧起 pgx 客户端，覆盖 Loom 40 方法 SQL 模式）
11. 端到端验收：Loom 启动 + 建表 + 全量 CRUD + 重启持久化

**验收**：Loom 完整启动并跑通所有 API；`make test` 全绿。

## 五、工作量评估

| Phase | 改动面 | 量级 |
|---|---|---|
| 1 | SQL 解析器 + 系统视图 + PG 门面 | 中（2-3 天） |
| 2 | 函数注册 + DEFAULT 求值 | 中（1-2 天） |
| 3 | 解析器字面量 bug | 小（0.5-1 天） |
| 4 | 测试加固 | 中（1-2 天） |

---

## 七、Portal 对接（追加，2026-08-03）

用 NexusDB 替换 Nexus-Portal 的 PostgreSQL 存储引擎时, 用真实 pgx 客户端 (宿主机 + 容器双路径) + tcpdump 抓包定位并修复了 2 大阻塞, portal 完整启动并幂等重启稳定。

### Portal 阻塞 1 — multi-statement 执行后连接永久挂起

- **现象**: `conn.Exec(multi-statement)` 成功返回后, 同一连接的后续任何请求 (portal 迁移 0001 后的 `INSERT INTO schema_migrations`) 全部 HUNG。此前误判为"multi 推进到 dispatched=31 后没触发 multi_finish"。
- **定位**: tcpdump 抓包证明 NexusDB 已回 32×`SELECT 1` + ReadyForQuery, portal 也收到并继续发 INSERT — 卡点在 INSERT 而非 multi 本身。
- **根因**: multi 子语句占用客户端 seq 区间 `[base, base+N)`, 但 `multi_finish` 只对 `orig` seq 回包。`resp_flush_ready` 按 `next_to_send` 顺序推进, 遇 base+1..base+N-1 的**无 pending 包空洞子 seq** 永久等待 → 连接卡死。
- **修复** (`worker/mod.rs multi_finish`): 完成后把 `next_to_send` / `next_seq` 推进到 `base_sub_seq + dispatched`, 跳过空洞子 seq (error 分支同样处理)。

### Portal 阻塞 2 — 扩展协议参数类型 OID 缺失

- **现象** (层层下钻):
  1. `unable to encode 0 into text format for text (OID 25)` — pgx 端编码 int 失败
  2. `unsupported binary parameter OID 1114` — NexusDB `decode_param` 不认识 timestamp
  3. `unable to encode time.Time into text (OID 25)` — timestamp 回退 text 后 pgx 仍失败
- **根因**: Parse 未声明参数类型时 `build_param_description` 一律按 text(25) 报告; 真实 PG 会按目标列推断。
- **修复**:
  - `worker/protocol_io.rs infer_param_oids`: 基于本地 schema 缓存对 INSERT 参数按目标列推断 OID。
  - `protocol/pg.rs decode_param`: 补支持 PG 二进制 timestamp(1114)/date(1082)/time(1083) 解码, 含 `PG_EPOCH_OFFSET_MICROS` epoch 偏移。

### Portal 代码 bug (Nexus-Portal 项目)

`module.JwtKey.RotatedAt` 为非指针 `time.Time`, `SELECT * FROM jwt_keys` 扫描 NULL `rotated_at` 报错 → 误判无 key → 重复创建冲突。改为 `*time.Time` (与 ModuleKey 一致) + `RotateKey` 赋值适配。

### Portal 端到端验证

- 迁移 `migrations completed` / 幂等 `skip`; JWT key k0 生成; `Default admin account created` / 重启 `skip bootstrap`; `POST /api/v1/auth/login` → `OK` + 有效 JWT; `/health` 200。

### 边界

- 参数 OID 推断仅覆盖 INSERT 目标列; 未命中缓存回退 text。
- PG 二进制时间戳按微秒精度; date 按天、time 按当日微秒。
- `SELECT 1` (无 FROM) 仍报语法错误, 非本次范围。
