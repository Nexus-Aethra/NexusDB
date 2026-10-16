# 修复: internal_update 破坏段首 shared=0 不变量 (P2)

> 日期: 2026-10-17 | 状态: **已完成**

## 问题

`internal_update` 定位 target 时, 若 target 恰好是段首 item (seg_idx > 0,
扫描第一个就命中), `prev_ptr` 仍是 target 自身 → `prev_key == key` →
重编码 `shared = key.len()`, **破坏段首 shared=0 自包含不变量** —
PageIndex::load / InternalItemPtr::new 随后解析该段首即错.

leaf 侧同型 bug 已于 2026-07-26 修复 (leaf.rs:932-936), internal 侧漏修
("修复不对称"); 当前 internal_update 无生产调用点, 一旦启用即触发.

## 修复

对齐 leaf 修法: target 为段首时 prev_key 视为空 (shared=0).

## 测试 (反向验证: 修复前断言 "段首 shared=0 不变量被破坏" 失败)

| 用例 | 结果 |
|---|---|
| `internal_update_segment_head_keeps_shared_zero` | ✅ 40 separator 造多段, 更新段首 child 后段首仍 shared=0, 页自洽 |
| page 全量 | ✅ 151 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: 事务中 DDL 静默回 OK (P2) — 同批次修复

**问题**: 独立 `CREATE INDEX` 在 worker 被吞并返回 OK — 客户端以为索引
已建, 实际查询计划无任何变化 (静默降级).

**修复**: `SqlStmt::CreateIndex` 改为显式报错并指引支持的内联写法
("declare indexes inline: CREATE TABLE t (..., INDEX(col))").
(CreateTable/DropTable/AlterTable 在事务中此前已正确拒绝; DdlStub 为
PG 兼容 deliberately-OK, 保留并在注释中说明.)

**测试**: `create_index_rejected_not_silently_ok` (报错且信息含指引;
内联 INDEX 不受影响); network 串行 223 passed / 0 failed; clippy 0; fmt 0.

---

## 附: 恒假谓词短路返回错误列定义 (P2) — 同批次修复

**问题**: `SELECT a, b FROM t WHERE 1=0` 的恒假短路发生在投影解析**之前**,
硬编码单列 ("", I64) 返回空结果 — 列数/列名与真实投影不符, 破坏客户端
结果集契约 (驱动/ORM 按列名取数直接错位).

**修复**: 短路移到投影解析 (proj/out_names) 与 ORDER BY 列校验之后,
用 `render_sql_rows(proto, bin, &schema, &proj, &out_names, &[])` 渲染
**真实投影形状**的空结果集; COUNT(*) 恒假渲染 0.

**测试**: `always_false_predicate_keeps_real_projection_shape` (捕获列定义
帧断言 [a, b]; 反向验证旧实现返回 [""]); network 串行 224 passed / 0 failed;
clippy 0; fmt 0.

---

## 附: UUID 列 IN >64 元素恒空集 (P2) — 同批次修复

**问题**: `eval_cond_leaf` 的 IN 二分路径 (>64 元素) 对 UUID 列直接拿 36
字符文本与 16B 存储值比较, 漏了 `coerce_cmp_lit_uuid` 的文本→16B 转换 —
同查询 64 元素正确、70 元素恒空, 语义不一致.

**修复**: 二分路径对 UUID 列逐元素 coercion (text→16B) 后重排二分;
含非法 UUID 文本则回退线性路径 (保守正确).

**测试**: `uuid_in_large_set_matches` (70 元素含目标→命中; 不含→空;
≤64 行为不变); 反向验证旧实现返回 []; network 串行 225 passed / 0 failed;
clippy 0; fmt 0. (注: 期间一次失败为 /tmp tmpfs 配额耗尽的环境问题,
清理后复跑通过.)

---

## 附: 系统表数值比较/排序退化为字典序 (P2) — 同批次修复

**问题**: `information_schema` 等系统表虚拟列全为 Str schema —
`WHERE ordinal_position > 2` 这类数值比较走 `sql_cmp(Bytes, Int)` 无匹配
分支 → 条件恒 false, **整个查询返回空集** (比字典序错序更严重);
ORDER BY 数值列也按字典序 ("10" < "2").

**修复**:
- `eval_pred_sysq` 叶子改走 `eval_cond_leaf_sysq`: 数值字面量 vs 可解析
  为数值的文本列值 → 按数值比较, 否则回退通用路径;
- 系统表 ORDER BY 改 `sysq_cmp`: 双方可解析为数值时按数值排序.

**测试**: `information_schema_numeric_compare_and_order` (12 列表,
`ordinal_position > 2` → c01..c11 共 11 行; DESC 数值序); 反向验证旧实现
返回空集 []; network 串行 226 passed / 0 failed; clippy 0; fmt 0.

---

## 附: AVG(DECIMAL) 精度回退 f64 (P2) — 同批次修复

**问题**: AVG 累加器全走 f64 — DECIMAL 列先转 f64 再累加, 金额均值出现
浮点误差 (0.10/0.20/0.30 的均值渲染成 0.20000000000000004).

**修复**: 新增 `AvgDec { acc: i128, scale, n }` 定标累加器:
- DECIMAL 源列 → 定标整数累加 (checked, 混合 scale 显式报错);
- finish: `(acc × 10^extra) / n` 整数除法, 输出 scale+extra (≤6 位附加
  精度, scale 38 封顶) 的 Decimal; 溢出兜底 f64;
- AVG 输出列类型对 Decimal 源声明为 Decimal{scale+6}; 其余源列维持 F64;
- scale 由首个入值推定 ( Accum::new 收到的 col_ty 对 AVG 是 out_ty,
  其 scale 不可信).

**测试**: `avg_decimal_exact` (0.20 精确 8 位; 0.15 除不尽; SUM 不受影响;
INT 列 AVG 维持 F64); 反向验证旧实现 0.20000000000000004;
network 串行 227 passed / 0 failed; clippy 0; fmt 0.

---

## 附: WAL strict barrier 失败仍回成功 (P2) — 同批次修复

**问题**: strict 模式 (crash 零丢失) 下, `wal_barrier()` 失败被三处 swallow:
Batch 臂 (仅记日志后发 `Ok(BatchResults)`)、group-commit held 路径 (held
回复照发成功)、同步慢路径 Put/Delete (`let _ =` 吞掉后发 Ok) —
客户端以为写已持久化, 实际 fsync 失败, 违背 strict 语义.

**修复**: 三处 barrier 失败均转为错误回复 —
Batch 臂: `Err(ShardErrorKind::StorageError("WAL persist failed: ..."))`
(同步 reply 与 reply_bus sink); group-commit: held 的 `TaskResult.result`
改 `BatchResult::Error(...)`; 同步 Put/Delete: 匹配 `(r, wal_err)` 优先
回 barrier 错误.

**测试**: shard_manager 串行 65 passed / 0 failed; clippy 0; fmt 0.
(barrier 失败需 IO 故障注入, 仓库无该设施, 按构造验证.)

---

## 附: pid page_idx u16→u8 截断 (P2) — 同批次修复

**问题**: `PidLocation::page_idx()` 是 u16, 但 chunk 只有 64 个页.
pager/pager_write 的 3 处消费点直接 `as u8`: 损坏/陈旧 pid 若带
page_idx >= 64, 截断后寻址到**错误页** (静默数据损坏), 且截断值 >= 64
时 `chunk_bytes[off..off+PAGE_SIZE]` 切片越界 panic.

**修复**: `types::pid_page_idx_checked()` (page_idx < PAGES_PER_CHUNK
才转 u8); pager 两条读路径 + pager_write 写路径改用它, 损坏 pid →
`InvalidData` 显式错误. recover.rs 的 pid.state 水位守卫从
`<= u8::MAX` 收紧为 `< PAGES_PER_CHUNK`. (meta_cache 加载时净化的初版
方案已回退 — 测试将 page_idx 用作不透明 u16 负载, 该层不应假设地址语义.)

**测试**: `pid_page_idx_checked_bounds` (0..63 合法; 64/200/255/256/1000
拒绝); storage 串行 508 passed / 0 failed; clippy 0; fmt 0.

---

## 附: recover 固定共享临时文件 /tmp/nexus_recover_empty.mate (P2) — 同批次修复

**问题**: `recover_for_shard` 在 `page.mate` 缺失时用**固定共享路径**
`/tmp/nexus_recover_empty.mate` 兜底并直接返回空状态, 两个后果:

1. **数据损坏 (严重)**: `.block` 有数据但 `page.mate` 丢失 → 扫描被整体跳过,
   存量页全部变孤儿; 且 `next_vpid/next_file_id/pid_alloc` 全部归零 →
   重启后新写入从 `000001.block` page 1 开始, **覆写 .block 里的活页**.
2. **多实例互踩**: 多进程/多测试 recover 全新库时打开同一 /tmp 文件 —
   同步 `flush_dirty` 互相覆写; 且同步刷盘落 `/tmp` 而异步刷盘落
   `block_dir/page.mate` (pager_backend `take_meta_flush_batch`), 两条路径
   分家 → 重启后元数据丢失/不一致; 文件还永久残留在 /tmp.

**修复**: 不再走临时文件分支 — 直接在库目录 `MetaCache::open(page.mate)`
(自带 `create(true)`), 元数据落点与异步刷盘一致, mate 缺失时扫描照常执行
(从 page header 重建映射); 仅当**库目录本身不存在** (调用方未
`create_if_missing` 的全新路径, 无 `.block` 可扫) 才回退到
`open_ephemeral_mate()`: 名字带 pid+纳秒时间戳 (多实例不撞名), 打开后立即
unlink (仅 fd 存活 → 无共享路径、无残留、退出即回收).

**测试** (反向验证: `git stash` 掉 recover.rs 后两条均失败):

| 用例 | 结果 |
|---|---|
| `recover_scans_blocks_when_mate_missing` | ✅ 有 .block 无 page.mate → 5 个 vpid 全部扫回, pid 水位 (0,0,5) 不回退 |
| `recover_mate_backend_is_db_local_not_shared_tmp` | ✅ 全新 shard 目录: flush 落库目录 page.mate 且重启读回; 固定共享 /tmp 文件内容不被写/不被截断; 匿名 mate unlink 无残留 |
| storage 全量串行 | ✅ 510 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: leaf_get_with 1000 步假 miss (P2) — 同批次修复

**问题**: `leaf_get_with` (`leaf.rs`) 段内线性扫描用**固定 1000 步上限**,
`loop_count > 1000` 即 `return None` — 单段 item 数超 1000 (bulk split /
索引重建可产出的大段) 时, 对**真实存在的 key** 也返回 None:
静默丢数据 (点查 miss → 上层按"键不存在"处理).

**修复**: 迭代次数改按段自身的 `item_count` 界定 (与 `leaf_scan_from` 同源
语义, 哨兵占一个名额), 删掉魔法数 1000; 段内扫尽即返回 None, 不再跨段乱闯.
`item_count` 本身不可信 (损坏页) 时由 `ptr.next()` 的解码失败/边界检查兜底
终止, 不会死循环.

**测试** (反向验证: stash 掉 leaf.rs 后 `k01000` 起断言失败, left=None):

| 用例 | 结果 |
|---|---|
| `leaf_get_with_scans_segment_larger_than_1000_items` | ✅ 1200 key 合并成单段 (item_count=1201), k00000/k00999/k01000/k01001/k01199 全部命中, 不存在 key 仍 None, scan_from 覆盖 200 项 |
| page 全量串行 | ✅ 152 passed / 0 failed |
| storage 全量串行 (leaf_get_with 热路径消费方) | ✅ 510 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: IN 子查询跨类型折叠恒 false (静默空集) (P2) — 同批次修复

**问题**: IN 集合元素与列类型跨型时, `eval_cond_leaf` 线性路径对每个元素调
`sql_cmp`, 跨型返回 `None` 按 false → **静默空集**. 例:
`SELECT id FROM u WHERE id IN (SELECT uid FROM o)` (id=bigint, uid=text)
返回 0 行且无任何提示 — 客户端无法区分"类型不匹配"与"确实没有匹配行".
IN 子查询折叠产物 (`fold_one_subq`) 与字面量 IN 列表同一 Cond 结构, 两条
路径都中招; SELECT/DELETE/UPDATE 都受影响.

**修复**: 计划期显式报错 (PG 兼容语义 "IN types ... cannot be matched"):
新增 `check_in_set_types(schema, pred)` — 逐元素 `sql_to_col(col_ty, v)`,
失败即 Err; 挂在 `sql_plan_select` (覆盖全部 SELECT 计划路径) 与
`sql_run_dml` 入口 (覆盖 Delete/Update). 可 coercion 的文本字面量
(`id IN ('1','3')`) 与同型集合不受影响.

**测试** (反向验证: stash 掉 worker 两文件后 `跨型 IN 子查询应报错, 实际 Rows([])` 失败):

| 用例 | 结果 |
|---|---|
| `mysql_in_type_mismatch_errors_not_silent_empty` | ✅ 同型 IN 子查询/可 coercion 文本仍正常; 跨型 IN 子查询、跨型字面量列表、DELETE 跨型 IN 均报 "IN types cannot be matched"; 报错后数据未被误删 |
| network 全量串行 | ✅ 228 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: prepared 语句注册表无上限 (P2) — 同批次修复

**问题**: `conn.mysql_stmts` (MySQL `COM_STMT_PREPARE`) 与 `conn.pg_stmts`
(PG `Parse` 命名语句) 均**无上限** — 单连接可无限注册, 注册表无界增长 →
worker OOM. worker 是单线程事件循环, 一个连接的内存泄漏会拖死同 worker
上的**全部**连接 (等效拒绝服务).

**修复**: `KvLimits` 新增 `max_prepared_stmts` (默认 1024; 对 ORM 通常个
位数~数十条预备语句足够宽裕), 随 `WorkerConfig.limits` 穿到
`process_sql_input` / `process_pg_input`:
- MySQL: 超限回 ERR **1461** (MySQL max_prepared_stmt_count 同义错误码);
- PG: 超限走 batch error → ErrorResponse;
- `pg_pending_prepares` (等 schema 的挂起项) 一并计入, 堵住旁路;
- CLOSE / Close 语句即时释放名额 (语义不变).

**测试** (反向验证: 中和两处上限判断后, 第 5 条 prepare 返回 Ok 而非 1461 → 断言失败):

| 用例 | 结果 |
|---|---|
| `mysql_prepared_stmt_registry_capped` | ✅ 上限内正常注册; 第 5 条 ERR 1461; CLOSE 后可继续 prepare 且能执行 |
| `pg_named_statement_registry_capped` | ✅ 上限内 ParseComplete; 超限 ErrorResponse "too many prepared statements"; Close 后可继续 Parse+Execute |
| network 全量串行 | ✅ 230 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: 事务读集仅覆盖 pk 点查 (P2) — 同批次修复

**问题**: SERIALIZABLE 的 OCC 读集**只有 pk 点查**登记
(`SqlRowCtx.read_key` / `sql_read_key`) — 范围扫描 / 全表扫 / 索引扫 / 聚合
的读完全不进 `read_set` → 提交验证看不见它们: 并发事务改/删本事务已读的行
不被发现 (丢失更新 / 不可重复读). 复现: 事务 A 全表扫读 5 行 → 事务 B 改
其中一行并提交 → A 只写另一行 → A 的 COMMIT **成功** (旧值写覆盖).

**修复** (worker 侧, 行级 OCC 语义不变):
1. `SqlSelectAgg` 新增 `txn_read: Option<(db, table)>` — 仅 SERIALIZABLE
   事务内的语句才填 (RC/非事务零开销); 7 处构造点统一经
   `txn_read_ctx(conn, db, &table)`.
2. 回包到达即逐行登记 `(pk, crc32(row))` 进 `conn.txn.read_set`
   (`or_insert` 首读指纹为准, 与点查一致); **只登记通过残余过滤的行**
   (索引下推的界是超集, 未命中的候选行不算"读过", 登记只会制造假冲突);
   解码失败保守登记.
3. 覆盖索引 (`cover`, 免回表) 与投影下推 (`down_proj`) 在事务内**禁用** —
   两者都只回投影列值, 没有 row 字节, 读集无从登记指纹 (正确性优先).
4. 主键区间 SELECT 在事务内改走 `ScanFilteredRows` (回包带 pk + row 字节),
   与原 DML phase1 同款 (ProjRows 路径无 pk).
5. 新增护栏 `TXN_MAX_READ_SET` (2^18 行): 扫描读登记后读集随扫描行数增长,
   超限置 `read_set_overflow` → COMMIT 显式报错 (宁可拒事务也不放行一个
   验证不全的事务), 防巨型扫描把 worker 内存吃穿.

**遗留边界 (文档化, 非本轮范围)**: ① 幻读 (并发**插入**落进已扫区间) 仍不
防 — 行级 OCC 无谓词/范围指纹, 与 PG SSI 有差距 (AGENTS.md/CHANGELOG 原有
声明, 本轮把"扫描读"补齐后剩下的就是这一条); ② JOIN 的 gather 走
`ScanFiltered` (ProjRows, 无 pk/row) 仍未登记; ③ 跨 shard COMMIT 仍是
best-effort (某 shard 验证失败时其余 shard 可能已应用 — 原有边界).

**测试** (反向验证: stash 掉 6 个 worker 源文件后, 两用例均 "事务成功提交"
→ 断言失败):

| 用例 | 结果 |
|---|---|
| `mysql_serializable_scan_reads_enter_read_set` | ✅ 范围扫/全表扫读过的行被并发改 → COMMIT 1213; 冲突未应用; 无并发修改不误报 |
| `pg_serializable_scan_reads_enter_read_set` | ✅ 同上 (PG 门面, 40001 + ReadyForQuery=I) |
| network 全量串行 | ✅ 232 passed / 0 failed |
| storage + page 全量串行 | ✅ 662 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: SELECT 结果集无内存护栏 (P2) — 同批次修复

**问题**: `materialize_select_agg` 把全部命中行堆进 `out_rows` 再排序/截断 —
无 LIMIT 的大表 ORDER BY / 大结果集渲染会把 worker 内存吃穿 (OOM). 对比
JOIN 路径早有 `JOIN_MAX_ROWS = 262_144` 护栏, SELECT 侧一直没有.
附带: COUNT(*) 也走同一条物化路径 (堆全部行只为了取 len), 大表 COUNT 白白
吃内存.

**修复**:
1. 新增 `SELECT_MAX_ROWS = 262_144` (与 JOIN_MAX_ROWS 对齐): 物化行数超限 →
   返回错误 "result set too large (limit N rows); add LIMIT or narrow WHERE"
   (客户端可加 LIMIT / 收窄 WHERE 重试); 护栏在聚合分支**之前**, GROUP BY
   的输入同样受保护.
2. COUNT(*) 改计数不物化 (`count_only` 快路径) — 大表 COUNT 内存恒定,
   且不再受护栏限制 (计数不需要物化).

**测试** (反向验证: 中和护栏判断后 `caps_result_set` 失败 — 262152 行全部
物化返回 Ok):

| 用例 | 结果 |
|---|---|
| `materialize_select_agg_caps_result_set` | ✅ 262152 行无 LIMIT → Err "result set too large" |
| `materialize_select_agg_count_does_not_materialize` | ✅ COUNT(*) 同样行数 → Ok 且计数正确 (不物化) |
| `materialize_select_agg_limit_under_cap_ok` | ✅ LIMIT 10 早停 → 只回 10 行, 不触发护栏 |
| network 全量串行 | ✅ 235 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: JOIN cross 中间积无逐行检查 (P2) — 同批次修复

**问题**: JOIN 累加器的 `JOIN_MAX_ROWS` 护栏只在**整块构建后**检查
(`if new_acc.len() > JOIN_MAX_ROWS`) — 笛卡尔积 / 同键多匹配的中间积在检查
之前就已全额物化: 两侧各 262144 行 (gather 侧各自合规) 时中间积可达 68B 行,
worker 先 OOM 才走到检查.

**修复**: 3 处内层 push 点改为**边构建边查** (驱动交换路径 / Cross 分支 /
等值匹配分支), 内存上界 = 护栏本身; 原有构建后检查保留为兜底. 错误消息统一
为 `JOIN_OVER_CAP_MSG` (文本不变, 客户端无感知).

**测试** (反向验证: 把 `JOIN_MAX_ROWS` 临时改成 `usize::MAX` 后用例失败 —
"笛卡尔积超限应报错, 实际返回 360000 行"):

| 用例 | 结果 |
|---|---|
| `mysql_join_cross_product_capped` | ✅ 600×600 = 360000 > 262144 → 报 "row cap exceeded"; 服务端后续查询仍健康 |
| network 全量串行 | ✅ 236 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

注: 本项缺陷的**可观测行为不变** (超限两种情况都报错), 差异在峰值内存 —
逐行护栏把中间积的内存上界从"乘积全额"压到"护栏值"; e2e 用例锁定护栏行为
(去掉护栏即失败), 逐行生效点由代码位置保证.

---

## 附: SqlSharedRoutes 锁 expect 中毒即 panic (P2) — 同批次修复

**问题**: `SqlSharedRoutes` (进程级单例, 跨 worker 跨 SQL 门面共用) 的 4 把
`RwLock` 全部用 `.expect("... lock")` 取值. 任意持锁 panic 把锁毒化后, 该
单例的**每一次**访问都 panic — 一个 panic 级联杀死全部 SQL 门面 worker, 也
让此前加的语句级 panic 隔离 (`guard_stmt_panic`) 失去意义.

**修复**: 新增毒容错的 `rlock`/`wlock` (`unwrap_or_else(PoisonError::into_inner)`),
替换全部 13 处访问点 (sql_routes.rs 自身 + resp_shard.rs / protocol_io.rs /
mod.rs). 这些锁保护的都是缓存/注册表 (routes bloom / created_here /
fk_incoming / cluster_ctl), 毒化后数据最多停在一次未完成的结构更新上,
后续写入自然修正 — 优于"整个进程陪葬".

**测试** (反向验证: 把 `rlock` 换回 `expect` 后用例失败 — 毒化锁上 panic):

| 用例 | 结果 |
|---|---|
| `poisoned_lock_access_does_not_panic` | ✅ 持写锁 panic 制造毒化 → `cluster_ctl()`/`unregister_fks()`/`incoming_fks()` 均不 panic |
| network 全量串行 | ✅ 237 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: PageIndex::load 前 4 段全量 decode 性能 (P2) — 同批次修复

**问题**: `PageIndex::load` 里有一段**纯诊断**代码 (`if i < 4`): 对前 4 个
cp 段逐 item `decode_item` + `full_key` 只为喂 `dprintln!`. 因 `full_key` 带
Err 语义 (不可 DCE), release 也全额执行 — 实测 **1392ns/次**, 而 load 是
每次读页的热路径.

**修复**: 该循环收敛到 `cfg!(debug_assertions)` — debug/测试保留完整校验,
release 编译期消除.

**测试**: `page_index_load_rejects_corrupt_item_in_first_segments`
(`#[cfg(debug_assertions)]` — 段内第 2 个 item 的 shared 改成 9999, load 必须
Err, 锁定"收敛不等于丢校验"); 性能探针 (release, 同页同调用次数):
**1392ns → 210ns/op (6.6×)**; page 全量 152 passed / 0 failed; clippy 0; fmt 0.

---

## 附: leaf_delete 冗余 O(N) 全页扫描 (P2) — 同批次修复

**问题**: `leaf_delete` 每次删除都调 `reconstruct_key_before` (从页头顺序
decode 到 target = O(N) 全页扫描) 算 `target_prev_key`, 而该值**只喂给
`dprintln!`** — release 里纯白做.

**修复**: 与 `dprintln!` 同条件惰性化 (`debug::debug_on("leaf")`, 新增 helper;
`DEBUG_PAGE` 是 const false → 编译期折叠, 零成本).

**测试**: 性能探针 (release, 100 insert + 1 delete/轮): **174642ns →
103603ns/op (1.69×)**; page 全量 152 passed / 0 failed (删除语义不变);
clippy 0; fmt 0.

---

## 附: ptr next 双重 decode (P2) — 同批次修复

**问题**: `LeafItemPtr::next` / `InternalItemPtr::next` 先调
`inner.next()` (decode 一次), 再 `decode_item` 解第二遍拿 value_len /
child_vpid — 段内每次前进都双倍解码.

**修复**: 新增 `ItemPtr::next_decoded(kind)` 一次解码同时返回新游标和解出的
`Item` (零拷贝借用 page); `next()` 复用它 (丢弃 Item), 两个子类型的
`next()` 改走单次解码路径.

**测试**: page 全量 152 passed / 0 failed (游标语义/取值不变);
clippy 0; fmt 0.

---

## 附: scanner --strict/--tolerant 均为无效 flag (P2) — 同批次修复

**问题**: `--strict` / `--tolerant` 解析后**从不读取** — `Globals::is_strict()`
零调用方, `page_io::read_page_strict` 零调用方. 设计文档 §3.1 承诺的
"strict: abort on first bad page (CI only) / tolerant (默认): never exit
non-zero on bad pages" 完全没落地: 两种模式行为一模一样 (永远 tolerant).

**修复**:
1. `ScannerError::BadPage(String)` 新变体; `Globals::strict_gate(what)` 闸门
   (strict → Err → main 退出码 2; tolerant → Ok).
2. `WalkSummary` 新增 `strict_hit: Option<String>`; `walk_tree` /
   `walk_tree_in_order` / `do_in_order` 新增 `strict` 参数 — strict 下撞到
   第一个坏页/不可读页/损坏 item 即记录并**中止遍历** (不再扫完整个目录).
3. 命令层接线: `verify` / `export` / `rescue` / `merge` (走 walk, 传
   `globals.is_strict()`, 结束按 `strict_hit` 返回 Err)、`export_force`
   (`stats.bad > 0` 时闸门)、`vpid` / `header` (单页命令: 坏页/不可读即闸门).
4. `range` / `lookup` 本来就对坏页返回 `Ok(1)` (非零退出), 无需改动 —
   但它们这样做意味着 tolerant 模式下也会非零退出, 与设计文档 §3.1 的
   "tolerant 永不非零" 有出入 (记录为遗留边界, 不在本项范围).

**测试** (反向验证: strict 参数恒 false 时 CLI 退出码回到 0):

| 用例 | 结果 |
|---|---|
| `strict_gate_only_fires_in_strict_mode` (cli) | ✅ tolerant → Ok; strict → Err 且带坏页详情 |
| `walk_strict_mode_halts_on_first_bad_page` (tree) | ✅ tolerant: `strict_hit=None` 只记数; strict: 记录 `vpid 1: ...` |
| 真机 CLI (合成坏页目录) | ✅ `--strict vpid 1` / `--strict verify 1` → 退出码 **2** + `fatal: bad page in strict mode: ...`; 不带 `--strict` → 退出码 0 且仍打印 `[BAD-PAGE]` 行 |
| scanner 单测 | ✅ 70 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: DefaultHasher 路由逻辑三份拷贝 (P2) — 同批次修复

**问题**: 同一套 (db, table, key) → shard 路由哈希在三处复制:
`shard_manager/src/router.rs:40` (`HashRouter::route`)、
`manager.rs:1382` (`route_db_table_key`)、
`network/src/worker/resp_dispatch.rs:65` (`hash_route_key`, 被 `hash_route_op`
复用). 任一处漂移 (输入顺序 / hasher / 取模) 都会让**同一个 key 在不同路径
落到不同 shard** — 例如 worker 把写发去 shard A, 而 OCC 读集验证 / 索引查询
去 shard B, 表现为"写成功却查不到"这类极难排查的数据不一致. 另外
`DefaultHasher` 的跨版本稳定性从未写明 (标准库不保证).

**修复**: 收敛为唯一实现 `shard_manager::router::route_hash(db, table, key,
num_shards)`, 三处全部委托; `lib.rs` 导出. 同时在文档里写明稳定性契约:
DefaultHasher 输出**不跨 Rust 版本稳定**, 故只用于同进程内分片选择 (数据布局
不跨进程/跨版本持久化; 若未来要跨版本稳定必须换固定算法并重分布数据).

**测试**:

| 用例 | 结果 |
|---|---|
| `route_hash_is_the_single_source_of_truth` (shard_manager) | ✅ trait 入口与自由函数同值; 输入顺序敏感; num_shards 敏感 |
| `hash_route_key_matches_shard_manager_single_source` (network 单测) | ✅ worker 侧路由与规范实现逐位相同 (4 组输入 × 4 种 num_shards) |
| shard_manager 全量串行 / network 全量串行 | ✅ 65 / 238 passed, 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: 无连接数上限 / 无 idle 超时 (P2) — 同批次修复

**问题**: acceptor/worker 对连接数**无上限**, 也无 idle 回收 — 连接可无限
堆积 (fd + `ConnState` 内存耗尽 = 拒绝服务), 客户端连上不关 / 半死连接会
**永久**占用 worker 槽位. 顺带: worker 里的 `let _limits = cfg.limits` 把整个
limits 配置丢掉 (连接相关字段从未被读取).

**修复** (kv 放进已有的 `KvLimits`, 零新增构造点):
1. `KvLimits` 新增 `max_connections_per_worker` (默认 4096; 全局上限 ≈
   本值 × worker_count) 与 `idle_timeout_secs` (默认 300; 0 = 关闭).
2. `ConnState::last_activity` (Instant) — `recv`/`recv_async` 读到数据即刷新
   (TLS 与明文两条路径都刷).
3. epoll worker: `reject_over_cap` (超限直接 `close(fd)`, 不建状态) 接入两条
   新连接路径 (循环顶部 drain + NEW_CONN_TOKEN 分支); `sweep_idle_conns`
   按 1s 节流在主循环扫描回收 (主循环本就 ≤100ms 一轮, 零额外唤醒成本).
4. coro worker (`NEXUS_CORO_WORKER=1`, 默认关闭): 同样接入连接数上限
   (复用已有的 `active` 原子计数). **idle 回收未覆盖** — 协程 worker 阻塞在
   `poll(conn_eventfd)` 上无周期 tick, 且 scheduler 无定时唤醒原语;
   记录为遗留边界 (默认路径不受影响).

**测试** (反向验证: 中和 `reject_over_cap` / `sweep_idle_conns` 后两用例均失败):

| 用例 | 结果 |
|---|---|
| `mysql_connection_cap_rejects_excess` | ✅ cap=2 时第 3 个连接被立即关闭 (读到 EOF); 已有连接仍可用; 释放槽位后可再连 |
| `mysql_idle_connection_reaped` | ✅ idle_timeout=1s, 空闲 2.5s 后服务端关闭连接 (读到 EOF; 超时算失败) |
| network 全量串行 | ✅ 240 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |

---

## 附: 认证无失败限速 (P2) — 同批次修复

**问题**: 认证失败**无限次** — MySQL/PG 每次失败关连接 (一次 TCP 连接只能猜
一次), 但攻击者可以全速重连重试; RESP (AUTH) 失败只回错误、**不关连接**,
一次连接即可无限猜. 等同于无速率限制的暴力破解.

**修复** (新增 `worker/auth_throttle.rs`):
- `AuthThrottle` 实例挂在进程级共享的 `SqlSharedRoutes` 上 (同一数据集群的
  全部门面/worker 共用一把闸; 不同集群互相独立 → 测试各起一个不会连锁锁定);
- 滑动窗口 + 锁定: `AUTH_WINDOW_SECS`(60s) 内失败达 `AUTH_MAX_FAILS`(10) 次
  → 锁定 `AUTH_LOCKOUT_SECS`(30s); 锁定期间任何认证请求**直接拒** (连口令都
  不比对), 错误信息带剩余秒数; 认证成功清零并解除锁定;
- `ConnState.auth_failures` + `MAX_FAILS_PER_CONN`(5): 单连接连续失败达上限
  即断开 (堵死"一次连接无限猜");
- 三门面接线: RESP `AUTH` (锁定期检查 + 失败计数 + 单连接断开)、MySQL
  handshake/AuthSwitch (锁定期检查 + 失败/成功计数)、PG startup/SASL
  (锁定期检查 + 失败/成功计数).

**遗留边界**: 粗粒度 (集群级, 不按来源 IP 细分); 按 IP 的精细限流需要
acceptor 侧连接来源状态, 留作后续项.

**测试** (反向验证: 移除 `record_failure` 调用后锁定期不出现 → 用例失败):

| 用例 | 结果 |
|---|---|
| `failures_then_lockout_then_success_resets` | ✅ 达阈值锁定、锁定期间拒、成功后解锁 |
| `below_threshold_not_locked` | ✅ 未达阈值不锁定 |
| `resp_auth_failures_are_rate_limited` | ✅ 单连接连续失败被断开; 两连接累计达阈值后新连接直接收 "too many authentication failures" (正确口令同样被拒) |
| network 全量串行 | ✅ 244 passed / 0 failed |
| clippy -D warnings / fmt | ✅ 0 / 0 |
