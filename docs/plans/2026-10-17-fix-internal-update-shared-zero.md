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
