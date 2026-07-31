# NexusDB — Changelog & Hindsight

> 详细修复历史 + 测试进度快照 + 环境 gotchas + 测试文件清单.
> 本文件由 `AGENTS.md` 拆分而来 (2026-07-20), AGENTS.md 只保留项目入口与设计原则摘要.
>
> 完整测试状态快照历史索引: 7-24 / 7-20 / 7-19 三个旧快照完整保留于 `git log CHANGELOG.md`
> 任意历史版本; 与本快照差异仅在测试计数 (随会话累积), 测试文件清单同步见代码目录.

**逆序时间线 (最新在上).**

---

## 2026-07-31 会话十五 (F73 大 IN / F74 关联 EXISTS 去相关 / F75 派生表参与 JOIN)

子查询后续三件套, 收尾 Phase 3 已知遗留。三项独立交付, 统一跨协议对拍。

### F73: 大 IN 上限提升 (>1024 不再报错)

- 阈值区分叶子类型: EXISTS 无上限 (仅存在性); scalar 保持 >1 行报错; IN 去重后上限 `SUBQ_IN_MAX=65536` (捕获阶段 OOM 护栏同值)
- IN 集合排序去重 (`sql::sort_in_set`, 同型: 全 Int 按值 / 全 Str 按字节): 解析期字面量 IN + fold_one_subq 折叠集合均排序
- `eval_cond_leaf` IN 分支: 集合 >64 且同型匹配列型 (I64+全 Int / Bytes+全 Str) → `binary_search`; 混型/跨型 coercion 回退线性
- 实机: 2000 行内层 IN (旧报错语句) 现出正确结果; 边界抽查 + NOT IN 补集正确

### F74: 关联 EXISTS/NOT EXISTS 单等值去相关

- `EXISTS (SELECT 1 FROM o WHERE o.uid=u.id [AND 非相关])` ≡ `u.id IN (SELECT uid FROM o WHERE 非相关)` — 解析期收 `SqlValue::ColRef`, 编排前 AST 改写为非关联 IN, 执行层零新机制
- `decorrelate_stmt/pred/leaf/exists` (worker): EXISTS 叶内层含 ColRef → 去相关 (内层 AND-only, 恰一条相关等值 Eq, 两侧一外一内, 其余无 ColRef); NOT EXISTS 在 Pred::Not 内自然成 NOT IN
- 不可去相关 (关联 IN/标量、多重相关、非等值、OR 内) → 清晰报错 "correlated subquery not supported (v1, only single-equality EXISTS)"
- 实机: 关联 EXISTS/NOT EXISTS (附加内层条件/两侧顺序/裸列与限定列) 结果对拍手工 JOIN; 各不可去相关形态报错

### F75: FROM 派生表参与 JOIN

- `SqlStmt::SelectJoin` 加 `from_inner: Option<Box<SqlStmt>>` (Some=首表派生表, from.table=别名); parse_join 拆 wrapper + `parse_join_from` (接受已解析 from + from_inner)
- worker `DerivedCtx` → enum {Standalone, JoinFrom{db, join_stmt}}; 内层复用完成点拦截物化, `finish_derived_join` 合成 schema (真实列型) + 预填 `SqlJoinCtx.tables[0]` (proj=全列 identity, prefilled=true), 转 sql_join_kickoff
- `JoinTable.prefilled`: kickoff 跳过其 FetchSchema/Gather, 不清空其 rows, 从首个非预填表开始 gather; 左深 hash join/外连接逻辑零感知
- 实机: 派生表 INNER/LEFT JOIN、混合投影+WHERE 限定列、内层聚合 GROUP BY 作首表 (group 列作 ON 键); JOIN 右侧派生表报错 (v1)

### gotcha

- **is_join_ahead 跨 RParen 误判**: `(SELECT .. FROM u) t LEFT JOIN` 中内层 `SELECT .. FROM u` 的 is_join_ahead 会在 3-token 窗口内看到 `)` 后的 LEFT 而误判为内层 JOIN → 内层 parse_join 提前 done() 撞剩余 token. 修复: is_join_ahead 遇 RParen 即停 (子查询边界不跨)
- F73 二分求值前提是集合已排序去重, 故 sort_in_set 必须在解析期字面量 IN 与折叠集合两处都调; 混型集合不排序 → eval 保守回退线性
- F74 ColRef 与 Param/Subquery 同 "执行前必解" 家族: sql_to_col 防御报 "unresolved column reference"; 列-列比较 (非关联) 也走此拒绝路径
- F75 prefilled 表 proj 必须强制全列 identity (物化行定宽), 否则 plan 算的子集 proj 与实际行宽错位

---

## 2026-07-31 会话十四 (F72: FROM 派生表 + Phase 3 收尾)

Phase 3 收尾: FROM 派生表 `SELECT ... FROM (SELECT ...) t` + F71 补 pg 驱动跨协议验收。
方案 = **零 TableSource ripple**: 不动 `Select.table: String`, 学 F67 SelectJoin 隔离先例用
独立 `SqlStmt::SelectDerived` 变体; 内层复用 F71 完成点拦截物化 (列定义+行集),
外层在 worker 内存过滤/投影/排序/截断 (sysq_finish 同款管线, 但保留内层真实列类型)。

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| D1 | `SqlStmt::SelectDerived{inner,alias,items,conds,order,limit,offset}` + parse_derived (FROM 处 peek_paren_select 接入; 必带别名; 孤 COUNT(*) 特例外拒聚合投影; 外层 WHERE 拒子查询) + bind_params 臂 | protocol/sql.rs |
| D2 | `DerivedCtx` + `sql_derived: HashMap<seq,ctx>` + dispatch 臂 (内层验证后同 seq 重入 sql_dispatch_stmt) + 两完成点拦截 (SqlSelectAgg → `Fire::DerivedDone(MatResult)`; SqlRowCtx → `derived_capture_rowctx`) + `finish_derived`/`derived_render` 内存管线 | worker.rs |
| D3 | e2e mysql_derived_tables (13 断言) + **mysql-connector + psycopg3 跨协议对拍 30/30** (F71 全部用例 pg 欠账补上) + 回归全绿 + clippy 0 | tests/sql_e2e.rs |

### 支持能力 (实机验收)

- 内层 = 任意单表 SELECT (含聚合/GROUP BY/ORDER/LIMIT); 输出列名 = 投影列名或聚合 label (`g`/`SUM(v)`)
- 外层: WHERE (`t.x`/裸 `x`, 含 OR/NOT 谓词树) + 投影 + ORDER (多键/DESC) + LIMIT/OFFSET + 孤 `COUNT(*)`
- 内层 pk 点查形态 (SqlRowCtx 路径) / 索引路径 / 全扫 / 聚合路径均可作内层
- 错误面: 无别名 / 未知列 / 错 qualifier / 内层 JOIN / 内层嵌套子查询 / 非孤 COUNT(*) 聚合投影 → 清晰报错

### 边界 / 已知限制 (v1 文档化)

- 派生表仅作唯一数据源 (不参与 JOIN); 外层无 GROUP BY/HAVING/聚合投影 (孤 COUNT(*) 特判除外); 外层 WHERE 无嵌套子查询
- 物化在 worker 内存 (JOIN_MAX_ROWS=262144 上限, 无流式); 外层谓词不下推 shard (数据已在内存)
- 外层 ORDER BY 聚合 label 需裸写 (`ORDER BY g` 可; `ORDER BY SUM(v)` 语法层不收, 同单表限制)
- 事务内 RYOW 直渲染路径不经拦截 (与 F71 同源已知边界, 低频组合)

### gotcha

- **拦截优先级**: SqlSelectAgg 完成点 `None if is_derived => Fire::DerivedDone(materialize_select_agg(agg))` 携带 MatResult 整体 (含 Err) — 错误在 fire 处统一清理 sql_derived ctx, 避免借用冲突与 ctx 泄漏
- **SqlRowCtx 拦截需自合成列定义** (COUNT → 单列 I64; 否则 proj 列 name+ty) — 该路径无 materialize 可用
- **parse_join 内层自然拒绝**: `(SELECT .. JOIN ..)` 在 parse_join 末尾 `p.done()` 撞 `)` 报 trailing tokens, 无需额外拦截
- pkill 自身包装进程: 沙箱 bwrap 命令行含模式串, `pkill -f` 会自杀; 用 `pkill -x NexusDB`

---

## 2026-07-31 会话十三 (F71: 非关联 WHERE 子查询)

Phase 3 第一部分: 非关联 WHERE 子查询 (IN/NOT IN + 标量 + EXISTS/NOT EXISTS)。
方案 = **内层先跑完 → 折叠成字面量/恒真恒假 → 外层走完全现有 WHERE/plan/eval/shard 路径**。

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| S1 | `SqlValue::Subquery(Box<SqlStmt>)` (与 Param 同构的“执行前必解”占位) + parse (IN/NOT IN/scalar/EXISTS, parse_select top 参 + parse_paren_subselect) | protocol/sql.rs |
| S2 | bind_params 子查询递归绑定 + sql_to_col 防御拒未折叠 | protocol/sql.rs, worker.rs |
| S3 | SubqCtx 顺序编排状态机 + materialize 拆分 (render_select_agg/render_agg_groups) + 折叠重跑; 拦截 SqlSelectAgg 与 SqlRowCtx 两完成路径 | worker.rs |
| S5 | e2e mysql_where_subqueries + mysql 驱动实机 + 回归 | tests/sql_e2e.rs |

### 支持能力 (实机验收)

- **IN / NOT IN** `x [NOT] IN (SELECT col FROM..)` — 内层列集 → IN 字面量集 (享 [min,max] 索引剪枝); 空集 → 恒假; NOT 包 Pred::Not
- **标量** `x op (SELECT ..)` — 0 行→NULL(恒假), 1 行→常量, >1 行→报错; 支持聚合内层 (SELECT MAX/MIN) 与 pk-point 内层
- **EXISTS / NOT EXISTS** `[NOT] EXISTS (SELECT 1 FROM..)` — 非空→恒真/空→恒假; 子查询中字面量投影 (SELECT 1) 视为全列
- DELETE/UPDATE ... WHERE 带子查询; 多个子查询顺序串行 (仿 SqlUniqueIns)

### 边界 / 已知限制

- **仅非关联** (内层不引用外层列); 关联子查询报错 (内层 `o.uid=u.id` 的外层列非字面量→解析/执行拒)
- **大结果集 IN 阈值拦截** (去重 >1024=SUBQ_MAX_ROWS 报错引导改写 JOIN; 半连接优化留后)
- 内层 v1 限单表 SELECT (JOIN 内层/嵌套子查询 拒, 避免绕过 SqlSelectAgg 拦截)
- NOT IN 遇 NULL 三值逻辑 v1 简化 (与现有 NULL 恒 false 一致)
- **FROM 派生表未含** (需 TableSource enum 波及 + 内存外层执行, 独立后续)

### gotcha

- **SqlValue::Subquery 与 Param 同模式**: 执行前必解占位, sql_to_col 防御报 "unresolved subquery"; 折叠保持 `Pred<Cond>` 类型不变 → 下游 plan/eval/shard 零改动
- **完成点两路径都要拦**: 内层可能走 SqlSelectAgg (Index/FullScan/agg) 或 SqlRowCtx (pk-point), 两处都需 is_subq 拦截 materialize 而非渲染
- EXISTS 恒真 = `And(vec![])`, 恒假 = `Not(Box::new(And(vec![])))` — 无需新 Cond 变体
- 折叠与 collect 必须同 DFS 序 (Leaf-first, And/Or 左右, Not 入内) 才能正确配对

---

## 2026-07-31 会话十二 (F70: JOIN gather 索引点查优化)

纯性能优化。probe 侧表 gather 时, 用前序已 gather 表的 ON 等值键值集合下推为
索引点查 (而非全表扫), 让 shard 只回匹配行。零跨线程、不改 JOIN 语义/结果。

### 性能实机 (mysql-connector, 3000 行/表)

| 查询 | 优化前 | 优化后 |
|---|---|---|
| `u JOIN o ON u.id=o.uid WHERE u.id=1500` (o.uid 有索引) | 16ms | **2.5ms (~6.3x)** |
| 同上但 o.uid 无索引 (对照) | — | 10ms (正确回退全表扫) |
| 50 键 build 侧 (u.id<50) | — | 3.0ms |
| LEFT JOIN (右表键集合点查) | — | 2.6ms |

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| K1 | `KeySetHint{iid,keys}` 类型 + `index_multi_point_local` (逐键等值点查+bloom短路+去重+批量回表) + table_scan_filtered_local 第3路 (key_set>hint>全扫) | storage/sql_rows.rs |
| K2 | `BatchOp::ScanFiltered` 加 key_set_hint + re-export + manager 透传 | request.rs/lib.rs/manager.rs |
| K3 | `sql_join_keyset_hint` 决策 + sql_join_broadcast 优先键集合 | worker.rs |
| K4 | 回归 (现有 JOIN e2e 全过验证不改语义) + 性能实机 + 文档 | — |

### 启用条件 (安全边界)

- 仅 **单列等值 ON** (多列组合键 v1 退回)
- 仅 **INNER / LEFT(右表)**; RIGHT/FULL/CROSS 退回全扫 (语义: finish 无法复活 shard 过滤掉的未匹配行)
- 新表 join 列**有普通二级索引** (无索引退回全扫, 无劣化)
- 前序键集合去重后 **<= JOIN_KEYSET_MAX(1024)** (超阈退回全扫)

### gotcha

- **优化不改结果**: tables[idx].rows 变为"join 键 ∈ 前序键集"的子集, 对 INNER/LEFT 是最终输出的精确子集 (未匹配右行本就不输出) → finish/折叠零改动
- **安全性依据 join kind 非残余兼底**: RIGHT/FULL 绝不下推 (与 F68 同原则)
- 键集合与新表自身 WHERE preds 正交: 索引点查取候选行后仍过 row_pass_preds, AND 叠加
- 剩余开销 (2.5ms vs pk 点查 0.05ms): JOIN 固有两轮串行 gather (u 回齐才 gather o) + 6 shard fan-out 往返, 非全表扫问题

---

## 2026-07-31 会话十一 (F69: OR/NOT/括号 谓词表达式树)

WHERE 从 AND-only 的 `Vec<Cond>` 升级为泛型谓词树 `Pred<C>`(Leaf/And/Or/Not),
支持 OR/NOT/括号嵌套, 覆盖单表 SELECT/DELETE/UPDATE/HAVING 与 JOIN 全路径。
(分阶段路线: Phase 1 JOIN 族 F68 ✓, 本轮 Phase 2; Phase 3 = 子查询, 另计划)

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| O1 | 泛型 `Pred<C>` + as_conjuncts/leaves/map/try_map/is_true + parse_where 改递归下降 (OR<AND<NOT<primary, 括号复用 LParen) | protocol/sql.rs |
| O2 | 5 个 `Vec<Cond>` 字段 + JoinCond + HAVING → `Pred`; bind_params 递归 try_map; parse_show/parse_join 适配 | protocol/sql.rs |
| O3 | sql_eval_conds → eval_cond_leaf + eval_pred 递归 (5 调用点); sql_plan_select 含 OR/NOT → FullScan 回退; JOIN eval_join_pred; HAVING eval_having_pred; sysq eval_pred_sysq | worker.rs |
| O4 | sql_join_broadcast/index_hint 下推用 as_conjuncts (含 OR → 空下推全扫) | worker.rs |
| O5 | e2e (mysql_or_predicates + mysql_or_join_having) + mysql/pg 驱动实机 + 回归 + 文档 | tests/sql_e2e.rs |

### 支持能力

- `WHERE a=1 OR b=2` / `(a=1 OR a=2) AND c>0` / `NOT (x=5)` / 任意嵌套括号
- 覆盖 SELECT / DELETE / UPDATE / JOIN WHERE / HAVING; 优先级 OR<AND<NOT<比较/括号
- **核心机制**: `Pred::as_conjuncts()` — 纯 leaf 合取返回平铺列表, 索引界/下推/bloom 原 AND 优化路径不变; 含 OR/NOT → None → FullScan/空下推, 完成点 eval_pred 递归残余保正确
- Cond 原样作 Pred::Leaf (sql_cmp/BETWEEN/LIKE desugar 均不动; desugar 产物 → And(leaves))

### 实机验收

mysql-connector + psycopg3: OR/NOT/嵌套括号/纯 AND 索引回退/DELETE·UPDATE OR/JOIN WHERE OR/HAVING OR 全正确, 跨协议一致.

### 边界 / 已知限制 (Phase 2)

- 含 OR/NOT 的查询不走索引 (全表扫 + 递归残余); 纯 AND 保持索引优化 (已知取舍)
- OR 不下推到 shard (JOIN/单表下推遇 OR 全扫; shard row_pass_preds 仍平铺 AND, 未改)
- 不做: OR-同列范围 → 索引并集扫; shard 侧谓词树下推; 子查询 (= Phase 3)
- NOT 语义: NULL 比较为 false, NOT false = true (二值简化, 与现有 NULL 恒 false 取舍一致; 与严格三值逻辑可能有差异)

### gotcha

- **as_conjuncts 解耦优化与正确性**: 所有 AND-假设代码 (索引界推导/下推/bloom/覆盖判定) 都通过 as_conjuncts 提平铺列表; 叶子列名校验用 leaves() (不论结构) — OR 查询也能报未知列
- **系统表 __ 内部叶子**: eval_pred_sysq 将 `__` 前缀叶子视为真 (生成器已处理)
- HAVING 有独立求值器 (输出列下标域) — AggSpec.having 改 `Pred<(usize,op,val)>`, eval_having_pred 递归

---

## 2026-07-31 会话十 (F68: JOIN 族完备化 Phase 1)

F67 两表 hash join 泛化为 **N 表左深 + 多条件 ON + RIGHT/FULL/CROSS/USING + 索引驱动 gather**。
仍 worker 完成点执行、零新增跨线程。(分阶段路线: Phase 2 = OR 谓词树; Phase 3 = 子查询, 均另计划)

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| A1 | AST 泛化: `SelectJoin{from, joins: Vec<JoinClause>}` + TableRef + OnPred(Eq/Cmp) + JoinKind(+Right/Full/Cross) + parse (N join/多 ON/USING/CROSS) | protocol/sql.rs |
| A2/A3 | SqlJoinCtx N 表状态机 (逐表补 schema → 逐表 Gather) + 左深迭代 hash join (宽行折叠) + 全 kind 语义 | worker.rs |
| A4 | ScanFiltered 加 index_hint + storage IndexHint + 索引范围扫 gather | storage/sql_rows.rs, request.rs, manager.rs, worker.rs |
| A5 | e2e (mysql_join_family) + mysql/pg 驱动实机 + 回归 + 文档 | tests/sql_e2e.rs |

### 支持能力

- **N 表左深** `a JOIN b ON.. JOIN c ON..` (书写顺序折叠, 无重排)
- **多条件 ON** `ON a.x=b.x AND a.y=b.y` (组合 hash 键) + 非等值残余 `AND a.t>b.t` (匹配时判定)
- **RIGHT** (未匹配右行补左 NULL) / **FULL** (双侧补) / **CROSS** (笛卡尔) / **USING(c)** (糖 → 未限定左侧 Eq, 按 join 位置作用域解析优先前序表)
- **索引驱动 gather**: 某表 WHERE 命中索引列 Eq/范围 → shard 走索引范围扫缩候选 (过度近似闭界 + 残余 preds 精确), 否则全扫
- 输出 `*` 展开各表全列 (列头 alias.col); ORDER/LIMIT/OFFSET; 行数上限每折叠步保护

### 实机验收

mysql-connector + psycopg3 双驱动: 3 表/RIGHT/FULL/CROSS/USING/多 ON/索引驱动 全正确, 跨协议一致.

### 边界 / 已知限制 (Phase 1)

- WHERE 仍 AND-only (OR = Phase 2); 无子查询 (= Phase 3)
- ON 至少一个等值对 (非 CROSS); JOIN 输入不做索引嵌套循环 (仅索引范围扫 gather)
- 大结果吃 worker 内存 (JOIN_MAX_ROWS 262144 上限, 无流式); 无一致快照
- USING 列在 `*` 不合并 (双表均出; 标准应合并, v1 小偏差)

### gotcha

- **宽行折叠**: acc = 已连接各表投影列拼接; col_offset[t] 定位; 外连接 null 扩展填 ColValue::Null 到对应切片
- **USING/未限定 ON 操作数**: 全局解析会歧义 → 专用 sql_join_resolve_on 按 join 位置限作用域 (未限定优先前序表, 取最后)
- **测试陷阱**: JOIN 结果内存中构建, 同键多行顺序取决于跨 shard gather 到达顺序 (非确定); e2e 断言必须用完整 ORDER BY

---

## 2026-07-31 会话九 (F67: 两表 hash JOIN, worker 完成点)

解决长期 gap: 跨 shard JOIN. 方案 = **JOIN 逻辑全在 worker (每连接单线程);
shard 只做本地单表扫+过滤+投影, fan-in 到 worker 后 build/probe**; 无 shard↔shard,
零新增跨线程原语, 不碰 Scheduler 同线程契约.

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| J1 | SelectJoin AST + 表别名 + 限定列 QualCol + JOIN/ON 解析 (独立变体隔离) | protocol/sql.rs |
| J2 | ScanPred/PredOp (定于 storage 避分层) + BatchOp::ScanFiltered + BatchResult::ProjRows + table_scan_filtered_local | storage/sql_rows.rs, request.rs, manager.rs |
| J3 | SqlJoinCtx 顺序状态机 (补 schema → GatherLeft → GatherRight) | worker.rs |
| J4 | hash join (右建表、左探测) + LEFT NULL 扩展 + 残余 WHERE + 输出列 + ORDER/LIMIT + 渲染 | worker.rs |
| J5 | e2e (mysql_join_two_tables) + mysql/pg 驱动实机 + 回归 + 文档 | tests/sql_e2e.rs |

### 支持能力

- **两表 `A [INNER|LEFT] JOIN B ON a.x = b.y`** (单 equi 条件); 表别名 `[AS] alias`; 限定列 `alias.col`
- **谓词下推**: 左表谓词恒下推; 右表谓词 INNER 下推 / LEFT 留 worker 残余 (保标准语义); finish 总会再残余全 WHERE (下推仅优化, 不影响正确性)
- **投影下推**: 各表只回 (输出 ∪ ON 键 ∪ WHERE ∪ ORDER) 引用列 (ProjRows 省带宽)
- SELECT * → 展开左右全列, 列头 `alias.col` 限定避重名; ORDER BY/LIMIT/OFFSET; 反向 ON (b.y=a.x) 等价
- 安全上限: 单侧 gather > 256K 行 → 报错止 OOM

### 跨线程账 (零新增)

worker→shard (现有 fan-out) + shard→worker (现有 reply_bus); 无 shard↔shard; 新增全为
worker 单线程内纯计算 (建表/探测), 无锁无跨线程. gather 复用 SqlSelectAgg fan-in 模板.

### 实机验收

mysql-connector + psycopg3 双驱动 INNER/LEFT/下推/重名列/`*` 全正确, 跨协议一致.

### 边界 / 已知限制 (文档化)

- v1 仅两表单 equi ON; 多表(≥3)/多 ON 条件/RIGHT/FULL/CROSS/USING/子查询/OR 不支持
- JOIN 输入全表扫 (带下推), 不走索引; 大结果吃 worker 内存 (有上限, 无流式)
- 无一致快照 (两侧 gather 时间差, 既有架构限制, 非 JOIN 新引入)
- 解析用独立 SelectJoin 变体隔离, 单表 Select 路径零改动; tokenizer 支持反引号/点号

### gotcha

- **分层**: BatchOp 在 shard_manager (storage 上层), 下推谓词不能用 network::Cond → ScanPred/PredOp 定于 storage::sql_rows, request.rs re-export
- **候选词向量下推 vs 正确性解耦**: finish 必须总重新应用全 WHERE, 下推只是带宽优化; LEFT 的右表谓词绝不能下推 (会在 null 扩展前错误删行)
- **LEFT 驱动侧稳定**: 固定右表 build、左表 probe, 保 LEFT 左驱动順序

---

## 2026-07-31 会话八 (F66: information_schema / SHOW 系统表虚拟化)

解决 GUI 工具 / ORM 反射依赖的系统元数据可见性. 方案 = **worker 层拦截系统表
查询 + 从活元数据合成虚拟表结果集** (复用 SELECT 完成点: 过滤/投影/排序/渲染).

### 交付总览

| # | 交付 | 文件 |
|---|---|---|
| C1 | SystemQuery 解析 (information_schema.* / pg_catalog.* 大小写不敏) + parse_select_tail 抽取 | protocol/sql.rs |
| C2 | CatalogDump BatchOp (任意单 shard 列当前 db 全表+schema) + BatchResult::Catalog | request.rs / manager.rs |
| C3 | worker 合成器 sysq_render_catalog / sysq_render_dblist / sysq_finish + sql_sysq 挂起 | worker.rs |
| C4 | e2e (mysql_information_schema + mysql_show_commands) + SQLAlchemy 实机反射 | tests/sql_e2e.rs |

### 支持的系统表 / 命令

- **information_schema**: `tables` / `columns` / `key_column_usage` / `schemata`
- **pg_catalog** (flat 单表, 无 JOIN): `pg_namespace` / `pg_class` / `pg_attribute`
- **SHOW** (MySQL 方言反射真路径): `SHOW [FULL] TABLES [FROM db]` / `SHOW [FULL] COLUMNS FROM t` /
  `SHOW CREATE TABLE t` (重建 MySQL DDL) / `SHOW DATABASES|SCHEMAS` / 其他 SHOW → 空结果 stub
- **反引号标识符** `` `name` `` (tokenizer) — SQLAlchemy `SHOW ... FROM \`db\`` 必需
- **`SELECT @@var`** 系统变量 stub (transaction_isolation/version/sql_mode/…) — SQLAlchemy 方言初始化探测;
  '@' 不过 tokenizer, 在 parse_prepared tokenize 前拦

### 实机验收 (SQLAlchemy 2.x + PyMySQL)

`inspect(engine)` 全链路通: `get_table_names()` (SHOW FULL TABLES) / `get_columns()`
(SHOW CREATE TABLE 解析) / `get_pk_constraint()` / `get_unique_constraints()` 全正确
(列类型 INTEGER/TEXT/DOUBLE, pk=id, unique=sku).

### 边界 / 已知限制 (文档化)

- **psql `\d` / `\dt` (pg_catalog 多表 JOIN) 不完整** — 需 JOIN, 留后; information_schema 单表反射可用
- v1 仅反射 current_db (跨 db information_schema 查询限当前库)
- pg_catalog 表为 flat 单表数据 (可直接 SELECT), 不支持 psql 依赖的 OID JOIN 语义
- 系统表只读虚拟 (无 DML); 复杂 WHERE (子查询/OR) 不支持
- 虚拟列均按 Str 输出 (ordinal_position 数字也用字符串, 与 MySQL information_schema 惯例一致)

### gotcha

- **CatalogDump locator table 为空**: ShardTask 执行前的 `ensure_table(db, table)` 遇空表名会
  报 "empty key is reserved for sentinel" (btree 空键). 修复: 无表名的元 op (table 空) 跳过 ensure_table.
- **SQLAlchemy MySQL 不走 information_schema**: get_table_names 走 `SHOW FULL TABLES`, get_columns 走
  `SHOW CREATE TABLE` (从 DDL 正则解析). information_schema 是跨标准备选, SHOW 才是 MySQL ORM 真路径.

---

## 2026-07-31 会话七 (F65: 全局跨 shard UNIQUE 约束)

解决长期文档化 gap: UNIQUE 跨 shard 漏检. 方案 = **opt-in `GLOBAL UNIQUE` 列**
+ email-shard 占坑 + 数据面 worker 编排 + 懒校对自愈.

### 交付总览 (计划 U1-U5)

| # | 交付 | 文件 |
|---|------|------|
| U1 | schema `IndexDef.global` + `GLOBAL UNIQUE` 语法 (列级, 隐含 NOT NULL, 不可为 pk); FMT_VER 1→2 (decode 兼容 v1); `TableSchema::new` 加 global_unique_cols 参 | `schema.rs`, `sql.rs` |
| U2 | 占坑行 `[U][iid][enc_val]`→`[state][txn_id][pk]` (自带 WAL); 单线程原子 unique_reserve/steal/confirm/release; BatchOp ReserveUnique/StealUnique/ConfirmUnique/ReleaseUnique + shard 端 exec | `keyspace.rs`, `sql_rows.rs`, `request.rs`, `manager.rs` |
| U3 | worker 顺序状态机 `SqlUniqueIns` (Reserve→Verify→Write→Confirm, 至多一个在途 op, 每 reply 推进一步); autocommit 单行 INSERT 全流程; 事务内写/UPDATE 全局唯一列 → v1 边界拒绝 | `worker.rs` |
| U4 | 懒校对: reserve 遇 COMMITTED 冲突 → Verify 回查持有者 pk-shard 行; 行存且值匹配→真冲突拒, 否则 stale→抢占 (删后重插自愈); PENDING 冲突→拒 (在飞保护) | `worker.rs` |
| U5 | e2e + 实机双驱动; 顺手修 SQLSTATE 映射 (ORM 异常分类) | tests, `mysql.rs` |

### 关键设计

- **不复用 DDL 2PC 协调器** (控制面 MVP/无 pending 态/内存态): 占坑 = email-shard 一条持久化物理行 (行本身即 prepare 记录, 自带 WAL 崩溃重放)
- **pk-shard 的行 = 唯一真相源**, 占坑行是二手 hint; 任何坑状态都可被回查行推翻
- **email-shard 单线程 = check-and-reserve 原子** (并发同值串行化, 无锁): 第一个占 PENDING, 第二个见 PENDING 即拒
- **新命名空间 `U`** (0x55, 避开 S/H/L/T/Z/#/$/I); enc_val 复用索引值编码, 路由与 pk 独立

### 验收

- **旗舰**: 多 shard 下不同 pk (落不同 shard) 同 email → 必拒 1062 (之前的 gap 场景); 遍历多 pk 全拒; 不同 email 各自成功; 幂等重插; 删后重插自愈
- **实机**: mysql-connector → `IntegrityError`(errno 1062), psycopg3 → `UniqueViolation`(23505), 跨协议全局唯一一致 (MySQL 写 PG 拒重)
- **顺手修复 SQLSTATE 映射**: build_err 按 errno 发正确 SQLSTATE (1062→23000/1213→40001/…), 之前恒发 HY000 导致 ORM 将 UNIQUE 冲突误归 DatabaseError 而非 IntegrityError
- 回归: net e2e 全绿 + storage+sm+cfg 537/0, clippy 0

### v1 边界 (文档化)

- 每个 global unique 写多 2-3 次跨 shard 往返 (reserve+write+confirm), 写延迟↑ — opt-in 付费, 普通表零影响
- 事务内写全局唯一表 / UPDATE 全局唯一列 / 多行 INSERT → v1 拒绝 (非静默破坏); 多列联合全局唯一 / 在线加 GLOBAL UNIQUE → 留后
- PENDING 在飞窗口内并发插同值第二个被拒 (客户端重试即成); stale 坑懒清 (不主动 GC)

---

## 2026-07-31 会话六 (F64: 端到端正确性检验 + 两项修复)

首次真实驱动 (mysql-connector + psycopg3) 端到端正确性检验: 订单系统工作流组合压全部功能 (schema/UNIQUE/事务原子性/SERIALIZABLE/GROUP BY/预处理/跨协议/索引) + 跨协议一致性. 发现并修复两项:

| # | 问题 | 根因 | 修复 |
|---|------|------|------|
| F64a | 事务内 UPDATE 后 pk 点查读不到自己的改动 (RYOW 破破) | v1 的 RYOW 仅覆盖 INSERT/DELETE (index 单下标), UPDATE 直通读盘 | `resolve_ryow` 重放同 pk 全部缓冲 op → Resolved(纯内存态)/NeedBase(读盘基行+overlay 叠加 sets); SqlRowCtx.ryow_overlay 在消费点叠加 |
| F64b | duplicate key 错误码返 1105 (非 MySQL 标准) | mysql_err_packet 漏映射 (PG 侧有 23505, MySQL 侧无) | 补 `duplicate key → 1062` (ER_DUP_ENTRY, ORM 据此识别 IntegrityError) |

### 验收

- 端到端 20 项检验全过 (含转账原子性/总额守恒、SERIALIZABLE 冲突捕获、GROUP BY/HAVING/AVG 报表、注入参数作字面值、MySQL↔PG 跨协议读写一致)
- 新增回归 e2e: `mysql_txn_ryow_update` (UPDATE overlay / 多次叠加 / INSERT→UPDATE 链 / UPDATE→DELETE / 跨连接隔离); `mysql_unique_index` errno 断言改 1062
- 回归: net e2e 全绿 + storage+sm+cfg 537/0, clippy 0
- **确认的文档化 gap** (非本轮引入): UNIQUE 全局跨 shard 漏检 (探测仅本 shard, 单 shard 正常拒绝)

---

## 2026-07-31 会话五 (F63: GROUP BY 聚合族)

### 交付总览 (计划 G1-G4)

| # | 交付 | 文件 |
|---|------|------|
| G1 | 解析扩展: `SelectItem::{Col, Agg{func,col}}` + `AggFn{Count/Sum/Avg/Min/Max}` (label 方法统一列头/HAVING/ORDER 匹配); SqlStmt::Select `cols:Vec<String>` → `items` + `group_by` + `having`; 旧 `count:bool` 退役; 解析校验非聚合项 ∈ group_by | `sql.rs` |
| G2 | worker 分桶完成点: `Accum` 累加器 (Count/SumI\|F+seen/Avg/Min/Max, NULL 忽略, SUM 溢出报错, 空集 SUM/AVG→NULL); 自包含类型标记组键编码 (NULL 归一组); WHERE→分桶→HAVING→ORDER→OFFSET/LIMIT; 分桶上限 64K | `worker.rs` |
| G3 | 合成结果集渲染 — 复用 `sql_rows_bytes` 三门面统一 (render_sql_count 收编为单列特例); 合成列头 ("SUM(amt)") 动态列定义 | `worker.rs` |
| G4 | e2e (mysql_group_by_aggregates / pg_group_by_aggregates) + 实机双驱动 | tests |

### 关键设计

- 落在最便宜的位置: SELECT 数据流已有 shard 收行→worker 聚合→完成点 (ORDER/COUNT/LIMIT 均在此), GROUP BY = 完成点插一步分桶 — shard/存储/协议帧零改动
- 含聚合/group_by 的 SELECT 走广播扫描/索引路径 (PkGet 降级广播 — 聚合需全量行); COUNT(*) 无 GROUP BY 保留旧特例路径 (零回归)
- 裸聚合 = 全表单桶退化 (SELECT SUM(x) FROM t 与 GROUP BY 同代码); 空表无 group_by 输出单行 (COUNT=0 其余 NULL — PG 语义)

### 边界 (文档化)

- 不做: 表达式聚合 SUM(a+b) (无表达式系统) / COUNT(DISTINCT) / GROUP_CONCAT / 别名 AS / GROUP BY 位置引用 / 窗口函数
- shard 端部分聚合下推 (COUNT/SUM 分配律) 留 v2 性能轮; 当前全量收行 (内存与 ORDER BY 现状一致)
- HAVING/ORDER 列名用聚合原文匹配 (大写归一), 不支持别名

### 验收

- e2e: 裸聚合 (COUNT/SUM/AVG/MIN/MAX + WHERE 索引路径) / COUNT(col) 忽 NULL / 空结果单行 / GROUP BY 单多列 / HAVING / ORDER BY 聚合列 DESC+LIMIT / 非聚合项不在 group_by 报错 / SUM 非数值报错 / 旧 COUNT(*) 不回归 — 全过
- **实机**: mysql-connector + psycopg3 跑 GROUP BY/HAVING/AVG/ORDER BY 聚合列, 结果与定义一致
- 回归: others 353 + storage 476 = **829/0**, clippy 0

---

## 2026-07-31 会话四 (F62: 事务 v2 — 多隔离级别标准 + OCC 验证 + SAVEPOINT)

### 交付总览 (计划 V1-V4)

| # | 交付 | 文件 |
|---|------|------|
| V1 | 隔离级别语法全集: `SET [SESSION] TRANSACTION ISOLATION LEVEL 四级 [READ ONLY\|WRITE]` (SET 整吞前剔出 TRANSACTION 子句) + `BEGIN/START TRANSACTION [ISOLATION LEVEL ...][READ ONLY\|WRITE]` 尾缀; 四级归并两档 (RU→RC, RR→Serializable); conn 默认 + 事务级覆盖 | `sql.rs`, `worker.rs` |
| V2 | OCC backward validation: SERIALIZABLE 事务内 pk 点查记 read_set (首读 crc32 指纹, RYOW 读不记); COMMIT 时 ReadCheck 按 pk 路由随 TxnApply 下发, shard 预检首步重读比对 — 变了整批拒 **40001/1213** (serialization failure); 纯验证批 (ops 空) 支持 | `request.rs`, `manager.rs`, `wal.rs` (crc32 pub) |
| V3 | SAVEPOINT / ROLLBACK TO / RELEASE: ops 水位截断 + index 重建; **E 态下 ROLLBACK TO 允许** (SQLAlchemy/psycopg 恢复 aborted 子事务标准路径), 成功后清 failed 位; read_set 不回滚 (保守更严格无损正确性) | `worker.rs` |
| V4 | READ ONLY 事务 (写拒 25006/1792); 错误码映射 40001/1213/25006/25P02 | `worker.rs` |

### 关键设计

- 隔离是语义规范非实现规范: SERIALIZABLE 靠 commit 时验证而非阻塞 — 仍然**零锁零 MVCC 零调度器改造**; shard 单线程 = 验证+应用天然原子 (别家要 latch 构造的窗口我们免费)
- 纯读 SER 事务 commit 直接成功 (无写则序列化点可取 BEGIN 时刻, 无需验证)
- RC 事务零 read-set 开销 (sql_read_key 在非 SER 直接 None)

### 诚实边界 (文档化)

- **不防幻读**: 行级 OCC 无谓词/范围指纹 — SERIALIZABLE 防脏读/不可重复读/丢失更新/行级写偏斜, 与 PG SSI 有差距; 扫描/索引读不进 read-set
- RR 与 SERIALIZABLE 在本实现中等价; crc32 指纹 ABA 碰撞 2^-32 理论存在 (升 64 位留后)
- 真快照读 (COW meta_cache 视图) 仍留后 — 需跨 shard 一致性快照点

### 验收

- e2e: mysql_isolation_levels (SER 冲突 1213 + 重试 + RC 对照 last-writer-wins + 无假阳性 + 纯读 + READ ONLY 1792) / mysql_savepoints (嵌套回滚/重复回滚/RELEASE 失效) / pg_serializable_conflict (40001 + SET SESSION 继承 + 25006 + **E 态 ROLLBACK TO 恢复 T 态**) — 全一次过
- **实机**: psycopg3 `isolation_level=SERIALIZABLE` 并发冲突被类型化捕获 `psycopg.errors.SerializationFailure` → 重试成功; SQLAlchemy 风格 savepoint 序列全通
- 回归: others 351 + storage 476 = **827/0**, clippy 0

---

## 2026-07-31 会话三 (F61: 事务 v1 — conn 层缓冲 + commit 原子批, RC)

### 交付总览 (计划 T1-T4)

| # | 交付 | 文件 |
|---|------|------|
| T1 | conn 层 `TxnState` (保序 ops + RYOW index + 8192 ops/8MB 护栏); 写语句截流 (INSERT / pk UPDATE·DELETE / 两阶段 phase2); RYOW pk 点查命中 write_set 直回缓冲 (INSERT 见新/DELETE 见空; UPDATE 直通读盘) | `worker.rs` |
| T2 | SqlStmt::Begin/Commit/Rollback (BEGIN/START TRANSACTION); 双协议状态 **resp_complete 单点注入**: PG ReadyForQuery 尾字节 I/T/E 改写 + 事务内 ErrorResponse 自动置 failed (25P02 拦截, ROLLBACK 清位); MySQL 纯 OK 包 status \|= IN_TRANS — 零渲染函数签名扩散 | `sql.rs`, `worker.rs` |
| T3 | `BatchOp::TxnApply` + `exec_task_op` 提取 (338 行 ShardTask 臂原样提函数, 与事务批共用); `exec_txn_apply` **先验后写** (ensure_table + row_put_check 预检 + 批内自冲突检测) + 无条件 wal_barrier; sql_rows 拆 `check_unique`/`row_put_check` | `manager.rs`, `request.rs`, `sql_rows.rs` |
| T4 | e2e ×3 (mysql_transactions / mysql_txn_unique_single_shard / pg_transactions) + 实机双驱动 + kill -9 | tests |

### 关键设计 (与用户对齐的基线)

- **shard/调度器零事务状态**: 时间维度 (交互式间隙属于客户端, 不占 shard) + 空间维度 (跨 shard 编排本就在 worker) → 事务 = conn 层缓冲, COMMIT 时 shard 只见一个原子批 (单线程 = 批内零并发穿插, 免锁免 MVCC)
- **OCC 路线**: 四种隔离是语义规范非实现规范, 阻塞是 2PL 的选择; v1 = RC + 原子写批; Serializable (read-set 值指纹验证) / 快照读 (COW meta_cache 视图, 非 MVCC) 留 v2
- **commit 持久化 = WAL barrier**: TxnApply 无条件 barrier (独立于 wal_mode periodic/strict), 回复到达 ⇒ 已 fsync; wal_mode=off 时退化 (文档化)
- **先验后写零部分应用**: 预检 = ensure_table 全表 + RowPut 逐个 unique 探测 + 批内自冲突 map; 预检后仅剩 IO 级失败 (灾难态标注 partially applied)
- **ROLLBACK/断连零成本**: 丢 write_set 即可, 连 shard 都不通知; bloom 事务中照喂 (rollback 只多假阳性, 只增语义无害)

### v1 语义边界 (文档化)

- 隔离 = RC; RYOW 仅 pk 点查 (扫描/索引/COUNT 读已提交态; UPDATE 后点查读已提交版本); affected 乐观估
- 跨 shard commit 原子性 best-effort (单 shard 严格; 已应用分片不回滚); unique 跨 shard 漏检为 O3 既有 gap 非事务引入
- DDL 在事务中拒绝; MySQL 门面不置 failed (语句失败事务继续, 符合 MySQL 语义); 事务仅 SQL 三门面 (RESP MULTI/EXEC 另议)

### 验收

- e2e: 可见性/RYOW/ROLLBACK/UPDATE·DELETE 混合/unique 零部分应用 (单 shard 专项)/批内自冲突/DDL 拒/重复 BEGIN/断连隐式回滚/PG I→T→E 状态字节/25P02 拒后续 — 全过
- **实机 strict**: mysql-connector (autocommit=False, start_transaction/commit/rollback) + **psycopg3 默认非 autocommit 模式** (驱动隐式 BEGIN) 全流程含 RYOW/aborted 恢复; **COMMIT 后立即 kill -9 → 20/20 全恢复** (WAL 重放日志可见)
- 回归: others 348 + storage 476 = **824/0**, clippy 0; String mixed 316K 无回退 (非事务路径零触碰)

---

## 2026-07-31 会话二 (F60: WAL 预写日志 — 三档可配, strict 零丢失)

### 交付总览 (计划 W1-W4)

| # | 交付 | 文件 |
|---|------|------|
| W1 | `storage/wal.rs`: 段文件 `{block_root}/shard_{N}.wal.{seq:06}` + 记录 len+crc32 (torn tail 静默截断) + 三档 WalMode + seal/drop_sealed/purge_all 生命周期 | `wal.rs` (新, ~370 行) |
| W2 | engine 接线: put_physical/put_physical_many/delete_physical 成功路径 append 结果态; open 时按段序重放 (ensure_table 覆盖惰性建表窗口) → flush → 删段; close 时 purge_all | `engine.rs`, `pager.rs` (meta_all_flushed) |
| W3 | strict 档: Batch 回复前 wal_barrier (天然批共享 fsync); ShardTask 组提交 (本轮有未 sync 写 → 回复押轮末一次 fsync 后统一 push); DDL (create_db/create_table, 含 2PC Prepare) 不进 WAL → 成功后强制 flush | `manager.rs` |
| W4 | 配置: `storage.wal_mode = "off"|"periodic"(默认)|"strict"` 全管道 + validate | `config`, `main.rs`, `nexusdb.toml` |

### 关键设计

- **插入点唯一**: 全部写路径 (String KV / SQL row / Redis 复合) 收敛到三个 physical 原语; 非幂等 RMW (INCR/APPEND/..) 在 shard 层先算结果态才落 KV → WAL 只记 (db,table,pkey,value/del), 重放天然幂等 (last-writer-wins)
- **段生命周期与刷盘对齐**: maybe_periodic_flush 触发快照 → 同轮内 seal (无并发写间隙, 段覆盖记录 ⊆ 快照); complete_meta_flush 全部确认 (meta_all_flushed) 后删 sealed 段; 晚删无害 (重放幂等), 早删禁止
- **fsync 后端分派**: IoUring 走 io_ops::fsync 异步 SQE (不阻塞 shard), StdFs 走 sync_data
- **strict 读 op 不受累**: 无待 sync 内容时直发回复; 乱序到达由 worker seq 重排兼容
- **DDL 不进 WAL** (catalog 页写非 kv 原语): create_db/create_table 成功后立即全量 flush (低频); 惰性建表窗口由重放侧 ensure_table 覆盖

### 验收

- **kill -9 实机**: strict 写完**立即**杀 50/50 全恢复 (旧行为必丢); periodic 写后 2s 杀 50/50
- **性能** (memtier String mixed): off 234K / periodic 231K (**-1.6%**, 达标 <5%) / strict 63K @ 8.1ms (组提交下保住 27% 吞吐, 严格持久化的合理代价); SQL 点查 51-57K 无回退
- 回归: storage 476 (+9 wal) + 其余 345 = **821/0**, clippy 0
- **gotcha 更新**: crash 测试不再需等 10s 刷盘 — strict 立即可杀, periodic 等 >1s 即可 (WAL 前的旧 gotcha 作废)
- 丢失窗口: off 10s → periodic ~1s → strict 0

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

