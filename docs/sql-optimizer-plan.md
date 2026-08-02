# NexusDB SQL 优化器计划（sql-optimizer）

> 目标：从"tree-walking parser + 执行期贪婪选路"升级为 **Parse → AST → Binder → Optimizer → 物理计划 → Executor** 的完整管线，让查询享受真正的优化（常量折叠、谓词下推、索引选择、排序利用索引、投影裁剪…）。
> 定位：**不改变 SQL 语义**，只做访问路径/执行顺序的智能选择 —— 为上层应用（Story Loom / Crucible）提供可预测、可扩展的查询性能。

## 一、现状分析（2026-08-02）

### 当前查询管线

```
SQL 文本 → parser (sql/parser.rs) → AST (SqlStmt)
                                        ↓  worker 执行期
                              sql_plan_select (sql_dispatch.rs:2622)
                                        ↓
                     SqlPlan::{ PkGet | Index | FullScan } → 广播 shard 执行
```

### 现有"规划"的局限

| 项 | 现状 | 局限 |
|----|------|------|
| 访问路径 | pk 等值 → `PkGet`；**首个**命中索引 → `Index`；否则 `FullScan` | 贪婪选首个索引，无代价比较；多索引场景选不到最优 |
| 谓词下推 | 纯 AND 合取下推到 shard（`ScanPred`）；OR/NOT 全扫兜底 | OR 不能走索引并集；复合索引前缀未利用 |
| limit/offset | 无条件且无排序时下推 | 有排序时不下推（即使索引天然有序） |
| 排序 | `ORDER BY` 显式排序 | 未利用索引有序性消排 |
| 投影 | 全列扫描后裁剪 | 无投影下推（shard 返回全行） |
| 常量 | 无折叠 | `WHERE id = 1+1` 每次计算 |
| 连接 | 仅支持 JOIN（worker 内存嵌套） | 无连接顺序/下推优化 |

**本质**：有"计划"（SqlPlan）但无"优化器"——选择规则是硬编码的贪婪启发式。

## 二、目标架构

```
SQL 文本
  │
  ▼
Parser (已有) ──► AST (SqlStmt)
  │
  ▼
Binder / 语义分析 (新: sql/binder.rs)
  │  列引用→列 id; 类型推导; 表/别名解析; 校验
  │  （复用现有 schema.col_by_name 校验，升级为绑定结果）
  ▼
Optimizer — 逻辑优化 (新: sql/optimizer.rs, RBO)
  │  · 常量折叠 / 恒真恒假谓词消除
  │  · 谓词简化 (a=a → true; NOT(NOT x) → x)
  │  · 谓词下推 (JOIN 内下推)
  │  · 投影裁剪 (只保留所需列)
  │  · OR → 索引并集 (IndexUnion)
  ▼
Optimizer — 物理优化 (新: sql/physical.rs, 规则+代价)
  │  · 访问路径: PkGet / IndexScan / IndexUnion / FullScan
  │  · 索引选择: 覆盖谓词最多的索引 + 复合前缀匹配 + 排序消排
  │  · limit/offset 下推 (含排序后下推)
  │  · 连接顺序 (小表驱动, M4+)
  ▼
物理计划 (SqlPlan 扩展为树: Scan→Filter→Project→Sort→Limit→Join→Agg)
  │
  ▼
Executor (已有 worker: 广播 shard + 完成点聚合)
```

### 新增模块（均放 `crates/network/src/protocol/sql/` 或 `worker/`）

| 模块 | 职责 | 依赖 |
|------|------|------|
| `sql/binder.rs` | AST → 绑定 AST（列→col id、类型推导、校验） | `ast.rs`, `TableSchema` |
| `sql/optimizer.rs` | RBO 逻辑优化（常量折叠/谓词简化/下推/投影裁剪/OR 展开） | `binder.rs` |
| `sql/physical.rs` | 物理计划生成（访问路径/索引选择/排序消排/limit 下推） | `optimizer.rs`, `TableSchema` |
| `worker/sql_exec_plan.rs` | 物理计划执行（现有 `sql_plan_select` 迁移 + 树执行） | `physical.rs` |

## 三、优化项清单与优先级

### P0 — 低风险高收益（纯规则，语义不变）

| # | 优化 | 说明 | 收益 |
|---|------|------|------|
| 1 | **常量折叠** | `WHERE a = 1+2` → `a = 3`；`WHERE 1=1` 消除 | 减少计算/谓词简化 | ✅ 2026-08-02 |
| 2 | **投影裁剪（列裁剪）** | `SELECT a FROM t` 只取列 a 的 shard 扫描 | 大幅减少 IO/传输（当前全行） |
| 3 | **复合索引前缀匹配** | 索引 `(a,b)` 命中 `WHERE a=? AND b=?` 或仅 `WHERE a=?` | 当前只匹配单列索引 |
| 4 | **多索引选择** | 候选多个索引，选覆盖谓词最多/界最紧者 | 替换贪婪首个索引 |
| 5 | **恒真/恒假谓词** | `WHERE 1=0` 直接返回空；`WHERE 1=1` 去条件 | 短路 |
| 6 | **NOT 化简** | `NOT (a = 1)` → `a <> 1`；`NOT (a > 1)` → `a <= 1` | 扩大索引可用范围 |

### P1 — 中收益（规则 + 物理配合）

| # | 优化 | 说明 | 收益 |
|---|------|------|------|
| 7 | **OR → 索引并集** | `WHERE a=1 OR a=2` → 两个 IndexScan 结果合并 | 当前全扫 |
| 8 | **排序消排（利用索引有序）** | `ORDER BY a` 走索引 `(a)` 天然有序 → 免 sort；配合 LIMIT | 大结果集排序成本 |
| 9 | **排序后 limit 下推** | 有排序+limit：shard 各自取 top-k → 合并再 top-k（多路归并） | 避免全量排序 |
| 10 | **谓词下推增强** | OR 分支各自下推；JOIN 内表过滤下推 | 减少中间行数 |
| 11 | **IS NULL 走索引** | `WHERE a IS NULL` 利用索引（NexusDB 索引支持 null 标记则可行） | 专项查询 | ⚠️ 语义已支持 (desugar 全扫, 修复 sql_cmp NULL 比较); 走索引需存储层 NULL 标记 (未做) |

### P2 — 代价优化（CBO，需统计信息）

| # | 优化 | 说明 | 收益 |
|---|------|------|------|
| 12 | **基数估算** | 从 shard 采样/维护每索引键基数 | 支持代价比较 | ✅ (M3-1 行数 + M3-4 索引列 distinct + M3-5 列 min/max 直方图基础; 统计持久化 stats.bin M3-1b); 完整 SampledHistogram 分桶直方图留 M3-6 |
| 13 | **连接顺序** | 小表驱动大表（NestedLoop 顺序） | JOIN 性能 | ✅ (M3-2: 双表 Inner 驱动交换, EstimateRows 选小表, 保列序; 方案 A: 行数合并一轮 + 小表阈值跳过统计, 开销收敛) |
| 14 | **哈希/合并连接** | 大结果集连接不依赖嵌套循环 | 大规模 JOIN | ✅ 已覆盖: worker 内存 hash join (右建 hash 左探测) + key_set 下推; 跨 shard 重分布哈希单机架构不适用 |
| 15 | **代价模型** | 行数/选择性 → 访问路径选择 | 优化器决策 | ✅ (M3-3: IN 大集合降权 + 无界范围降权 + M3-4 distinct 打破 Eq 平局; M3-5 min/max 区间占比接入需行数代价框架, 留 M3-6) |
| 15 | **物化视图/结果缓存** | 热点查询缓存（对接 RESP 内存） | 读密集场景 |

## 四、落地路线（里程碑）

### M1 — 优化器骨架 + P0 规则（安全 RBO）

```
1. 新增 sql/binder.rs: AST 绑定（列→col id, 类型校验, 简化的 ScalarExpr 求值常量）
2. 新增 sql/optimizer.rs: 常量折叠 / 恒真恒假消除 / NOT 化简 / 谓词简化
3. 新增 sql/physical.rs: 把现有 sql_plan_select 迁移为物理计划生成器,
   并升级: 多索引选择 + 复合前缀匹配
4. worker: SqlPlan 增加 Projection (列裁剪) 字段, shard 扫描只取所需列
5. 测试: 每个优化一条解析+执行断言
```

交付：`EXPLAIN SELECT` 雏形（打印物理计划），`scripts/sql_optimizer_test.sh` 全绿。

### M2 — 排序/limit/OR（物理增强）

```
1. ✅ 排序消排: ORDER BY 单列 ASC == 索引列 → 免 sort (SqlSelectAgg.sorted)
   · Index 计划: 索引序 (val,pk) 升序 = 排序序 → worker 端免 sql_order_cmp
   · FullScan 计划: TableScan 天然按 pk 序 → ORDER BY pk ASC 亦消排
   · DESC / 多列 / IndexUnion (跨分支无序) 不消排 → 回退全量排序 (正确性红线)
2. ✅ 排序后 limit 下推: sorted 且零残余过滤 (limit_push) → 每 shard 取
   top-(limit+offset), worker 归并后 early_cut; 残余过滤存在时 limit 不下推
   (过滤破坏 top-k 不变量), 仅免排序
3. ✅ OR 展开: IndexUnion 计划节点 (两个 IndexScan 合并)
4. ✅ 谓词下推: JOIN 内表过滤 (F68) + 同列等值 OR→IN 合并下推 (M2c) + 投影下推 (P0-2)
```

交付：常见分页/排序查询走索引 (LIMIT/OFFSET + ORDER BY 索引列)。

### M3 — CBO 起步（统计 + 连接）

```
1. ✅ 统计信息: shard 维护每索引近似基数, stats.bin 持久化 (M3-1/M3-1b)
2. ✅ 代价模型: 行数估算 → 访问路径/连接顺序选择 (M3-3/M3-4/M3-5)
3. ✅ 连接顺序: NestedLoop 小表驱动 (M3-2)
4. ✅ 方案 A (2026-08-02, 开销收敛调优): 双表行数合并一轮广播 (group 0/1 区分表)
   + 小表阈值 EST_SKIP_STATS_ROWS=1024 (两表行数均 ≤ → 跳过 distinct/ranges
   直接决策) → 小表 JOIN 固定 1 轮; 有索引大表 3 轮 (行数+distinct+ranges 各
   合并一轮); 无索引大表 1 轮 (候选空自动跳过).
   观测: /metrics nexusdb_sql_join_est_rounds / _skipped.
   轮数对比 (原实现 → 现实现): 无索引 2→1, 有索引小表 6→1, 有索引大表 6→3.
```

交付：`EXPLAIN` 含估算行数；多表 JOIN 顺序优化；统计开销收敛。

## 五、与现有代码的衔接

- **`SqlPlan` 扩展**：从 `{PkGet, Index, FullScan}` 枚举扩展为带投影/排序/并集的物理计划（保持向后兼容，现有 DML 路径复用）
- **`sql_plan_select` 退役**：迁移逻辑到 `physical.rs`，worker 调新生成器
- **确定性**：所有优化必须是确定性的（同 AST 同 schema → 同计划），不引入随机；保证多人同步一致性
- **可观测**：`EXPLAIN [FORMAT JSON] SELECT ...` 输出计划树（对接 RESP/PG 门面）

## 六、风险

| 风险 | 缓解 |
|------|------|
| 优化器引入 bug 导致错误结果 | 每个规则单测 + 现有 60 测试回归 + 优化前后结果一致性测试（`sql_optimizer_test.sh` 对比非优化执行） |
| 投影裁剪破坏残余过滤 | 只裁剪未参与过滤/排序/投影的列；裁剪前保留过滤所需列 |
| 排序消排误判 | 仅当 ORDER BY 完全 = 索引前缀且方向一致才消排 |
| 计划不确定性影响存档/同步 | 优化器纯函数式（输入 schema+AST），输出确定计划 |
