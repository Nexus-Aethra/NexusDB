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
