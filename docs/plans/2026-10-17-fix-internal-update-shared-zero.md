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
