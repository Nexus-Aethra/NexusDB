# 修复计划+实施记录: scanner 遍历漏 internal 页 first_child (新 P1-19)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-19
> 状态: **已完成 (实际根因比审计描述更深, 见下)**

## 1. 问题 (两层, 第二层为审计未点破的真根因)

**表层 (审计所述)**: walk_tree / do_in_order / lookup 三处遍历只跟 item 的
child_vpid, 漏掉 internal 页的 first_child — export 数据不完整.

**真根因 (本次修复中发现)**: internal 页的最左 child 存在**页头 vpid 字段**
(引擎 chunk_writer.rs:131-136 明确注释: "Internal page 的 vpid 字段被 page
crate 复用作 first_child, 必须保留 caller 设置", 且仅对非 Internal 页自动
填自身 vpid). 而 scanner 的 `PageReport::decode` 对**所有页**校验"页头 vpid
== 自身 vpid" → 每个 internal 页都被误判 `VpidMismatch/bad` → walk_tree
拒绝下降 → **真实多层树的扫描从来就走不出根页** (比"漏左子树"严重得多).

## 2. 修复 (三处)

1. `page_decode.rs`: Internal 页跳过 vpid 匹配校验 (其正确性由 first_child
   语义承担 — 页头 vpid 理应 != 自身 vpid);
2. `tree.rs iter_internal_children` (walk_tree/BFS): 先收页头 first_child,
   再收各 item 的 child;
3. `tree.rs do_in_order` (in-order export): 先递归 first_child 子树;
4. `commands/lookup.rs`: first_child 从页头读 (旧实现从哨兵 item 读, 恒为
   0 → target < 首个 separator 时误报 not found).

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 新回归 `in_order_walk_includes_first_child_subtree` | ✅ 3 页树 (internal root + 左右叶): in-order 导出全部 4 key (修复前只有右子树 2 个); BFS 覆盖全部 3 页 |
| `cargo test -p nexusdb-scanner` (66 测试) | ✅ 66 passed / 0 failed |
| 全 workspace 串行 | ✅ 1097 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

顺带记录: scanner 的 `lookup_mate` 不校验 slot 存活 — 全零 page.mate 会把
所有 vpid 解析到 page 0 (测试需写真值 slot); 真实救援场景的 mate 来自引擎,
暂不扩大范围.
