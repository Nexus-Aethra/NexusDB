# 修复: full_key 对损坏 item 静默截断产出错 key (P2)

> 日期: 2026-10-17 | 状态: **已完成**

## 问题

`Item::full_key` 对 `shared_prefix_len > prev_key.len()` 的损坏 item 用
`min()` 静默截断 — 产出**错误 key** 且无任何提示 (C3 同族: 静默错读).

## 修复

- `full_key` 返回 `Result<Vec<u8>, PageError>`: shared 越界 →
  `Err(ItemDecode("corrupt item: ..."))`;
- 全部 38 个调用点适配 (page/storage/scanner):
  - Result 上下文直接 `?`;
  - dump/export/range 等遍历工具: 记 bad/停止, 不静默错 key;
  - try_bulk_leaf_split: 回退常规 split 路径.

## 测试

| 用例 | 结果 |
|---|---|
| `full_key_rejects_shared_overflow` | ✅ shared=10/prev=3B → Err; 合法 shared=2 → "abx" |
| 全 workspace 串行 | ✅ 1105 passed / 0 failed, exit 0 |
| clippy -D warnings / fmt | ✅ 0 / 0 |
