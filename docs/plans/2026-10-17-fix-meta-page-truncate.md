# 修复计划+实施记录: init_meta_page truncate(true) 可清空全部数据 (新 P1-3)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-3
> 状态: **已完成**

## 1. 问题

`init_meta_page` (engine_io.rs) 以 `truncate(true)` 打开
`block_dir/000001.block` 写 MetaPage. 当 recover 因故未识别 vpid 0 映射
(meta 判定失误) 而走"全新库"分支时, **打开动作即把含真实数据的整个文件
清空** — 一次误判 = 全量数据毁灭. 同仓库 chunk_writer.rs:598 早有注释
明示此教训 ("不能 truncate(true), 否则 reopen 时会清空已存在数据"),
此处只是漏改.

## 2. 修复

`truncate(true)` → `truncate(false)`: 只写 page 0 (offset 0), 文件其余
字节原样保留. 全新库的文件本就不存在 (create(true) 新建) 或为空, 不受
影响; 已有数据文件的未引用页由 liveness/GC 回收, 不应由 open 误判清空.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| `init_meta_page_preserves_existing_data` | ✅ 3 页预置数据文件: 长度不变, page 1+ 逐字节保留, page 0 为 MetaPage, vpid 0 登记 |
| `init_meta_page_creates_missing_file` | ✅ 文件不存在时正常创建+登记 |
| `cargo test -p storage` (串行) | ✅ 507 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |
