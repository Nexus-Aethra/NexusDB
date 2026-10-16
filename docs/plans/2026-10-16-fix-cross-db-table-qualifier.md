# 修复计划+实施记录: strip_db_qual 静默改写跨库表名 (新 P0-3)

> 日期: 2026-10-16 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P0-3
> 状态: **已完成并提交**

## 1. 问题

解析器 `table_ident()` (parser.rs) 静默剥离 db 限定前缀 (`db.tbl` → `tbl`),
worker 全部按 `conn.current_db` 寻址 → `USE app; SELECT * FROM otherdb.users`
**静默查询 app.users**: 读返回错误库数据 (跨库暴露), 写污染/删除当前库数据,
无日志无指标. 探针实测 SELECT/INSERT/UPDATE/DELETE/DROP/SHOW 全部语句类型
(含反引号形式) 的限定名均在解析期被丢弃.

## 2. 修复设计 (已实施)

**解析期保留限定名 + 执行期单点校验** (AST 与下游不变, 波及面收敛到一个函数):

| 文件 | 改动 |
|---|---|
| `protocol/sql/ast.rs` | 删除 `strip_db_qual` |
| `protocol/sql/parser.rs` | `table_ident()` 原样返回 ident (保留 `db.tbl`) |
| `protocol/sql/parser_select.rs` | 移除系统表分派后的剥离; `SHOW COLUMNS FROM t FROM db` 的库限定从 `let _ = ident` 丢弃改为拼入 `db.t` |
| `worker/sql_dbqual.rs` (新增) | `normalize_stmt_tables`: 三态规则 — 无限定透传 / 同库剥离 / 跨库显式报错 (库不存在→`Unknown database` 1049/3D000; 存在→`Cross-database access is not supported` 1235/0A000). 覆盖单表语句 ×10 / JOIN 的 from+joins (含默认别名同步) / 嵌套 inner (派生表+EXISTS) / SHOW 虚拟表 __table__ 条件 |
| `worker/sql_dispatch.rs` | `sql_dispatch_stmt` 入口统一校验 (三门面 + RESP + 递归调用共 8 个调用方全部覆盖) |
| `worker/sql_eval.rs` / `protocol_io.rs` | 新错误码映射 (PG 0A000 / MySQL 1235) |

## 3. 测试 (全部实测)

| 验证 | 结果 |
|---|---|
| **反向验证** (git stash 暂存修复后跑新 e2e) | ❌ `SELECT v FROM other.users: 期望 Err(1235), 实际 Rows([[Some("app_row")]])` — 精确复现静默错数据 |
| 修复后 e2e (`sql_cross_db_regression.rs`) | ✅ 跨库读写删 DROP/SHOW 全报错; 同库限定名/反引号形式放行; 当前库零污染 |
| `sql_dbqual` 单元测试 | ✅ 7/7 (无限定/同库/跨库存在/跨库不存在/JOIN别名/SHOW cond/嵌套) |
| `mysql_orm_shapes` (既有测试) | 原断言依赖旧 bug 语义 (`default.orm_t` 静默按当前库解析), 已更新为同库限定 `app.orm_t` + 跨库报错断言 |
| `cargo test -p network` (串行) | ✅ 211 passed / 0 failed |
| **全 workspace 串行回归** | ✅ **1074 passed / 0 failed / 22 ignored, exit 0** (基线 1066 + 新增 8) |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

## 4. 已知边界

- 反引号引用且名内带 '.' 的表名 (`` `my.table` ``) 与限定名在 tokenizer 层不可区分:
  旧实现静默截成 `table` (错表), 现实现报错 (loud fail), 方向正确.
- 真跨库路由 (按限定名路由到目标库) 未做 — 属功能项, 建议单独立项.

## 5. 顺带发现 (未修, 建议立项)

全量回归中两次命中同一**既有 flaky**: `ShardManager::create_db` →
`refresh_db_view` → `block_on_v2` 在测试 setup 阶段无限 park (gdb 栈实证),
即审计 P1-11 "shard 线程无超时 park" 模式 (丢失唤醒竞态). 与本修复无关
(栈在 start_server, 任何 SQL 之前; 且修复前全量 1066/0 通过过), 但会随机
挂起测试套件. 建议: block_on_v2 加超时 + refresh_db_view 失败路径.
