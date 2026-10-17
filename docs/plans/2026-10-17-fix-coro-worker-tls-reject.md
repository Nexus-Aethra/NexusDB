# 修复计划+实施记录: 协程 worker TLS 半修 (新 P1-4)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-4
> 状态: **已完成 (最小安全修复; 完整异步 TLS 为后续功能项)**

## 1. 问题

协程 worker (`NEXUS_CORO_WORKER=1`) 与 epoll worker 共用
`process_sql_input`/`process_pg_input` — MySQL SSLRequest / PG SSLRequest
均可触发 `start_tls()` 升级. 但协程的读路径 `recv_async` (io_uring)
**无 TLS 解密分支** (仅同步 recv 有): 升级成功后密文被当明文解析,
MySQL 握手挂死 / PG StartupMessage 解析失败.

## 2. 修复 (最小安全面)

协程连接上**显式拒绝** TLS 升级, 而非静默进入坏状态:
- `ConnState` 新增 `coro_worker` 标记 (worker_coro 构造后置 true);
- MySQL SSLRequest (协程): 回 ERR 1043 "TLS not supported on this
  connection" + 断连 (客户端可回落明文重连);
- PG SSLRequest (协程): 回 'N' (拒绝 SSL), 与"未配置 TLS"行为一致,
  客户端按明文继续.

epoll worker 路径不变 (其 recv 有完整 rustls 解密分支).

## 3. 测试与验证

| 验证 | 结果 |
|---|---|
| 全 workspace 串行 (含 worker_coro_e2e 协程路径) | ✅ 1102 passed / 0 failed, exit 0 |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

注: 仓库无 TLS e2e 设施 (无证书生成依赖, 测试均 tls_config: None),
按构造验证 + 协程路径全量回归; 建议后续为协程 worker 补
recv_async 的 TLS 解密分支 (功能项).
