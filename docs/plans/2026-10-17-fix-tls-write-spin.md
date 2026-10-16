# 修复计划+实施记录: TLS 写路径 WouldBlock 自旋 (新 P1-5)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-5
> 状态: **已完成**

## 1. 问题

`ConnState::send_bytes` 两条写路径在 `WouldBlock` 时**无限 yield_now
自旋**:
1. TLS 路径 (`tls.write_tls` 泵密文): 慢客户端 (不读 socket) 会把 worker
   线程 100% CPU 钉死在此 — **同 worker 上所有连接一起停摆**;
2. 协程 worker 的遗留明文路径: 同病.

(epoll worker 的明文路径已有有界缓冲 + EPOLLOUT 续写, 不受影响.)

## 2. 修复

两条路径统一有界化 (与 P1-12 同模式):
- 前 64 次 yield (瞬堵的常见情形);
- 之后 100µs 退避 sleep — CPU 从 100% 降到 ~0;
- 30s 总上限: 超限 `close_after_flush = true` 断开该慢客户端 — stall
  从"永久"变为"有界", 其余连接恢复.

## 3. 测试与验证

| 验证 | 结果 |
|---|---|
| `cargo test -p network` (串行) | ✅ 222 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

注: 行为变化仅在 socket 持续 WouldBlock >64 次 yield 时出现 (测试客户端
均及时读, 不触发); 仓库无 TLS e2e harness (TLS 为 opt-in 且仅 SQL/PG),
按构造验证. 完整方案 (协程/TLS 的异步写集成) 属 P2-4 范围.
