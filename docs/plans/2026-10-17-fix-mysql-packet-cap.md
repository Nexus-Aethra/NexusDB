# 修复计划+实施记录: MySQL 续包累积无上限致远程 OOM (新 P1-6)

> 日期: 2026-10-17 | 对应审计条目: CODE-AUDIT-REPORT-2026-10-16.md 新 P1-6
> 状态: **已完成**

## 1. 问题

`read_packet_full` 拼接续包时**无总大小上限**: 恶意/故障客户端可持续发送
0xFFFFFF 满包 → 服务端内存无界增长 → 远程 OOM. 另发现同族向量: recv
循环读到 WouldBlock 才停, read_buf 可在一轮事件内无界增长.

## 2. 修复 (三层)

1. **逻辑包上限** `MAX_PACKET_TOTAL = 64MB` (MySQL 默认 max_allowed_packet,
   覆盖合法大 bulk INSERT): read_packet_full 累积超限返回
   `PacketRead::TooLarge`, 调用方回 **ERR 1153 (ER_NET_PACKET_TOO_LARGE)
   + 断连**. (Option 返回值改 Result — 区分"半包"与"超限".)
2. **recv 单轮上限**: read_buf ≥ 64MB+64KB 时停止本轮读取 (剩余数据留待
   后续事件轮次, process 先消费).
3. **停滞超限流拒绝**: read_packet_full 返回 Incomplete 但未消费缓冲区已
   超上限 → 该逻辑包必然超限 (续片永不到齐), 立即 ERR+断连 — 否则 recv
   上限与"解不出包"之间会**空转** (自查发现的次级bug).

实现参数化 (cap/full 可注入) 使单测无需 64MB 分配.

## 3. 测试与验证 (全部实测)

| 验证 | 结果 |
|---|---|
| 单测: 累积未超限 | ✅ 3 满包+短包完整重组 |
| 单测: 累积超限 | ✅ 第 5 个满包时 TooLarge; 贴近 cap 放行 |
| 单测: C5 既有 16MB+1000 边界 round-trip | ✅ 不受影响 |
| e2e: 65MB 完整超大包 | ✅ ERR 1153 + 断连 |
| e2e: 停滞超限流 (64MB+半包) | ✅ ERR 1153 (~4s); **修复前客户端干等 30s 超时失败** (服务端无响应) |
| `cargo test -p network` (串行) | ✅ 221 passed / 0 failed |
| clippy `-D warnings` / fmt | ✅ 0 / 0 |

注: <64MB 的包行为与修复前完全一致 (cap 检查是唯一行为变化); 协议级
>16MB round-trip 由既有 C5 单测覆盖.
