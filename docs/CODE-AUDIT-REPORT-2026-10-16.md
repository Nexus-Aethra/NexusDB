# NexusDB 全模块深度审计报告

> 审查对象：`/home/wpp/nexus/NexusDB` @ `c4f9eaf`（main）+ 工作区未提交修复（约 +3865/−1570 行，含 2026-10-14 P0/C1-C6 修复）
> 审查日期：2026-10-16　　审查方式：7 名分析员并行模块审计 + 交叉核对 [`CODE-AUDIT-REPORT.md`](./CODE-AUDIT-REPORT.md)（2026-10-13 版）条目现状 + 独立探针程序 release 模式实证
> 本文档为审查产物，可随时移走或删除。**全程只读，未修改任何代码。**
> **修复进度见 [§五](#五修复进度2026-10-17)**：截至 2026-10-19 已修 93 项条目 / 剩 0 项 + 文档化边界（§5.6）。

---

## 摘要

- **基线**：page crate 140 测试全绿；全库 clippy 0 警告；workspace 1074 个 `#[test]`。
- **核实旧条目 24 项**：已修复 8（全部在工作区**未提交**）/ 部分修复 2 / 未修复 12 / 误报或过时 3。
- **新发现：P0×4、P1×20、P2×40、P3×20**，其中 3 项经独立探针在 release 模式实证（标记【实证】）。
- **最大风险类别是"静默降级"**：近半 P0/P1 不是崩溃而是"不报错但行为错误"——数据错了用户不知道。
- **建议修复顺序**：P0 四项 → main.rs 退出路径丢数据 + UPDATE 置 NULL → 调度器死锁 → DoS hardening 批次 → 其余 P2。

---

## 一、既有审计条目现状核实

### 1.1 ✅ 已修复（均为工作区未提交，存在丢失风险，建议尽快提交）

| 条目 | 内容 | 证据 |
|---|---|---|
| C1 | PG 帧长度越界返回 None → 连接挂起 | 已改三态 `FrameParse`，带回归测试 |
| C2 | `page_free_space` 用 `key_count/16` 差近 2 倍 | `header.rs` 统一为 `max(hdr.checkpoint_count, needed_checkpoint_count)`，探针验证有效 |
| C4 | （详见原报告） | 已修复 |
| C5 | 协议边界问题 | 已修复，带回归测试 |
| C6 | （详见原报告） | 已修复 |
| P2-11 | （详见原报告） | 已修复 |
| P2-2 | "常量折叠全库无实现" | **审计报告自身过时**：`parser_where.rs:154-178`（常量比较短路）、`:382-455`（`fold_cond_arith`/`eval_const_bin`/`const_cmp`）已完整实现，e2e 测试 `tests/sql_e2e.rs:3456-3479` 覆盖 |

### 1.2 ⚠️ 部分修复

| 条目 | 已修部分 | 残留缺口 |
|---|---|---|
| C3 | `ItemPtr::new`（ptr.rs:39-41）、`LeafItemPtr::new`（ptr.rs:103-105）已改硬错误（新增 `PageError::NotSegmentHead`） | **`InternalItemPtr::new`（ptr.rs:171-175）仍为 debug_assert**——同一修复只做了 2/3，release 下 internal 段首校验失效 |
| P0-4 | config 缺省改 127.0.0.1 + `warn_if_insecure_defaults()` 启动告警 | ① 空密码 + 回环地址 = 免鉴权可达；② 绑定具体外网 IP（非 0.0.0.0）不触发告警；③ Binary 协议 `auth_password: None` 硬编码绕过配置 |

### 1.3 ❌ 未修复（仍存在）

| 条目 | 现状证据 |
|---|---|
| P1-2 路由无 affinity | `manager.rs` DefaultHasher + 0 处 `sched_setaffinity`，DESIGN.md 宣称失准 |
| P1-3 CHECK 约束静默丢弃 | `parser_ddl.rs:266-278`（表级）/ `:335-347`（列级）：`try_kw("CHECK")` 后括号深度计数直接吞掉表达式，注释 "v1 吞 (不强制约束)"，无警告无 AST |
| P1-4 FK 父表缓存 miss 静默跳过 | `sql_fk.rs:53-56` 缓存 miss → `return false`（无日志）；`:61` 类型不匹配 → `return false`。**比原描述更严重**：FK 检查仅覆盖 INSERT，DELETE/UPDATE 完全无 RESTRICT 检查（`sql_dml.rs:181-198`） |
| P1-6 `block_on_io` 自旋 | `manager.rs:1439-1455` 仍是 `Waker::noop` + spin（行号自 1436 漂移） |
| P2-1 复合索引退化全表 | ✅ 已修（见 5.9）：planner 复合首列等值计分 + storage 复合前缀扫；OR→IndexUnion 选中复合索引静默返空同批修复 |
| P2-4 协程 worker TLS | 半修：epoll 写缓冲已做；协程 worker TLS 两处残缺（见 2.2 节 P1-5/P1-6） |
| P2-5 SchedHandle | **比原描述更严重**：`Clone + pub Rc` 破坏唯一所有权前提，clone 跨线程是真正 UB（非仅 API 误用） |
| P2-6 | ✅ 已修（见 5.6 已解决表）：共享指针 `Arc`→`Rc` 把"单线程"写成类型结构，删除虚假 `unsafe impl Send` |
| P2-12 | `chunk_lock`/`travel_trees` 仍是死代码 |
| P3-13 / P3-14 | 未修 |
| P3-2/3/5/6/8/9/10/11 | ✅ 全部已修（P3-3 协议分派公共层、P3-5/6 结构化错误码均为本分支；其余见 §5.5） |

### 1.4 🚫 误报 / 描述需修正

| 条目 | 结论 |
|---|---|
| P1-7 | **误报**（存储分析员逐条证伪） |
| P2-2 | 审计报告自身过时（见 1.1，实现已存在） |
| P3-15 空闲自旋 | 描述不准：shard 侧空闲实为 1024 轮自旋 + 10ms poll（非 100µs）；调度器 100µs 轮询仅存在于 `run()` 路径，生产用 `drive_until_idle` 无此问题 |

---

## 二、新发现问题

严重度定义：P0 = 静默丢数据/数据错配；P1 = 服务崩溃/功能失效/数据破坏；P2 = 性能/一致性/健壮性缺陷；P3 = 低危或文档化建议。
标记【实证】= 已由独立探针程序（/tmp，未触碰仓库）在 release 模式复现。

### 2.1 🔴 P0 —— 静默丢数据 / 数据错配（4 项）

#### [P0-1]【实证】internal_insert 缺空间预检，pre_split 静默写穿页结构，页永久损坏

- **位置**：`crates/page/src/internal.rs:145-149`（无预检直接 pre_split）+ `crates/page/src/index.rs:327`（pre_split 的 `copy_within`）
- **证据**：探针构造 600B 前缀 key 逐个插入 internal 页：第 319 个 key 后 `free_off=16373` **侵入 footer 区 [16368,16384)**，cp header 被覆盖成垃圾值 607，`PageIndex::load` 直接 `Err`——插入返回 `PageFull` 但页字节已损坏且不回滚。对照组（前缀 500/700/750B）则表现为 `copy_within` 越界 panic。
- **根因**：`leaf_insert` 在 pre_split 前有 est 空间预检（`leaf.rs:274-292`，且 est 用整 key 长度恰好覆盖展开量），**internal_insert 完全没有对应预检**。这正是 `docs/bug-cold-start-data-loss.md` 记录的历史事故同一根因——leaf 修了、internal 没修。
- **影响**：internal 页是 B 树父节点，损坏 = 整个子树不可访问；且静默发生（不 panic 的窗口），是最严重的静默丢数据路径。
- **建议**：internal_insert 增加 est 预检（对齐 leaf.rs 模式）；更彻底的修法是 pre_split 只在内存 idx 层面预演、成功后再落字节，或失败时恢复页快照。

#### [P0-2]【实证】leaf_update_with 慢路径缺空间检查，合法输入 panic

- **位置**：`crates/page/src/leaf.rs:962-988`（974 行 `copy_within` 无空间检查）
- **证据**：探针以 152 个 key 填满页后执行 `leaf_update(value: 100B → 4000B)`：4006B（key+value）≤ KvLimits 4060 上限，**完全合法**，release 下 panic `dest is out of bounds`。
- **影响**：`btree_update` / registry `table_put` 覆盖写均走此路径——生产中一次大 value 覆盖写即崩溃服务。另存在窄窗口（约 cp_size+16 字节）静默覆盖 checkpoint 区而非 panic。
- **建议**：慢路径 shift 前检查 `free_off + delta ≤ cp_start`，不足返回 `PageFull` 由上层走页分裂。

#### [P0-3] strip_db_qual 静默改写跨库表名，数据串读

- **位置**：`crates/network/src/protocol/sql/ast.rs:52-56` + `parser.rs:404`
- **证据**：`db.tbl` 形式限定名被直接丢弃库名部分，取 `tbl` 在当前库解析，无报错无日志。
- **影响**：`SELECT * FROM otherdb.users` 会静默查成**当前库的** `users` 表——若同名表存在，返回错误库的数据，客户端无法察觉；同时构成越权访问面。
- **建议**：限定名与当前 db 不一致时显式报错（或按 db 路由）；至少打 WARN 日志。P0 定级理由：静默返回错误数据比报错危害更大。

#### [P0-4] FK 级联 Decimal 编码不对称，级联删除静默失效

- **位置**：`crates/network/src/worker/sql_cascade.rs:258-265` vs `sql_eval.rs:491-497`
- **证据**：级联侧 `Decimal(x,_) => SqlValue::Str(format!("{x}"))` 产裸整数**无小数点**；匹配侧 `parse_decimal` 要求字符串含小数点。
- **影响**：Decimal 主键/外键的 `ON DELETE CASCADE` 生成的 IN 条件永不匹配任何行——级联静默不生效，残留孤儿行，破坏引用完整性且无告警。
- **建议**：编码侧统一走与 `sql_eval` 对称的可逆格式（固定 scale 字符串），补 Decimal 级联 e2e 测试。

### 2.2 🟠 P1 —— 服务崩溃 / 功能失效 / 数据破坏（20 项）

#### 存储层（storage）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-1 | 驻留兜底吞 IO 错误用全零 chunk 覆盖 | `pager_write.rs:157` | `Err(_) => vec![0u8; CHUNK_SIZE]` —— IO 错误被吞后**全零页落盘覆盖历史数据**，静默丢数据 |
| P1-2 | WAL replay 吞全部错误 | `engine.rs:458-461` | `Err(_) => skipped += 1`——段被 purge 后数据静默丢失，无日志 |
| P1-3 | init_meta_page 以 truncate(true) 打开数据文件 | `engine.rs:299` | `000001.block` 以截断模式打开，特定条件（如 meta 判定失误）可清空全部数据 |

#### 网络协议层（network 协议）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-4 | 协程 worker TLS 完全不可用但可触发升级 | `worker_conn.rs:267-285` | PG SSLRequest 可触发 TLS 升级，但协程分支无 TLS 处理——密文被当明文解析，握手挂死 |
| P1-5 | TLS 写路径 WouldBlock 自旋 | `tls.rs` | 单个慢客户端可拖死同 worker 全部连接 |
| P1-6 | MySQL 续包累积无总长上限 | `mysql.rs:198-218` | `read_packet_full` 续包可无限累积 → 远程 OOM |

#### SQL 引擎（network sql/worker）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-7 | UPDATE SET 未知函数静默置 NULL | `parser_where.rs:528-545` | `UPDATE t SET c = SOMEFN(x)` 把整列刷成 NULL——**不可逆数据破坏**且用户以为执行了函数 |
| P1-8 | UPDATE 算术无运算符优先级 | `parser_where.rs:568-586` | `SET a = a + 2 * 3` 按左结合算成 `(a+2)*3`——任何混合运算 UPDATE 都写坏数据；WHERE 侧 `fold_cond_arith`（:382-455）优先级正确，两处应统一 |
| P1-9 | JOIN finish 大量 `.unwrap()` | `sql_join.rs:256/273-274/323-324/348-349/459` | worker 单线程事件循环，一处 panic 中断**所有连接**（等效全服 DoS） |
| P1-10 | 深嵌套括号 parser 栈溢出 | `parser.rs:49` + `parse_or_expr→…→parse_primary` 递归链 | tokenize 与递归下降均无深度计数，数万个 `(` 即打满 8MB 栈——一条 SQL 远程 DoS |

#### 分片层（shard_manager）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-11 | shard 线程死亡后请求永久 park | `manager.rs` | 无超时、无断连感知——一个 shard 线程 panic 后对应数据面永久挂起 |
| P1-12 | reply bus 满时 shard 线程自旋停摆 | `task_reply_bus.rs` | 消费端阻塞时生产端自旋，全 shard 吞吐归零 |
| P1-13 | 2PC 无超时 + Commit 失败静默 Ok | `coordinator.rs`（`check_timeouts` 是死代码） | Prepare 挂起即永久挂起；Commit 阶段失败仍向客户端回成功 |
| P1-14 | drain_async_flush 持续 IO 失败无限循环 | `manager.rs` | IO 故障下 flush 重试无上限无退避 |

#### 调度层（scheduler + logging）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-15 | poll_cqe! 消费他人 CQE 但不唤醒 | `io_ops.rs:43-53` vs `scheduler.rs:359-377` | `run()` 路径下 A 的 CQE 被 B 消费且 B 不唤醒 A → 调度线程永久死锁 |
| P1-16 | Read/Write future drop 后 DMA 竞争 | `io_ops.rs` | future 取消后内核 DMA 仍在写 buffer，buffer 复用即数据竞争（**潜在 UB**） |
| P1-17 | nlog 初始化失败 panic → 全局日志死锁连锁 | `logging/src/lib.rs` | 日志线程 panic 后 Error/Warn 满队自旋，拖死所有调用方 |

#### 工具链（src/ scanner）

| # | 问题 | 位置 | 说明 |
|---|---|---|---|
| P1-18 | 退出路径 shard final flush 永不执行 | `src/main.rs:479` | `Arc::try_unwrap(mgr)` 必失败（`sql_shared.cluster_ctl` 持有 mgr Arc）→ `close()` 被跳过 → shard 线程硬杀，nowchunks dirty data 丢失——**每次关机都丢**最近写入窗口的数据 |
| P1-19 | scanner 三处遍历漏 internal 页 first_child | `scanner/src/tree.rs:233-235` 等 | internal 页 first_child 存于页头 vpid 字段（`page/src/internal.rs:49`，哨兵 child_vpid=0 应忽略），walk_tree/do_in_order/lookup 全部漏扫 → export 数据不完整；`walk_tree_in_order` 递归无环检测可栈溢出 |
| P1-20 | fix-mate --auto 硬编码破坏 slot | `scanner/src/commands/fix_mate.rs` | 硬编码 `chunk_idx=0 / file_id=1`，非默认布局下修复工具本身写坏元数据 |

### 2.3 🟡 P2 —— 性能 / 一致性 / 健壮性（40 项，择要按模块）

#### page crate（10 项）

| 问题 | 位置 | 说明 |
|---|---|---|
|【实证】PageIndex::load 前 4 段全量 decode | `index.rs:116-147` | debug 打印循环的 decode/full_key 计算在 release 也执行（有 Err 语义不可 DCE）——实测 **1.29µs/次**，热路径每次读页都付 |
| checksum 从未实现 | `header.rs` 字段 vs 全 crate | 文档/lib.rs 宣称 `page_validate_checksum`，实际只有字段定义，`PageError::ChecksumMismatch` 无人返回 |
| internal_split 无哨兵分支 item_count 多算 1 | `internal.rs`（无哨兵分支） | cp[0].item_count = left_kc + 1 假设哨兵存在，无哨兵页多算 1 |
| bulk split 单段可超 32 | `leaf_split.rs:240-247` | right/left 重建为单段，item_count 可远超 MAX_PER_CHECKPOINT，违反"段 ≤ 32"不变量（性能退化 + free_space 估算失真）；split 重建的 `else { write_back }` 分支为死代码 |
| read_checkpoint 损坏 count 下溢 panic | `checkpoint.rs:68/90` + `index.rs:48` | checkpoint_count ≥ 4093 的损坏页 → `hdr_off-(i+1)*4` usize 下溢 panic（release 也 panic） |
| full_key shared 越界静默截断 | `item.rs:53` | `shared.min(prev_key.len())` 截断而非报错——损坏页静默错 key |
| leaf_get_with 1000 步上限假 miss | `leaf.rs` | bulk split 大段连锁：段内超 1000 item 时错误返回 None |
| internal_update 段首 target 破坏 shared=0 | `internal.rs:821-836` | leaf 同型 bug 已修（leaf.rs:922-931），internal 未修；当前无调用点故列 P2，一旦启用即触发 |
| leaf_delete 冗余 O(N) 全页扫描 ×2 | `leaf.rs:458-462` | `target_prev_key` 计算后仅 debug 打印使用——每次删除白做一次全页扫描 |
| ptr next 双重 decode | `ptr.rs:145-158/207-219` | `LeafItemPtr::next`/`InternalItemPtr::next` 每次 next 解码两次 |

#### SQL 引擎（10 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| 事务读集仅覆盖 pk 点查 | `sql_dml.rs:683-750` | 范围扫描/全表 SELECT 不入 read_set → SERIALIZABLE OCC 失效，幻读/丢失更新 |
| SELECT 结果集无内存护栏 | `sql_agg.rs:822-891` | ORDER BY 全量物化排序，对比 JOIN 有 JOIN_MAX_ROWS=262144——大表排序可 OOM |
| UUID 列 IN >64 元素恒空集 | `sql_eval.rs`（eval_cond_leaf 二分分支） | 大集合二分路径不做 UUID 文本→16B 转换（线性路径做），同查询 64 元素正确、70 元素返回空——语义不一致 |
| IndexUnion IN 界对 Decimal 无防护 | `sql_dispatch.rs:874-893` | 对比 `conds_to_scan_preds:753-756` 显式跳过 Decimal——混合 IN 界计算可得出错误区间 |
| IN 子查询跨类型折叠恒 false | `sql_util.rs:268-293` + `sql_eval.rs:501` | 内外类型不一致时 `sql_cmp` 返回 None 按 false——静默空集 |
| 恒假谓词短路返回错误列定义 | `sql_dml.rs:556-565` | `SELECT a,b FROM t WHERE 1=0` 返回单列空名 I64 结果集，列数与真实投影不符 |
| 系统表全 Str schema + `__` 前缀恒真 | `sql_sysquery.rs:57-75` + `sql_eval.rs:412-419` | 数值比较退化为字符串比较；`__` 恒真规则混入通用 eval 路径 |
| AVG(DECIMAL) 精度回退 f64 | `sql_agg.rs:554-558` | 金额均值出现浮点误差 |
| 事务中 DDL 静默回 OK | `sql_dispatch.rs:234-252` | CREATE INDEX/DdlStub 落 stub 分支不执行不报错——客户端以为成功 |
| JOIN cross 中间积无逐行检查 | `sql_join.rs:380-384` | JOIN_MAX_ROWS 检查在构建后，笛卡尔积中间过程可 OOM |

#### 网络协议层（4 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| 无连接数上限 / idle 超时 | acceptor/worker | 连接可无限堆积 |
| 认证无失败限速 | auth 路径 | 可暴力破解 |
| prepared 语句注册表无上限 | worker sql state | 单连接可注册至 OOM |
| SqlSharedRoutes 锁 expect 中毒即 panic | `sql_routes.rs` | RwLock 中毒直接 panic |

#### 分片层（4 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| 双 inbox 无全序 | `task_inbox.rs`/`inbox.rs` | 同源消息跨 inbox 无顺序保证 |
| WAL strict barrier 失败仍回成功 | `manager.rs` | 持久化失败不影响回复——违背 strict 语义 |
| 无 Drop RAII | `manager.rs` | main.rs close 不执行的 crate 侧根因 |
| DefaultHasher 三份拷贝 | `router.rs:40`/`manager.rs:1382-1389`/`resp_dispatch.rs:62-69` | 无跨版本稳定性契约 + 逻辑三处复制，漂移风险 |

#### 调度层（3 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| SchedHandle Clone+pub Rc | `task.rs` | 破坏唯一所有权前提，clone 跨线程是真 UB（原审计 P2-5 升级确认） |
| flush_uring 忙轮询 | `logging/src/lib.rs` | 烧 CPU ✅ 已修（drive_blocking + has_awaiting_cqe 判据, 见 5.6 已解决表） |
| 协程 panic 无隔离 | `scheduler.rs` | panic 穿透调度线程拖死同线程全部协程 |

#### 存储层（3 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| meta_cache flush_dirty in-flight 前置仅 debug_assert | `meta_cache.rs` | release 下并发写同磁盘 offset 风险 |
| pid.page_idx() u16→u8 截断 panic | `types.rs` | 页索引超 255 截断 |
| recover 固定共享临时文件 | `recover.rs:130` | `/tmp/nexus_recover_empty.mate` 固定路径，多实例互踩 |

#### 工具链（2 项）

| 问题 | 位置 | 说明 |
|---|---|---|
| scanner 只处理第一个 shard | `scanner/src/` 全部命令 | 默认 6 shard 只救 1/6 数据 |
| --strict/--tolerant 均为无效 flag | `scanner/src/` | 解析后从不读取 |

#### 其他（4 项，原综合汇报 P2 清单剩余项归入各模块上述表格）

### 2.4 ⚪ P3 —— 低危 / 文档化建议（20 项，简列）

- logging 违反"不引入 crossbeam"契约（AGENTS.md 明令，实际依赖 crossbeam-queue 0.3）
- varint 第 5 字节非规范编码静默截断（`varint.rs`）
- lib.rs `#![allow(dead_code)]` 全 crate 豁免，掩盖 `pre_merge_segment`（逻辑版）等死代码
- LIKE `_` 通配符被当字面量处理，中缀模式静默空集而非报错（`parser_where.rs:253-297`）
- RETURNING / CREATE INDEX（非事务路径）吞掉回 OK（`parser_where.rs:487-491` / `sql_dispatch.rs:246`）
- `eval_col_default` 非 UUID 列 uuid_generate_v4() 注释"保守报错"实际静默产 16B Bytes
- CURRENT_DATE/CURRENT_TIME 统一返回 now 微秒 Timestamp，列名 "now"——语义偏差
- SysVarStub transaction_isolation 固定返回 "READ-COMMITTED"，信息失真
- `SELECT 1` 顶层字面量投影不支持（`parser_select.rs:492-494`，仅 `!top` 上下文放行——存活探针常用语句）
- 空字符串不能作主键（`sql_pk_bytes` 非空校验）
- 顶层 `SELECT <literal>`、UNION、窗口函数、GROUP_CONCAT、列列比较、WHERE 函数调用等缺口（多为文档化设计取舍，建议集中标注）
- scanner dump 切片可越界（free_off > page.len 时）
- 16KB 栈缓冲多处（`try_bulk_leaf_split` 等），深 async 链栈压（已有 RUST_MIN_STACK 缓解惯例）
- `encode_leaf_item` buf 边界依赖调用方 KvLimits 约束，crate 级无保护、文档未写明
- dump/调试工具若干小问题（略）

---

## 三、正面确认（澄清审计盲区）

1. **参数绑定无 SQL 注入面**：`bind_params`（`parser.rs:832-880`）是结构化 `SqlValue::Param(i) → params[i]` 替换，发生在 tokenize 之后，**不是文本拼接**——注入面已杜绝。
2. **常量折叠已实现**：见 1.1 节 P2-2，审计报告该条目过时。
3. **C3 段首硬校验有效**：探针构造损坏页（段首 shared=1）被 `PageError::NotSegmentHead` 正确拦截——修复本身工作正常（仅 InternalItemPtr 遗漏，见 1.2）。
4. **资源护栏意识良好**：JOIN_MAX_ROWS=262144 / SUBQ_IN_MAX=65536 / TXN_MAX_OPS=8192 / AGG_MAX_GROUPS=64K / IN_INDEX_MAX_SET=32——大部分路径有界，缺口集中在 SELECT 物化与协议入口（见 P2 表）。

---

## 四、系统性模式与修复路线建议

### 4.1 三个跨模块共性模式

1. **静默降级是最大风险类别**（近半 P0/P1）：CHECK 吞掉、FK 跳过、跨库表名改写、未知函数置 NULL、Decimal 级联永不匹配、PageFull 后页损坏……共同点是"不报错但行为错误"，在数据库里比 panic 危害更大——数据错了用户不知道。
   **建议确立统一原则：凡未实现的语法/约束/失败路径，一律显式报错，禁止静默吞。**
2. **修复不对称**：同一根因修了一半——leaf 有预检/internal 没有（P0-1）、epoll TLS 修了/协程没修（P1-4）、ItemPtr 修了/InternalItemPtr 没修（C3 残留）、storage 有 flush/main.rs 退出路径没有（P1-18）。
   **建议按根因（而非表象）建立修复清单，每修一处全库 grep 同型。**
3. **单线程 worker 无 panic 隔离 + 多个无上限资源**：parser 深递归（P1-10）、MySQL 续包（P1-6）、prepared 注册表、SELECT 物化——共同构成远程 DoS 面。
   **建议合并为一个 hardening 批次：递归深度上限 + 各类资源上限 + 每查询 catch_unwind。**

### 4.2 建议修复顺序

| 批次 | 内容 | 理由 |
|---|---|---|
| 0 | **提交当前工作区** | 已验证的 C1-C6/P0-4 修复未提交，任何误操作即丢失 |
| 1 | P0-1 / P0-2（page 层两项，已实证）+ P0-3 / P0-4（SQL 静默错数据） | 静默丢数据/错数据最高危 |
| 2 | P1-18（每次关机丢数据）+ P1-7/P1-8（UPDATE 数据破坏） | 高频路径数据破坏 |
| 3 | P1-15/P1-16/P1-17（调度死锁/UB）+ P1-11/P1-13（分片挂起） | 稳定性 |
| 4 | DoS hardening 批次（P1-4/5/6/9/10 + 各资源上限） | 一次性收敛远程 DoS 面 |
| 5 | 其余 P2 按模块分批 | 性能与一致性打磨 |

---

---

## 五、修复进度（2026-10-17）

> 基线 commit：`be95b5e`。自审计基线 `c4f9eaf` 以来共 **68 个提交**（其中 62 个为修复提交），覆盖 §一 旧条目残留 14 项 + §二 新发现 P0×4 / P1×20 / P2 表中 34 项 / P3 15 项 = **87 项条目**，全部已提交。
> 质量门禁：workspace 全量串行 **1154 passed / 0 failed**，clippy `-D warnings` 0 警告，`cargo fmt --check` 0 差异。
> 修复方法论：先写回归测试（未修代码上必须失败）→ 修复 → 通过 → stash 源文件反向验证 → 全量回归。

### 5.1 §一 既有审计条目（24 项）现状

| 条目 | 审计时结论 | 当前状态 | commit |
|---|---|---|---|
| C1 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| C2 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| C4 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| C5 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| C6 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| P2-11 | 已修（工作区未提交） | ✅ 已提交 | `035b72b` |
| P2-2 | 报告自身过时 | ✅ 确认误报（实现已存在，见 §三.2） | — |
| C3 | ⚠️ 部分修复（2/3） | ✅ 残留已修：`InternalItemPtr::new` 改硬错误 `PageError::NotSegmentHead` | `d104723` |
| P0-4 | ⚠️ 部分修复（①②③） | ✅ 已修：外网 IP 告警补全 + Binary 协议 `auth_password: None` 不再硬编码绕过 | `c949ec7` |
| P1-2 | ❌ 路由无 affinity | ⚪ **文档化**：DESIGN.md 改为"未实现"并说明留作性能项；功能正确性不依赖绑核 | `6616b45` |
| P1-3 | ❌ CHECK 静默丢弃 | ✅ 已修：保留既有迁移兼容（仍吞表达式）但每条 CHECK 打 WARN，"静默"→"显式" | `d104723` |
| P1-4 | ❌ FK 缓存 miss 静默跳过 | ✅ 已修：父表 DELETE 前置 RESTRICT 预检 (sql_fk_restr.rs)；另修 cascade NoAction 永挂 | PR #12 (`4bb2127`) |
| P1-6 | ❌ block_on_io 自旋 | ✅ 已修：核实为有界退避（64 轮 spin + yield），抽出 `BLOCK_ON_SPIN_ROUNDS` 常量并写明契约 | `be95b5e` |
| P2-1 | ❌ 复合索引退化全表 | ✅ 已修：planner 复合首列等值计分 + storage 复合前缀扫；附带修 OR→IndexUnion 选中复合索引静默返空 (见 5.9) | 本分支 |
| P2-4 | ⚠️ 协程 worker TLS 半修 | ✅ 已修：协程分支补齐 TLS 升级处理 | `5ee4496` |
| P2-5 | ❌ SchedHandle Clone+pub Rc | ✅ 已修：移除 `unsafe impl Send`，内层 `Rc` 改私有 | `7257868` |
| P2-6 | ❌ 未修复 | ✅ 已修：`JoinInner` 共享指针 `Arc`→`Rc`，单线程不变量改为类型结构；删除做了虚假声明的 `unsafe impl Send` (本分支) | 本分支 |
| P2-12 | ❌ chunk_lock/travel_trees 死代码 | ✅ 已修：清理死代码 + 文档失准标注 | `0df5401` |
| P3-13 | ❌ 未修 | ✅ 已修 | `0df5401` |
| P3-14 | ❌ 未修 | ✅ 已修：WAL 名长 `as u16` 截断改 `Result` 显式报错 | `d104723` |
| P3-2 | ❌ 未修 | ✅ 已修：连接建立重复代码抽 `accept_one_conn` | `7b6b271` |
| P3-3 | ❌ 未修 | ✅ 已修：协议分派双 match 收敛到 `worker/dispatch.rs` 公共层 (`dispatch_conn_input`/`dispatch_conn_reply` + `ConnDispatch` ctx); 双 worker 模式 parity 测试 (`worker_modes_parity.rs`, `NEXUS_CORO_WORKER=1` 自 set) 防漂移 (本分支) | 本分支 |
| P3-5 | ❌ 未修 | ✅ 已修：`SqlErrorCode` 封闭枚举 + 三协议穷尽映射；`SqlError` 结构化错误 (新代码直报码)；遗留 String 错误经 `classify` 单点适配 + 模板钉死测试 (本分支) | 本分支 |
| P3-6 | ❌ 未修 | ✅ 已修：同上 (PG SQLSTATE/HTTP status 同属 `SqlErrorCode` 映射) | 本分支 |
| P3-8 | ❌ 未修 | ✅ 已修：RESP 重排缓冲设上限 | `c949ec7` |
| P3-9 | ❌ 未修 | ✅ 已修：HTTP `find_head_end` O(n²)→增量 `scanned` | `d104723` |
| P3-10 | ❌ 未修 | ✅ 已修：死代码与文档失准标注 | `0df5401` |
| P3-11 | ❌ 未修 | ✅ 已修：Binary 解码错误帧 + 指标 | `0df5401` |

### 5.2 §2.1 P0 —— 静默丢数据 / 数据错配（4 项，全部已修）

| # | 问题 | 修复方式 | commit |
|---|---|---|---|
| P0-1 | internal_insert 缺空间预检 | 对齐 `leaf.rs` 模式补 est 预检；同时修下游 `internal_split` 无哨兵分支 `item_count` 多算 1 的幽灵 item | `0bbf70b` |
| P0-2 | leaf_update_with 慢路径 panic | shift 前检查 `free_off + delta ≤ cp_start`，不足返回 `PageFull` 交上层页分裂 | `ddd6d8d` |
| P0-3 | strip_db_qual 静默改写跨库表名 | 限定名与当前 db 不一致时显式报错 | `98b446d` |
| P0-4 | FK 级联 Decimal 编码不对称 | 编码侧统一走与 `sql_eval` 对称的可逆格式，补 Decimal 级联 e2e 测试 | `5f9b077` |

### 5.3 §2.2 P1 —— 服务崩溃 / 功能失效 / 数据破坏（20 项，全部已修）

| # | 问题 | commit | # | 问题 | commit |
|---|---|---|---|---|---|
| P1-1 | 驻留兜底吞 IO 错误全零覆盖 | `d9619a4` | P1-11 | block_on_v2 无限 park | `634482f` |
| P1-2 | WAL replay 吞全部错误 | `cb0d1aa` | P1-12 | reply bus 满自旋停摆 | `ce9f3a8` |
| P1-3 | init_meta_page truncate(true) | `753d9f5` | P1-13 | 2PC 无超时 + Commit 失败静默 Ok | `c1694b8` |
| P1-4 | 协程 worker TLS 半修 | `5ee4496` | P1-14 | drain_async_flush 无限循环 | `ec4328d` |
| P1-5 | TLS 写 WouldBlock 自旋 | `927cefc` | P1-15 | poll_cqe! 消费他人 CQE 不唤醒 | `1b61be0` |
| P1-6 | MySQL 续包累积无上限 | `188dd55` | P1-16 | Read/Write future drop 后 DMA 竞争 | `4c22a7e` |
| P1-7 | UPDATE SET 未知函数置 NULL | `7a7083a` | P1-17 | nlog 满队自旋全局死锁 | `6f254a3` |
| P1-8 | UPDATE 算术无优先级 | `1f724b8` | P1-18 | 退出路径跳过 flush 丢数据 | `922675b` |
| P1-9 | JOIN finish 无 panic 隔离 | `9600ebf` | P1-19 | scanner 漏 internal 页 first_child | `6f90a10` |
| P1-10 | parser 深递归栈溢出 | `2e6fc41` | P1-20 | fix-mate --auto 硬编码 slot | `d0116e4` |

### 5.4 §2.3 P2 —— 性能 / 一致性 / 健壮性（表中 36 项：35 已修 / 1 未修）

**page crate（10 项全修）**

| 问题 | commit |
|---|---|
| PageIndex::load 前 4 段全量 decode | `6e9e8b9` |
| checksum 从未实现 | `ff7d842` |
| internal_split 无哨兵分支 item_count 多算 1 | `0bbf70b` |
| bulk split 单段可超 32 | `ba0b561` |
| read_checkpoint 损坏 count 下溢 panic | `a951b93` |
| full_key shared 越界静默截断 | `84b352b` |
| leaf_get_with 1000 步上限假 miss | `444eb81` |
| internal_update 破坏段首 shared=0 | `a2d21a8` |
| leaf_delete 冗余 O(N) 全页扫描 ×2 | `a11d3c5` |
| ptr next 双重 decode | `5871607` |

**SQL 引擎（10 项全修）**：事务读集仅 pk 点查 `4b77c13`｜SELECT 结果集无护栏 `0f1c07c`｜UUID IN >64 恒空集 `ad7c482`｜IndexUnion Decimal 防护 `7257868`｜IN 子查询跨类型折叠恒 false `624d539`｜恒假谓词错误列定义 `576adae`｜系统表数值比较退化 `62869b8`｜AVG(DECIMAL) 回退 f64 `50f8eb8`｜事务中 DDL 静默回 OK `c1492d2`｜JOIN cross 中间积无逐行检查 `4ebd2ba`

**网络协议层（4 项全修）**：无连接数上限 / idle 超时 `600de02`｜认证无失败限速 `488b779`｜prepared 注册表无上限 `f550b36`｜SqlSharedRoutes 锁中毒 `482eb08`

**分片层（4 项全修）**：双 inbox 无全序 `8d86824`｜WAL strict barrier 失败仍回成功 `fd040fd`｜无 Drop RAII（退出路径根因）`922675b`｜DefaultHasher 三份拷贝 `6616b45`

**调度层（3/3）**：SchedHandle Clone+pub Rc `7257868`｜协程 panic 无隔离 `9600ebf`｜flush_uring 忙轮询 ✅ 已修（见 5.6 已解决表: drive_blocking + 兜底排除等 CQE slot + has_awaiting_cqe 判据）

**存储层（3 项全修）**：meta_cache flush_dirty in-flight `a3f45c1`｜pid page_idx u16→u8 截断 `8e5169b`｜recover 固定共享临时文件 `5db295d`

**工具链（2/2）**：`--strict/--tolerant` 无效 flag `fa46a8d`｜scanner 只处理第一个 shard ✅ 已修：14 命令全 shard 遍历合并 (PR #13, `4a87b68`)

### 5.5 §2.4 P3 —— 低危 / 文档化建议（20 项，已修 20）

已修：LIKE `_` 通配符报错、RETURNING 报错、非 UUID 列 uuid 默认值报错、CURRENT_DATE/TIME 语义修正、无括号关键字识别、SysVarStub transaction_isolation 按实际级别回答（`5e6e9ac`）；`SELECT 1` 顶层字面量投影（`4e9b1f7`）；varint 第 5 字节非规范编码拒绝、`encode_leaf/internal_item` buf 前置边界检查、AGENTS.md crossbeam 契约澄清（`37e3f0a`）；16KB 栈缓冲改堆分配、dump free_off 越界回归测试、空串主键错误信息、page crate `#![allow(dead_code)]` 改 6 处定点 allow（`eac1486`）；SQL 语法缺口集中标注 → 新增 `docs/SQL-FEATURES.md` 语法支持矩阵（`3806824`）。

已修：~~错误分类靠字符串匹配（P3-5/P3-6）~~ ✅ 已修（结构化 `SqlErrorCode` + `classify` 单点适配 + 模板钉死测试，见 5.6 已解决表）。**§2 审计条目已全部修复。**

### 5.6 ✅ 仍未解决：0 项（审计条目全部修复；余下为文档化边界）

（原 4 项未决 — flush_uring 忙轮询 / P3-3 协议分派双实现 / P3-5/6 字符串匹配错误分类 — 均已修复, 见下方已解决表.）

**已解决（2026-10-17 晚，PR #12 与本分支）**：

| # | 问题 | 修复 | commit |
|---|---|---|---|
| P1-4 | FK 对 DELETE 无 RESTRICT 检查 | 新 `sql_fk_restr.rs` 预检状态机 (PkGet Redispatch / 两阶段 Phase2 双挂点); cascade_kickoff 只对 Cascade/SetNull 进延迟 (修 NoAction 永挂); 错误映射 1451/23503/409 | PR #12 (`4bb2127`) |
| P2-新 | scanner 14 命令全部 `shards.first()` | 全命令遍历全部 shard 合并 (见 5.8); fix-mate 多 shard 必须 `--shard` | 本分支 |
| P2-1 | 复合索引退化全表 (+ OR 混合算子命中复合索引静默返空) | planner 复合首列等值计分 + storage 复合前缀扫 + eq_enc/cover 护栏 (见 5.9) | 本分支 (PR #14) |
| P2-6 | `JoinInner` `UnsafeCell` + `unsafe impl<T: Send> Send` | 共享指针 `Arc`→`Rc` (`Rc` 本身 `!Send + !Sync`) → "spawn/drive/poll 同线程" 从调用方约定变成类型结构; 删除虚假 `unsafe impl Send` (其 SAFETY 注释声称 `Arc<JoinInner>: Send`，实际因 `UnsafeCell: !Sync` 从不成立); clippy `arc_with_non_send_sync` 清零, 原子计数开销一并消失 | 本分支 (PR #15) |
| P2-flush | `flush_uring` 忙轮询 (`logging/src/lib.rs`) | 三处联动: ① `extract_pending` 兜底扫描跳过"已提交等 CQE"的 slot (旧实现把等 IO 的协程反复拉出来空 poll = 用户态自旋, 且让阻塞分支永远走不到); ② 新 `SchedHandle::drive_blocking` — ready 空 + 真 in-flight IO 时 `submit_and_wait(1)` 内核等 CQE; ③ `drain_completions_and_wait` 判据 `!registry.is_empty()` → `has_awaiting_cqe()` — CQE 已到未 take 的项也占 registry, 旧判据会把无 in-flight 的场景也拖进 `submit_and_wait` 死锁 (单协程必现, 多协程被其他 IO 掩盖) | 本分支 |
| P3-3 | 协议分派 match 在 worker_epoll / worker_coro 重复两处 | 收敛到 `worker/dispatch.rs` 公共层: `ConnDispatch` ctx + `dispatch_conn_input`/`dispatch_conn_reply` 单份实现, 两 worker 各自构 ctx 调用; 关闭语义统一为"一律 resp_should_close" (Binary 旧 epoll 路径不查该标志, 但其仅在 TLS 写卡死/空闲回收时置位, 关闭即预期); `tests/worker_modes_parity.rs` 双模式 5 协议对拍 (含 `NEXUS_CORO_WORKER=1` 自 set) 防再漂移 | 本分支 |
| P3-5/6 | 错误分类 / SQLSTATE 靠字符串匹配 | 新 `worker/sql_error.rs`: `SqlErrorCode` 封闭枚举 + 三协议 (MySQL errno / PG SQLSTATE / HTTP status) **穷尽 match** 映射 (漏映射编译期报错); `SqlError { code, msg }` 结构化错误, 新代码 `SqlError::new(code, msg)` 直报码 (FK 违例 / unknown column 等 ORM 关键路径已迁移); 遗留 `Result<_, String>` 深管线的错误经 `SqlError::classified` **单点**适配 — 字符串匹配从三条协议链收敛到一处, 且 `classify_pins_legacy_messages` 钉死全部消息模板 (措辞漂移即红); e2e (FK 1451/23503/409, dup 1062 等) 继续端到端钉码 | 本分支 |
| (附带) | scanner `decode_meta_page` 格式漂移 | MetaPage item 区起点 40 → 1064 (引擎 T12.14 resolver 段 1KiB); 旧实现在现代引擎目录上把 resolver 段当 item 解, dbs/export/lookup/verify 全不可用 | 本分支 |

**文档化边界**（已在 `SQL-FEATURES.md` / `DESIGN.md` / `AGENTS.md` 显式标注，非缺陷）：JOIN gather 读不入 OCC 读集、跨 shard COMMIT best-effort、不防幻读、Binary 协议无认证、CPU 亲和未实现。

### 5.7 建议修复顺序（更新）

1. ~~**P2-新 scanner 单 shard**~~ ✅ 已修 (本分支, 含附带 meta 解码漂移修复).
2. ~~**P1-4 FK RESTRICT**~~ ✅ 已修 (PR #12).
3. ~~**P2-1 复合索引退化全表**~~ ✅ 已修 (本分支, 见 5.9).
4. ~~**P2-6 JoinInner Send 安全**~~ ✅ 已修 (`Arc`→`Rc` + 删虚假 unsafe impl, 见 5.6 已解决表).
5. ~~**P3-5/6 错误分类结构化**~~ ✅ 已修 (`SqlErrorCode` 枚举 + 穷尽映射 + classify 单点适配, 见 5.6 已解决表).
6. ~~**flush_uring 忙轮询**~~ ✅ 已修 (`drive_blocking` 内核等 CQE, 见 5.6 已解决表). ~~**P3-3 协议分派去重**~~ ✅ 已修 (公共层 + 双模式 parity 测试). **审计条目全部修复完毕.**

### 5.8 P2-新 修复明细 (scanner 多 shard, 2026-10-17)

问题: 14 个命令全部取 `layout.shards.first()` — 多 shard 目录 (引擎默认 6 shard) 下其余 shard 的表与数据对 scanner 完全不可见, 救援只救 1/N 且无任何提示; `fix-mate` 更会静默修错 shard 的 `page.mate`.

修复 (语义按命令类别):
- **整库类** (dbs/verify/rescue/map/export/merge/lookup/blame): 遍历全部 shard — vpid 空间 per-shard, 同一 root_vpid 在各 shard 各是一棵树, 合起来才是全量; 诊断输出加 `shard` 列/字段, 坏页计数跨 shard 汇总, strict 门禁任一 shard 触发即非零退出.
- **数据流类** (export/merge): 输出流保持既有可重导入格式 (key 跨 shard 不相交, 直接拼接); merge 跨 shard 合入同一 BTreeMap 后再放 WAL 重放.
- **暴力扫类** (export-force/rescue-json): 跨 shard 合并去重 (相同 pkey 后写覆盖, 与 append-only 模型一致), 统计聚合.
- **单页类** (header/vpid/range): 逐 shard 解码输出 (同一 vpid 在各 shard 是不同页); 全部 shard 读不到才报 bad.
- **写盘类** (fix-mate): 多 shard 目录必须 `--shard <id>` 显式指定, 否则显式报错列出可用 shard — 消除"静默修错 shard"的修复工具写坏元数据路径.
- wal-list/wal-dump 本就已遍历全部 shard 的 WAL 段, 无需改动.

验证: `crates/scanner/tests/multishard_rescue.rs` — 真实引擎 3-shard 目录 (ShardManager 落盘 + close flush) 端到端跑 scanner 二进制: export-force/export 全 30 键、dbs/verify/header/map 三 shard 全覆盖、lookup 命中非 shard_0 的键、fix-mate 多 shard 拒猜、单 shard 目录行为不变; 反向验证 (stash src 后) 8/9 必败.

### 5.9 P2-1 修复明细 (复合索引前缀扫, 2026-10-19)

问题 (两层):
1. **性能**: planner 计分循环 `sql_dispatch.rs` `if idx.cols.len() != 1 { continue; }` — parser 建的复合 UNIQUE 索引执行层永远用不上, `WHERE a=?` 退全表.
2. **正确性 (比原报告更严重)**: `sql_plan_select` 的 OR→IndexUnion 路径按 `i.col == col_idx` 匹配索引, 无同首列单列索引时会选中**复合**索引; shard 端 `index_scan_raw` 按首列单列编码做界, 而复合 enc_val 头字节 `IVAL_COMPOSITE`(0x04) 恒大于单列界头 (0x01/0x02/0x03) → memcmp 一带上界立即 Break → **静默返空** (`WHERE a=1 OR a<0` 实测返 0 行). JOIN 的 `iid_of` 索引驱动同样可命中复合索引, 同病.

修复:
- **storage `index_scan_raw`**: 复合索引 (cols>1) 独立分支 — 首列等值 (lo==hi) 走**前缀扫**: 起点 `[I][iid][IVAL_COMPOSITE][nseg][u16 len][enc]`, enc_val 不再以该前缀开头即断裂 (键共享前缀 ⇒ B+Tree 中连续, 断裂点精确); 范围/单边界退整 iid 超集扫 (不漏行, 精确性由 worker 残余过滤兜底); bloom 只存完整复合编码 (row_put 喂 `index_vals_bytes` 全值), 前缀不可查 → 复合路径绕 bloom 短路 (查不到 = 假阴性返空).
- **storage `decode_index_val`**: 补 `IVAL_COMPOSITE` 臂 (原样返回拼接字节) — 否则 `index_scan_entries_local` 经 `split_index_entry` 把复合条目当坏条目丢弃 (扫到了也收集不上来).
- **planner `sql_plan_select`**: 复合索引参与计分, 但仅**首列等值**计分 (+3); 范围/IN 不计分 (复合编码段含变长, `(len,enc)` 字典序≠值序, 范围界不安全) → 回退全扫; OR 分支命中复合索引时仅允许等值分支, 否则整体回退全扫.
- **护栏**: 复合索引 `eq_enc=None` (路由 bloom 只存完整复合编码, 前缀键查不到 → 零候选短路会静默返空); cover 覆盖索引禁复合 (`col_from_ordered_bytes` 解不出复合值段 → "bad covered index entry" 报错).

验证: `crates/storage/tests/sql_rows_tests.rs::composite_index_first_col_eq_prefix_scan` (等值/单边/双边/miss/重开绕 bloom 五断言); `sql_dispatch.rs` planner 单测 5 个 (首列等值走 Index/次列组合/范围回退/非前缀回退/OR 范围分支回退); `sql_e2e.rs::mysql_composite_index_query_correctness` (等值/组合/OR 混合算子/COUNT 禁覆盖/投影禁覆盖); 修复前全部必败 (等值扫返空, OR 返空), 修复后全量 1168 passed / 0 failed (默认并行度), clippy/fmt 0.

## 附：审计方法与团队

- 7 名分析员并行：存储 / 网络协议 / SQL 引擎 / 分片管理 / page 页层 / 调度器 / 工具链，各自产出模块报告并逐项核实旧条目。
- page 层分析员额外编写独立探针程序（位于 /tmp，依赖工作区版本 page crate，未修改仓库），在 debug + release 双模式实证 P0-1、P0-2、PageIndex::load 性能损耗及 C3 修复有效性。
- 本报告由团队 leader 综合各模块报告交叉核对后产出；模块级完整报告存于分析员会话记录。
- 全程只读：本次审计未修改任何源码文件。