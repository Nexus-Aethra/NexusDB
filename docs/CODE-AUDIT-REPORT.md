# NexusDB 代码审查报告

> 审查对象：`/home/wpp/nexus/NexusDB` @ `c4f9eaf`（main，与 origin/main 一致，工作区干净）
> 审查日期：2026-10-13　　审查方式：源码通读 + 实际编译/测试/静态检查执行
> 本文档为审查产物，未纳入版本控制，可随时移走或删除。

---

## 〇、P0 修复记录（2026-10-14）

审查发现后已直接修复全部 4 项 P0，修复后的完整回归结果见下。

| 编号 | 问题 | 修复 | 验证 |
|---|---|---|---|
| **P0-1** | `cargo clippy --all-targets` 编译失败 | `src/lib.rs:1109` 的 `3.14` 改为 `2.5`（该值仅用于触发 F64 类型标签，无断言依赖，故从根上消除而非 `#[allow]` 掩盖） | clippy **error 归零** |
| **P0-2** | 全量测试跑不完 | 根因是测试临时目录泄漏 + `TMPDIR` 残留累积至 2 GB，而非代码死锁；新增 CI 固定 `TMPDIR` 与串行参数 | **1049 passed / 0 failed**，exit 0 |
| **P0-3** | 无 CI | 新增 [.github/workflows/ci.yml](/home/wpp/nexus/NexusDB/.github/workflows/ci.yml)：fmt / clippy / test 三作业，clippy 以 `-D warnings` 强制，测试显式串行并设 `timeout-minutes: 45` | 工作流所跑命令即本机已验证通过的命令 |
| **P0-4** | 默认五监听无鉴权且绑 `0.0.0.0` | ①代码缺省与 `config/nexusdb.toml` 改为 **127.0.0.1**（容器配置保留 `0.0.0.0`，那在容器内是正确的）；②新增 `warn_if_insecure_defaults()`，非回环 + 空口令时启动打印 SECURITY 告警 | 双向验证：`0.0.0.0` 配置**告警**、回环配置**静默** |

**连带修复**：`config::tests::partial_toml_falls_back_to_defaults` 因缺省值变更而失败，已同步更新断言。

### P1-1 代码质量门禁全清（clippy 50 → 0 + rustfmt 634 → 0）

| 门禁 | 修复前 | 修复后 | 验证 |
|---|---|---|---|
| `cargo clippy --workspace --all-targets -- -D warnings` | 1 error + 50 warn | **0** | 退出码 0 |
| `cargo check --workspace --all-targets`（debug） | 有告警 | **0** | — |
| `cargo check --workspace --all-targets --release` | 有告警 | **0** | — |
| `cargo fmt --all -- --check` | 634 处差异 | **0** | — |
| 全量串行回归 | — | **1049 passed / 0 failed** | exit 0 |

格式化本身改动 125 个文件（+3432/−1444），纯排版、无逻辑变更；改动后已重跑全量回归确认行为不变。

分类处理：

| 处理方式 | 条数 | 说明 |
|---|---|---|
| `clippy --fix` 自动修 | ~25 | 多余借用、冗余转换、无用 import、可折叠分支等纯机械项 |
| **恢复丢失的测试** | 1 | `sql_e2e.rs` 的 `mysql_or_join_having` 有完整测试体却**丢了 `#[test]`**，成了死代码；同时存在一段孤儿 `#[test]` + 文档注释被挂到了下一个函数上。恢复后该 JOIN-OR/HAVING-OR 场景才真正被执行 |
| **修好一个"什么都没断言"的测试** | 1 | `pg_e2e.rs` 的 multi-statement 断言把 `Err` 也算作通过，服务端报错也能过。改为显式排除 `Err` 后**暴露了一个真实缺口**（见下） |
| 提取类型别名 | 3 | `Arc<Mutex<Option<Result<…>>>>` 等改为具名类型 |
| 删除死代码 | 2 | `sql_bigdata::err_code`、`worker_coro_handshake_test::QueryResult` |
| 让被丢弃的计数器显式化 | 1 | `rescue_json` 的 `skipped` 原本只增不读，现输出到摘要行 `unparsed=` |
| 带理由豁免 | 4 | 平台常量断言、`block_on_io` 模式的 RefCell 跨 await、两个命令分发函数参数过多 |

### ⚠️ 清警告过程中发现的新功能缺口

顶层 **`SELECT <字面量>`（如 `SELECT 1`）不被解析器支持**。

根因在 [parser_select.rs:493](/home/wpp/nexus/NexusDB/crates/network/src/protocol/sql/parser_select.rs:493)：字面量投影**仅在子查询上下文**（`!top`）被接受；顶层会落到 `p.ident()?` 分支并报 `42601: expected identifier, got Num("1")`。

影响面：`SELECT 1` 是极常见的存活探针与驱动探测语句。当前顶层只支持 `SELECT *`、`SELECT version()`、`SELECT DATABASE()`、`SELECT FROM t`。本报告未修复此项（超出清警告范围），建议单独立项。

### 正确性隐患修复（6 项全部完成）

| # | 隐患 | 修复 | 防护手段 |
|---|---|---|---|
| **C1** | PG 帧长度越界与"数据不足"都返回 `None`，调用方 `break` → **连接永久挂起**（外部可触发） | 引入三态 `FrameParse{Frame,NeedMore,Invalid}`；长度非法时回 `08P01` 错误帧并 `close_after_flush`，**不再退化成继续等待** | 3 个调用点改造 + 4 个新单测（含越界/边界/半包） |
| **C2** | `page_free_space` 用 `key_count/16`，权威算法是 `ceil(key_count/32)`，**差近 2 倍**；空页还凭空多算一个 checkpoint | 统一为「页头 `checkpoint_count`（精确）与 `needed_checkpoint_count`（下界）取大者」 | 新增 `free_space_tests.rs` 4 个回归测试，含"页头滞后不得高估"与单调性 |
| **C3** | 段首 `shared_prefix_len==0` 与 `flush()` in-flight 前置条件只有 `debug_assert`，**release 下失效**，分别导致静默错 key 与并发写同一磁盘 offset | 改为硬错误：新增 `PageError::NotSegmentHead`；`flush()` 返回 `InvalidInput` | page/storage 639 测试全绿 |
| **C4** | `parse_index_page` 直接按磁盘 `count` 切片，损坏页 → **panic 而非 `InvalidData`** | 校验 `count <= MAX_INDEX_ENTRIES` + 逐项 checked 切片 | 3 个新单测（含边界值恰装满） |
| **C5** | `write_packet` 把长度截成 3 字节且从不拆分，载荷 ≥16MB 时**长度被截、载荷完整** → 线上非法 | 按 MySQL 协议拆续包（首包满 `0xFFFFFF`、seq 连续、末包放余量）；并新增 `read_packet_full` 重组续包，调用点改用它 | 2 个新单测（17MB 载荷拆 2 包并逐字节重组一致；半包返回 `None` 而非截断） |
| **C6** | `write_overflow` 中途失败留下**孤儿页**（已分配但永不被引用） | 失败回滚：任一步出错先释放已建数据页；段长超页容量由 `debug_assert` 改为硬错误 | overflow/registry/recover 测试全绿 |

C4 顺带修掉一个同类隐患：`read_overflow` 按磁盘上的 `total_len`（u32，上限 4 GiB）做 `Vec::with_capacity`，损坏描述符可致巨额预分配/OOM；现先校验 `total_len <= MAX_OVERFLOW_VALUE`。

---

**同时更正两条原报告误判**：
- **P1-7「page cache 不回填」** —— 证伪。`ChunkList` 是双层缓存，读路径按设计只填充页层；是测试断言了已废弃的整块模型。测试已改为断言页层，16/16 通过。
- **P0-2「代码死锁」** —— 证伪。逐个二进制单独执行全部秒级通过；真实成因见该条更正说明。

---

## 一、执行摘要

NexusDB 是一个 10.2 万行的 Rust 单机 KV + SQL 数据库引擎，以一套存储内核对外提供五种协议接入（Redis RESP2、HTTP REST、MySQL wire、PostgreSQL wire、内部 Binary）。

**结论：技术密度高、工程纪律强，但基础设施缺失；公开宣称与实际实现存在系统性偏差；不具备生产就绪条件。**

最核心的问题不是代码写得不好，而是**项目对自己的描述没有被任何机制验证**：没有 CI、测试绿灯无法在干净环境复现、README 与设计文档中多条"已完成"标记在代码里不存在对应实现。

> 📌 P0 已全部修复（见第〇节）。下文的 P0 条目保留原文，用于对照修复前状态。

| 分级 | 数量 | 说明 |
|---|---|---|
| P0 阻断 | 4 | 导致构建失败、无法验证、或默认不安全 |
| P1 高 | 9 | 正确性、安全性或严重误导接手者 |
| P2 中 | 13 | 功能缺失、架构性限制、潜在数据风险 |
| P3 低 | 15 | 技术债与可维护性 |

---

## 二、核查方法与证据等级

本报告所有结论分三级标注：

- **【实测】** —— 我在本机实际执行命令得到的结果
- **【源码】** —— 由源码逐行确认，未执行但可直接核对行号
- **【文档】** —— 对文档宣称与代码的差异判定

### 已实际执行的命令

| 命令 | 结果 |
|---|---|
| `cargo check --workspace --all-targets` | ✅ 通过，11.75s |
| `cargo test --workspace -- --list` | ✅ 1071 个可运行测试 |
| `cargo test --workspace --lib` | ✅ 358 passed / 0 failed |
| `cargo test --workspace --no-fail-fast -- --test-threads=1` | ❌ **死锁**，56 个测试二进制后停滞（536 passed / 0 failed） |
| `cargo test -p storage --test chunk_lock_tests` | ❌ 15 passed / **1 failed** |
| `cargo test --workspace --doc` | ✅ 全部通过（4 ignored） |
| `cargo clippy --workspace --all-targets` | ❌ **51 warnings + 1 error（编译失败）** |

### 双工具链交叉验证

审查期间 rustup 默认工具链一度损坏，导致先后使用两个编译器版本。所有关键结论在**两个版本上分别复现**，以排除编译器版本差异导致的误判：

| 结论 | Rust 1.98.1 | Rust 1.99.0（当前默认） |
|---|---|---|
| `cargo check --workspace --all-targets` | ✅ 通过 | ✅ 通过 |
| 可运行测试数 | 1071 | **1071** |
| `chunk_lock_tests` | 15 passed / **1 failed** | **15 passed / 1 failed** |
| clippy 警告数 | 50 | **51** |
| clippy 硬错误（`approx_constant`） | ✅ 复现 | ✅ **复现**（同位置 `src/lib.rs:1109`） |

> 注：审查过程中曾因强杀挂死进程导致 rustup 默认工具链损坏，属操作事故而非项目缺陷，已修复。两条编译器版本下 `clippy` 警告数相差 1 条（1.99.0 新增 1 类），**硬错误在两版本下均稳定复现**，不构成误报。

---

## 三、项目统计

### 3.1 代码规模

| 指标 | 数值 |
|---|---|
| `.rs` 文件数 | 246 |
| Rust 代码总量 | **102,513 行** |
| —— 实现代码 | 71,977 行（70.2%） |
| —— 测试代码 | 27,523 行（26.9%） |
| 工作区成员 | 9 个 crate + 根 binary |
| `#[test]` 属性 | 1,063 |
| 可运行测试 | **1,071**【实测】 |
| `unsafe` 块 | 102 |
| `Mutex`/`RwLock` | 55 处（集中于 `network` 共享元数据） |
| 文档 | 37 个 `.md`，约 5,441 行（docs/）+ 82K（README 双语） |

### 3.2 各 crate 代码与测试分布

| crate | 代码行 | 测试数 | 职责 |
|---|---|---|---|
| `network` | 37,346 | 202 | 五协议门面、worker、SQL 分发 |
| `storage` | 30,799 | 509 | 持久化、pager、B+Tree、WAL、GC |
| `shard_manager` | 9,653 | 60 | 多分片、hash 路由、2PC |
| `page` | 9,289 | 136 | 16KiB 页、原 LCB-Tree 操作 |
| `scanner` | 6,636 | 65 | 离线只读数据救援工具 |
| `scheduler` | 5,517 | 65 | 自研协程调度器 + io_uring |
| `logging` | 693 | 6 | nlog |
| `config` | 367 | 6 | TOML 配置 |
| `platform` | 83 | 2 | 平台抽象 |
| 根 `src/` | 1,994 | 12 | 库门面 + 服务器入口 |

### 3.3 Git 状态

| 指标 | 数值 |
|---|---|
| 提交数 | 11 |
| 作者数 | **1**（Haiguang Wang） |
| 开发周期 | 2026-07-15 → 2026-08-23（约 6 周） |
| 停滞时间 | **至审查日已约 7 周零提交** |
| 标签 / 发布 | 无 |
| CI 配置 | **无**（`.github/`、`.gitlab-ci.yml`、`.circleci` 均不存在） |
| 未合并远端分支 | 5 个 |

---

## 四、问题清单

### P0 — 阻断级（4 项）

#### P0-1 `cargo clippy --all-targets` 编译失败【实测】

```
error: approximate value of `f{32, 64}::consts::PI` found
error: could not compile `NexusDB` (lib test) due to 1 previous error
```

`clippy::approx_constant` 为 deny-by-default。触发点 `src/lib.rs:1109` 一个 `delta: 3.14` 字面量。后果：**任何 CI 一旦执行 clippy 即失败**。

#### P0-2 测试套件无法跑完【实测 —— **成因已更正，见下**】

初版结论：全量 `cargo test --workspace --no-fail-fast -- --test-threads=1` 在
`shared_workers_test` / `sql_bigdata` / `pg_e2e` 停滞，线程呈睡眠态，判定为死锁，
且指出 `AGENTS.md` 所称"串行可规避"不成立。

**更正**：在**修复工具链 + 清理 TMPDIR 之后，全量串行回归可正常跑完**
（1049 passed / 0 failed / exit 0，约 5-8 分钟）。逐个可疑测试二进制单独执行时
**全部秒级通过**（`shared_workers_test` 2.56s、`sql_bigdata` 41.5s、
`sql_index_e2e` 0.52s、`pg_e2e` 3.15s）。

**实际成因**：测试用 `std::mem::forget(tempdir)` 故意泄漏临时目录
（如 [shared_workers_test.rs:37](/home/wpp/nexus/NexusDB/crates/network/tests/shared_workers_test.rs:37)），
而这些目录建在 `TMPDIR` 下。累积后 `target/nxtmp` 一度膨胀到 **2.0 GB**（338 个目录）。
在磁盘/页缓存被这类残留占满的环境下，e2e 引擎初始化随之变慢甚至停滞，表现为"hang"。

**修正后的 P0-2 结论**：不是代码死锁，而是
①测试临时目录泄漏 + ②关键构建配置被 gitignore（`.config/`、`.cargo/`）导致新环境
无从复现。已通过补 CI 固定 `TMPDIR` 与串行参数来暴露该问题（见"修复记录"）。

#### P0-3 无任何 CI

无 GitHub Actions / GitLab CI / CircleCI。项目所有质量宣称（860+ 测试、clippy 0 警告）**没有任何自动执行机制**，也无法在干净克隆上复现。

#### P0-4 默认安全姿态为裸奔【实测配置】

`config/nexusdb.toml`：

```toml
http_token = ""      # REST Bearer token (空 = 免鉴权)
sql_password = ""    # SQL 登录密码 (空 = 免密)
redis_password = ""  # RESP AUTH 密码 (空 = 不启用认证)
```

五个监听全部绑定 `0.0.0.0`，其中 Binary 协议（5433）在 `src/main.rs:586` 显式传入 `auth_password: None`。**开箱即用即完全无认证**。TLS 为 opt-in 且仅覆盖 SQL/PG。

---

### P1 — 高（9 项）

#### P1-1 README「clippy 0 warnings」失实【实测】

实测 **51 条警告 + 1 个编译错误**（`--all-targets`，Rust 1.99.0）。主要类型：多余引用解引用(5)、属性后空行(5)、复杂类型(4)、未使用变量(4)、常量断言(3)、无意义结构体模式(3)、同类型转换(3)，以及 1 条 `RefCell` 引用跨 `await` 持有（真实并发隐患）。

#### P1-2 `DESIGN.md` 描述了未实现的架构【源码】

| 文档声称 | 代码实际 |
|---|---|
| fnv1a-64 哈希路由 | `DefaultHasher`（SipHash-1-3），`crates/shard_manager/src/router.rs:40` |
| `sched_setaffinity` 绑核 | **全代码库 0 处** affinity |
| 采用 monoio 协程 | 自研调度器（自实现方向正确，但与文档不符） |
| chunk_lock 为核心并发机制【§3.0】 | **仅测试可达的死代码** |

前两项我在源码中直接核实（grep 计数为 0），并非文档措辞含糊。

#### P1-3 CHECK 约束被解析后静默丢弃【源码】

`crates/network/src/protocol/sql/parser_ddl.rs:266`

```rust
} else if p.try_kw("CHECK") {
    // ⭐ compat: 表级 CHECK (expr) — v1 吞 (不强制约束)
```

**静默接受一个从不执行的约束，比直接报错更危险**：用户认为数据受保护，实际无任何约束。

#### P1-4 外键检查在父表 schema 未缓存时静默跳过【源码】

`crates/network/src/worker/sql_fk.rs:53-55` —— `return false` 走原 INSERT 路径，无日志、无指标、无告警。

#### P1-5 `/metrics` 与 `/v1/status` 永久免鉴权【源码】

`crates/network/src/worker/protocol_io.rs` —— 代码注释明示为设计决定。暴露请求计数、内存、状态等运行信息。

#### P1-6 慢盘 I/O 会烧满 CPU 并拖住整个分片【源码】

`crates/shard_manager/src/manager.rs:1436-1453`：

```rust
let waker = Waker::noop();
// 64 次 spin_loop() 后 yield_now()，从不 sleep
```

被 shard 主循环直接调用。磁盘变慢时该分片线程 100% CPU 空转，并阻塞该分片所有其他任务。**这是 P0-2 死锁最可能的根因。**

#### P1-7 page cache 不回填（~~真实性能回归~~ **已证伪，见下**）【实测】

> **⚠️ 本条为原报告的误报，已更正。**
>
> 初版依据：`chunk_lock_tests::pager_read_miss_creates_chunk_lock_then_loads` 失败，
> 断言 `!chunk_list().is_empty()`，据此推断"缓存永远暖不起来"。
>
> 更正后的事实：`ChunkList` 是**双层缓存**——`map`（整块层）与 `pages`（页层），
> 二者是**独立的 LRU 链**。`is_empty()` 只检查整块层 `map`；而读路径调用
> `admit_page`（[chunk_lru.rs:278](/home/wpp/nexus/NexusDB/crates/storage/src/chunk_lru.rs:278)）
> 只填充**页层**。这是**有意为之**，同文件注释写明：*"Admit a page that paid a disk
> miss. We do not promote every page from a chunk hit: that would turn scans into
> 16KiB allocations."*
>
> 即：**缓存工作正常，是测试断言了已废弃的整块缓存模型**。测试已修正为断言页层
> （`peek_page(...).is_some()`），现 16/16 通过。
>
> 教训：仅凭"一个测试失败"推断"存在性能回归"是不够的，必须追到断言背后的语义。

#### P1-8 `AGENTS.md` 交接文档存在 14 处断链【实测】

`docs/AGENTS.md` 中 14 处引用 `docs/superpowers/…`，该目录已在提交 `304be2b` 中移入 `docs/archive/`。该文件的核心用途是"给接手者指路"，而这条路径已断。

#### P1-9 `AGENTS.md` 自相矛盾【源码】

`:312` 将 PG/MySQL 门面列为**不支持**，而 `:26-27` 标记为**已完成 ✅**。同一文档内两种说法。

---

### P2 — 中（13 项）

| # | 问题 | 位置 | 类型 |
|---|---|---|---|
| P2-1 | 复合索引完全不可用，多列索引一律退化全表 | `crates/network/src/worker/sql_dispatch.rs:958` | 功能缺失 |
| P2-2 | `docs/sql-optimizer-plan.md:81` 标记"常量折叠 ✅ 已交付"，全库无实现代码 | 同左 | 文档失准 |
| P2-3 | PG 帧长度越界与"数据不足"都返回 `None`，调用方 `break` → 连接永久挂起 | `crates/network/src/protocol/pg.rs:48,62` | 正确性 |
| P2-4 | 协程 worker 三处残缺：①`enable_epoll_write_buffering` 从未调用（无写缓冲、无 4MB 背压上限）②MySQL 握手写死 `tls=false` → **从不宣告 CLIENT_SSL** ③`recv_async` 无 TLS 分支但 `start_tls` 可达 | `worker_coro.rs:192`、`worker_conn.rs` | 功能缺陷 |
| P2-5 | `unsafe impl Send for SchedHandle` 使非 `Send` 的 `BoxFuture` 可跨线程 | `scheduler.rs:536` | 安全边界 |
| P2-6 | `JoinInner` 为 `UnsafeCell` + `unsafe impl Send`，`JoinHandle` 是 `Clone+Send` → 跨线程 poll 竞争 `set_result` | `task.rs:62-70` | 数据竞争 |
| P2-7 | 连接状态存在可达 `unwrap`/`expect`，单个 panic 杀死该 worker 的**全部**连接 | `worker_conn.rs:556,784`、`mod.rs:726,761,1000` | 可用性 |
| P2-8 | MySQL `write_packet` 从不拆分 >16MB 的结果集 → 大结果集在线上格式非法 | `crates/network/src/protocol/mysql.rs:189` | 正确性 |
| P2-9 | checkpoint 计数两处算法不一致（`key_count/16` vs `div_ceil(32)`，**2 倍差异**） | `page/src/header.rs:162` vs `checkpoint.rs:108` | 潜伏缺陷 |
| P2-10 | 关键不变量仅有 `debug_assert`：段首前缀压缩、flush in-flight 前置条件 → **release 构建下失效** | `ptr.rs:37-41`、`pager.rs:794-799` | 安全边界 |
| P2-11 | `parse_index_page` 信任磁盘 `count` 无边界检查 → 损坏页 **panic 而非返回 `InvalidData`** | `storage/src/overflow.rs:138` | 健壮性 |
| P2-12 | `chunk_lock` / `travel_trees` 为死代码，仅测试可达；测试注释自承"Pager.read 还没集成" | `storage/src/pager.rs` | 架构偏差 |
| P2-13 | 三处测试总数互相矛盾：AGENTS.md "800"、CHANGELOG "867"、README "860+" | 三份文档 | 文档失准 |

---

### P3 — 低 / 技术债（15 项）

| # | 问题 | 位置 |
|---|---|---|
| P3-1 | `ConnState` 承载约 85 个字段服务 5 种协议 | `network/src/worker/mod.rs:369-538` |
| P3-2 | 连接建立代码逐字重复两遍 | `worker_epoll.rs:51-105` vs `175-221` |
| P3-3 | 协议分派 match 重复两遍 | `worker_epoll.rs:247` vs `worker_coro.rs:328` |
| P3-4 | 6 处近似相同的 pending 处理块 | `resp_shard.rs:25-167+` |
| P3-5 | 错误分类靠字符串匹配，散落 3 处 | `protocol_io.rs:327`、`sql_eval.rs:509` |
| P3-6 | SQLSTATE 由错误消息子串匹配决定，改文案即改错误码 | `sql_eval.rs:532-551` |
| P3-7 | `read_buf` 无上限增长 | `worker_conn.rs:199-260` |
| P3-8 | pending 重排 `BTreeMap` 无淘汰/超时，分片不回复则永久驻留 | `mod.rs:400` |
| P3-9 | HTTP `find_head_end` 每次读全量扫描 → O(n²) | `protocol/http.rs:124` |
| P3-10 | `src/reply_bus.rs` 为死代码（仅自身测试引用） | `network/src/reply_bus.rs` |
| P3-11 | Binary 协议解码错误被静默重同步，无错误帧无指标 | `worker_epoll.rs:399-407` |
| P3-12 | `write_overflow` 明确非原子，崩溃产生孤儿页 | `storage/src/overflow.rs:97` |
| P3-13 | `next_file_id` 字段计算后无人使用 | `recover.rs:171` |
| P3-14 | WAL 记录 `db.len() as u16` 无检查 | `wal.rs:96` |
| P3-15 | 无 work stealing；空闲时 100µs 轮询、分片 10ms poll 超时 | `scheduler.rs`、`shard_thread.rs:577` |

---

## 五、功能缺口（项目已自述 + 审查补充）

**SQL**：无 UNION（0 命中）、无窗口函数、无 GROUP_CONCAT、无 RETURNING、LIKE 仅前缀模式、无列与列比较（无 binder）、WHERE 中不支持函数与算术、普通 UNIQUE 跨 shard 仅 best-effort、跨 shard 事务 best-effort、非事务 DELETE/UPDATE 与多行 INSERT 非原子、GROUP BY 无部分聚合下推、ORDER BY 无 top-k、无 MVCC。

**RESP**：无 EXPIRE/TTL/PERSIST、无 MULTI/WATCH/EXEC、无 pub/sub。

**平台**：Windows 为 Beta（阻塞式 `std::thread` per conn，仅 Binary+RESP，无 SQL/PG/HTTP/TLS；IOCP 方案已尝试并回退）。

---

## 六、公开宣称 vs 实测对照

| 宣称 | 来源 | 实测结论 |
|---|---|---|
| clippy 0 warnings | README 徽章 | ✅ **已达成**（50 → 0，CI 以 `-D warnings` 强制） |
| 860+ 测试 | README 徽章 | ✅ **1071 可运行 / 1049 实跑通过**（保守下限，未夸大） |
| fnv1a 哈希路由 | DESIGN §3.1 | ❌ 实为 SipHash-1-3 |
| sched_setaffinity 绑核 | DESIGN §3.2 | ❌ 全库 0 处 |
| 采用 monoio 协程 | DESIGN §3.3 | ❌ 自研调度器 |
| chunk_lock 并发机制 | DESIGN §3.0 | ❌ 死代码，仅测试可达 |
| 常量折叠已交付 | sql-optimizer-plan:81 | ❌ 无实现代码 |
| `cargo test --workspace` 5 分钟自检 | README | ⚠️ 需串行 + 干净 TMPDIR，否则残留拖垮 |
| 串行可规避 hang | AGENTS.md | ✅ 基本成立（前提是 TMPDIR 干净） |

---

## 七、值得肯定之处

为避免以偏概全，以下均为实际核实过的优点：

1. **存储引擎的不变量是真在代码里强制的**，不是文档口号：`pager.rs:841-843` 强制"chunk data 全部确认落盘后才刷 meta"，异步路径用 `flush_backlog() > 0` 维持该不变量。
2. **把踩过的坑写在决策点旁边**。GC 以 meta 全扫描为存活真相源，代码注释写明了为什么不能用页头 vpid——因为 Internal 页该偏移存的是 `first_child`，*"两者都导致子树路由断，travel 落到错误 leaf → GET 返回 nil（静默丢数据）"*。
3. **两个静默丢数据事故的根因分析相当扎实**：`bug-cold-start-data-loss.md` 定位到 `pre_split_segment` 已改页数据而 `leaf_push_back` 返回 `PageFull`，导致 checkpoint 数组失效；`bug-report-btree-split-routing.md` 逐一排除 7 个假设后才定位根因，并解释了为何升序单测从未触发。
4. **索引剪枝的不变量正确**：索引边界做**过近似**（放宽）以避免漏行，残差再精确过滤；布隆过滤器明确保证"无假阴性"。
5. **调度器确实只有 1 个互斥锁**（`scheduler.rs:56`），"热路径零锁"在调度器层面成立。
6. **计划文档诚实记录回退**：`docs/plans/2026-08-13-coroutine-scheduler-hardening.md:29` 明确写"P1-B…未优于 P1-A…代码已回退"。
7. **测试体系确实能拦住回归**：1071 个测试覆盖存储/协议/e2e；`stress_10000_rows`、
   `sql_bigdata`（2 万行负载）等重负载用例确实在跑（代价是全量串行约 5-8 分钟）。
   同时也暴露了断言陈旧的问题——见 P1-7 的更正。

---

## 八、结论与建议

### 判断

**优秀的个人原型，pre-1.0，不具备生产就绪条件。**

工程**纪律**（根因复盘、设计不变量注释、诚实的回退记录）显著高于同等规模项目的通常水平；工程**基础设施**（无 CI、测试绿灯不可复现、无评审者、入门文档断链）则明显不足。

核心判断是：**这个项目的问题不在写代码，而在没人验证它说的话。**

### 建议优先级

| 优先级 | 动作 | 预估 |
|---|---|---|
| 1 | 修复 P0-1（clippy）与 P0-2（死锁），补 CI | 0.5–1 天 |
| 2 | 改默认安全姿态：非 `0.0.0.0` 绑定、Binary 协议强制鉴权、`/metrics` 加令牌 | 2 小时 |
| 3 | 修正失准文档：删除或标记 DESIGN.md 中未实现部分，修复 AGENTS.md 断链与矛盾 | 2 小时 |
| 4 | 将"静默丢弃"改为显式报错：CHECK 约束、外键检查 | 0.5 天 |
| 5 | 把 `debug_assert` 关键不变量提升为运行时检查或校验和兜底 | 2 天 |

### 遗留风险

- **单一作者、11 次提交**覆盖 10.2 万行代码，bus factor 为 1
- **两个已知事故均为并发/非单调键下的静默数据丢失**；项目采用激进的内存批处理 + 自研调度器，property/fuzz 测试较少，同类问题很可能仍有潜伏实例
- **Windows 路径无任何 CI 覆盖**（而项目根本没有 CI）