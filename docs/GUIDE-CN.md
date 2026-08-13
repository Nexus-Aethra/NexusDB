# NexusDB 功能介绍与使用文档

> **语言 / Language**: [English](./GUIDE.md) | **简体中文**

> 面向使用者的上手指南。设计与实现细节见 [`DESIGN.md`](./DESIGN.md);修复/演进历史见 [`CHANGELOG.md`](./CHANGELOG.md)。

## 目录
1. [这是什么](#1-这是什么)
2. [快速开始](#2-快速开始)
3. [多协议门面接入](#3-多协议门面接入)
4. [SQL 能力与示例](#4-sql-能力与示例)
5. [数据类型](#5-数据类型)
6. [安全:认证与 TLS](#6-安全认证与-tls)
7. [配置参考](#7-配置参考)
8. [性能实测](#8-性能实测)
9. [能力边界](#9-能力边界)

---

## 1. 这是什么

NexusDB 是一个用 Rust 2024 编写的**嵌入式单机、写密集/低延迟/高并发 KV + SQL 数据库引擎**,核心特征:

- **架构**:Share-Nothing + Per-Core 线程 + io_uring 异步 I/O + 自研协程调度器(不依赖 tokio)。
- **存储**:COW append-only + LCB-Tree 页(前缀压缩)+ 多 db/多表物理隔离 + 崩溃恢复。
- **多协议门面(一套内核,五种接入)**:
  - **RESP2**(Redis 兼容):五大数据结构 + Geo + Bitmap
  - **MySQL wire**:`mysql` CLI / 驱动直连
  - **PostgreSQL wire**:`psql` / psycopg 直连
  - **HTTP REST**:KV + SQL JSON 接口 + Prometheus `/metrics`
  - **Binary**(内部/压测用)
- **SQL 子集**:DDL/DML/SELECT(JOIN、子查询、聚合、GROUP BY/HAVING、DISTINCT、表达式聚合)、事务(OCC 隔离级别 + SAVEPOINT)、本地二级索引 + 布隆剪枝 + GLOBAL UNIQUE。
- **安全**:MySQL `caching_sha2_password` / PostgreSQL `SCRAM-SHA-256` 认证 + **rustls TLS**(opt-in)。

> MySQL 和 PostgreSQL 两个门面**共用同一内核、同一份数据**,可同库互读写。

---

## 2. 快速开始

### 构建
```bash
# 调试构建
cargo build
# 发布构建 (性能测试/部署用)
cargo build --release
```

> 注:存储层 async 帧较大,运行/测试需要更大线程栈:`RUST_MIN_STACK=67108864`。

### 最简配置 (`config/nexusdb.toml`)
```toml
[server]
sql_addr = "127.0.0.1:5434"      # MySQL wire
pg_addr  = "127.0.0.1:5435"      # PostgreSQL wire
redis_addr = "127.0.0.1:6379"    # RESP (Redis)
http_addr  = "127.0.0.1:6778"    # HTTP REST
sql_password = ""                # 空 = 免密

[storage]
block_root = "./data"
default_db = "default"
default_table = "default"
```

### 启动
```bash
RUST_MIN_STACK=67108864 ./target/release/NexusDB --config config/nexusdb.toml
```
启动后日志会打印各门面监听地址。任一门面 `addr` 留空即禁用该门面。

### Docker 部署
镜像随仓库提供 [`container/Dockerfile`](../container/Dockerfile)(多阶段:Rust builder → debian-slim)+ [`container/docker-compose.yml`](../container/docker-compose.yml)+ 容器默认配置 [`container/docker.toml`](../container/docker.toml)(受限 seccomp 环境备 [`container/docker-stdfs.toml`](../container/docker-stdfs.toml))。
```bash
# 构建镜像
docker build -t nexusdb:latest .

# 直接运行 (持久化到命名卷 nexusdb-data)
docker run -d --name nexusdb \
  -p 6379:6379 -p 5434:5434 -p 5435:5435 -p 6778:6778 \
  -v nexusdb-data:/data nexusdb:latest

# 或 compose 一键起
docker compose up -d
```
- 数据持久化在容器 `/data`(`VOLUME`);内置 `HEALTHCHECK` 探测 HTTP `/v1/status`。
- 覆盖配置:`-v /path/your.toml:/etc/nexusdb/nexusdb.toml`。
- **io_uring 注意**:默认 `io_backend=io_uring`,需较新内核;若被宿主 seccomp 拦截而报 I/O 错,改配置为 `io_backend="stdfs"`(挂自定义配置),或 `docker run --security-opt seccomp=unconfined`。
- 启用 TLS:挂证书目录并在配置里设置 `tls_cert`/`tls_key`。

### Windows 部署 (Beta, 2026-08-13)

原生 Windows 支持是 beta 阶段,在 `feat/resp-sql-schema-adapter` 分支。计划 + 踩坑
见 [`docs/plans/2026-08-13-windows-portability.md`](./plans/2026-08-13-windows-portability.md) +
[`docs/plans/2026-08-13-windows-iocp.md`](./plans/2026-08-13-windows-iocp.md)。

```bash
# 1) 工具链
rustup default stable-x86_64-pc-windows-msvc

# 2) 编译
git clone https://github.com/Nexus-Aethra/NexusDB.git
cd NexusDB
cargo build --release --workspace
```

最小 `config/nexusdb-test.toml` (Windows 上自动纠正 `io_backend` 为 `stdfs`):

```toml
[server]
listen_addr = "127.0.0.1:5433"     # Binary
redis_addr  = "127.0.0.1:6380"     # RESP (用 6380 避开 win 自带的 redis-server)
worker_count = 1
sql_addr = ""                    # SQL/PG/HTTP 是 Linux 路径
pg_addr   = ""
http_addr = ""

[storage]
block_root = "./data-test"
num_shards = 2
io_backend = "stdfs"
create_if_missing = true
default_db = "default"
default_table = "default"
precreate_dbs = 1
```

运行 + smoke:

```bash
./target/release/NexusDB.exe --config config/nexusdb-test.toml
# 另一个 shell (Redis.Redis 装在 Program Files 下):
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 PING             # PONG
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 SET user:1 alice # OK
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 GET user:1       # alice
& "C:\Program Files\Redis\redis-cli.exe" -p 6380 DEL user:1       # 1
```

Windows 上能用的: Binary (5433) + RESP (6380), `PING`/`AUTH`/`SET`/`GET`/`DEL`/`MGET`/`MSET`/等, WAL 持久化 + 崩溃恢复, Ctrl-C 优雅停止。

Windows 上还不能用的: MySQL / PostgreSQL / HTTP REST / TLS 门面, IOCP/RIO 性能路径,
memtier 压测, 860+ 测试矩阵。`INCR`/`HSET`/`LPUSH`/`SADD`/`ZADD`/`DBSIZE`/`INFO`/`CLIENT LIST`
在 dispatch 树上还没接 (Linux `portable.rs` 上同样没接, 是协议层本身的工作, 跟
Windows runtime 无关)。

踩坑:

- **没有 `io_uring`**: Windows 上 `io_backend` 字段被忽略; 缺省 config 路径自动降级
  到 `stdfs`, 也可以显式写 `io_backend = "stdfs"` 显式表达。
- **6379 端口被自带 redis-server 占用**: `Redis.Redis` winget 包装的
  `redis-server.exe` 跑在 SYSTEM 账户, 没 admin 杀不掉; 测试用 6380 (或任何空闲
  端口) 避让。生产可改成自己 config 里的空闲端口。
- **Listener `set_nonblocking(true)` 状态继承到 child socket**: acceptor 轮询 `stop`
  atomic 需要这个, 但 winsock 会把 non-blocking 继承给 accept 出来的子 socket。
  子连接线程的 read 把 `WSAEWOULDBLOCK` (10035) 和 `WSAETIMEDOUT` 当正常背压
  (短 sleep + retry); **绝不能** `return Err` 关连接, 不然 client 在连发命令时
  会看到 "An existing connection was forcibly closed"。
- **`#[repr(C)]` 是 IOCP `OverlappedData` 的硬约束**: 以后再启用 IOCP 路径时,
  `OVERLAPPED` 必须是第一个字段, struct 必须 `#[repr(C)]`。Rust 默认 `repr(Rust)`
  会 reorder 字段, GQCS 拿到的 ptr 强转后会拿到错位的状态。
- **`windows-sys = "0.61"` 类型细节**:
  - `ACCEPTEX` 不存在, 是 `LPFN_ACCEPTEX` (`Option<unsafe extern "system" fn(...)>`)
  - `setsockopt` 第 4 参是 `PSTR` (`*const u8`), 不是 `*const c_void`

---

## 3. 多协议门面接入

### 3.1 RESP(Redis 兼容,默认 6379)
```bash
redis-cli -p 6379
> SET user:1 alice
OK
> GET user:1
"alice"
> HSET h f1 v1 f2 v2
> LPUSH mylist a b c
> ZADD z 1 one 2 two
> GEOADD geo 13.361 38.115 "palermo"
> SETBIT bm 7 1
```
支持 String/Hash/List/Set/ZSet 五大结构 + Geo + Bitmap 命令面。

### 3.2 MySQL wire(默认 5434)
```bash
mysql -h127.0.0.1 -P5434 -uroot
```
```python
# mysql-connector-python
import mysql.connector
c = mysql.connector.connect(host="127.0.0.1", port=5434, user="root",
                            password="", database="default")
cur = c.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO t VALUES (1, 'alice')")
cur.execute("SELECT * FROM t"); print(cur.fetchall())
```

### 3.3 PostgreSQL wire(默认 5435)
```bash
psql -h 127.0.0.1 -p 5435 -U root -d default
```
```python
# psycopg 3
import psycopg
c = psycopg.connect("host=127.0.0.1 port=5435 user=root dbname=default", autocommit=True)
with c.cursor() as cur:
    cur.execute("SELECT id, name FROM t")
    print(cur.fetchall())
```

### 3.4 SQLAlchemy ORM(MySQL 或 PostgreSQL 方言)
```python
from sqlalchemy import create_engine, text
# MySQL 方言
eng = create_engine("mysql+mysqlconnector://root:@127.0.0.1:5434/default")
# 或 PostgreSQL 方言
# eng = create_engine("postgresql+psycopg://root:@127.0.0.1:5435/default")
with eng.begin() as conn:
    conn.execute(text("CREATE TABLE u (id INT PRIMARY KEY, age INT)"))
    conn.execute(text("INSERT INTO u VALUES (1, 30)"))
    print(conn.execute(text("SELECT * FROM u")).fetchall())
```
基础 CRUD / JOIN / 分页 / 反射 / 迁移(ADD COLUMN)实机可用。

### 3.5 HTTP REST(默认 6778)
```bash
# KV
curl -X PUT  http://127.0.0.1:6778/v1/kv/user:1 -d 'alice'
curl http://127.0.0.1:6778/v1/kv/user:1
# SQL (JSON)
curl -X POST http://127.0.0.1:6778/v1/sql -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT * FROM t"}'
# 监控
curl http://127.0.0.1:6778/metrics       # Prometheus 指标
curl http://127.0.0.1:6778/v1/status
```

---

## 4. SQL 能力与示例

### DDL
```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  email TEXT,
  age INT,
  INDEX(age),                 -- 本地二级索引
  UNIQUE(email)               -- 唯一索引 (本 shard); 跨 shard 全局唯一用 GLOBAL UNIQUE
);
ALTER TABLE users ADD COLUMN created DATE;   -- 追加可空列 (零数据重写)
DESCRIBE users;
SHOW CREATE TABLE users;
DROP TABLE users;
```

### DML
```sql
INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25);
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE age < 18;
```

### SELECT(投影 / 过滤 / 排序 / 分页 / 别名)
```sql
SELECT id, name AS n FROM users WHERE age >= 18 AND name LIKE 'a%'
  ORDER BY age DESC LIMIT 10 OFFSET 5;          -- 或 MySQL LIMIT 5,10
SELECT * FROM db1.users;                         -- db.table 限定名
```

### 聚合 / GROUP BY / HAVING / DISTINCT
```sql
SELECT age, COUNT(*), SUM(score), AVG(score) FROM users GROUP BY age HAVING COUNT(*) > 1;
SELECT COUNT(DISTINCT age) FROM users;
SELECT DISTINCT age FROM users;
SELECT SUM(price * qty) FROM orders;             -- 表达式聚合
```

### JOIN / 子查询
```sql
SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid;   -- INNER/LEFT/RIGHT/FULL/CROSS/USING
SELECT * FROM users WHERE id IN (SELECT uid FROM orders);          -- 非关联 IN
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.uid = u.id);  -- 单等值关联 EXISTS
SELECT * FROM (SELECT uid, SUM(amt) s FROM orders GROUP BY uid) t WHERE t.s > 100;  -- FROM 派生表
```

### 事务
```sql
BEGIN;                                  -- 或 BEGIN ISOLATION LEVEL SERIALIZABLE
UPDATE users SET age = 40 WHERE id = 1;
SAVEPOINT sp1;
DELETE FROM users WHERE id = 2;
ROLLBACK TO sp1;
COMMIT;
```
OCC 隔离级别 + SAVEPOINT;单 shard 严格原子,跨 shard best-effort;DDL 在事务中被拒。

### 系统表
```sql
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
SHOW TABLES; SHOW DATABASES;
```

---

## 5. 数据类型

| SQL 类型 | 存储 | 说明 |
|---|---|---|
| `INT/BIGINT/SMALLINT` | i64 | 整数 |
| `BOOLEAN/BOOL` | i64(0/1) | 布尔;渲染 MySQL `1/0`,PG `t/f` |
| `DOUBLE/FLOAT/REAL` | f64 | 浮点 |
| `DECIMAL/NUMERIC(p,s)` | i128 定点 | **精确金额**(精度 ≤ 38);SUM 精确;驱动返回原生 `Decimal` |
| `TEXT/VARCHAR(n)/CHAR(n)` | 变长字节 | 字符串 |
| `BLOB/BYTES/BYTEA` | 变长字节 | 二进制 |
| `DATE/TIME/TIMESTAMP/DATETIME` | i64 微秒 | 时间(UTC 裸值);驱动返回原生 `date`/`datetime` |
| `JSON/JSONB` | 文本字节 | 半结构化(单行 < 64KB) |
| `UUID` | 16B | 驱动返回原生 `UUID` |

**类型示例:**
```sql
CREATE TABLE account (
  id INT PRIMARY KEY,
  active BOOLEAN,
  balance DECIMAL(18,2),        -- 精确金额
  created DATE,
  updated TIMESTAMP,
  profile JSON,
  token UUID
);
INSERT INTO account VALUES
  (1, TRUE, '1234.56', DATE '2024-06-01',
   TIMESTAMP '2024-06-01 09:30:00', '{"vip":true}',
   '550e8400-e29b-41d4-a716-446655440000');

SELECT SUM(balance) FROM account;               -- 精确 (不丢精度)
SELECT id FROM account WHERE created > DATE '2024-01-01' ORDER BY created;
SELECT id FROM account WHERE active = TRUE;
```
> psycopg3 会把上表各列直接映射为 Python 的 `bool` / `Decimal` / `date` / `datetime` / `dict` / `UUID`;mysql-connector(含预处理二进制协议)返回原生 `datetime` / `Decimal`。

---

## 6. 安全:认证与 TLS

### 6.1 认证(密码)
配置 `sql_password` 即启用登录认证(MySQL 与 PostgreSQL 门面共用):
```toml
[server]
sql_password = "s3cret"
```
- **PostgreSQL 门面**:非空口令走 **SCRAM-SHA-256**(彻底消除明文口令)。
- **MySQL 门面**:支持 `caching_sha2_password` fast-auth 与 `mysql_native_password`(挑战响应,自动兜底)。
- 空口令 = 免密(任意用户名放行);错误口令拒绝(MySQL 1045 / PG 28P01)。

```python
# psycopg 自动用 SCRAM
psycopg.connect("host=127.0.0.1 port=5435 user=root password=s3cret dbname=default")
# mysql-connector 自动协商 caching_sha2 / native
mysql.connector.connect(host="127.0.0.1", port=5434, user="root", password="s3cret", database="default")
```

### 6.2 TLS 传输加密(opt-in)
配置证书 + 私钥路径即启用(**两项均非空才启用**;不配 = 纯明文,零开销):
```toml
[server]
tls_cert = "/etc/nexusdb/cert.pem"   # 证书链 PEM
tls_key  = "/etc/nexusdb/key.pem"    # 私钥 PEM (PKCS8 / PKCS1 / SEC1)
```
生成自签证书(测试用):
```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout key.pem -out cert.pem -days 3650 \
  -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost"
```
连接(STARTTLS 式:SQL 门面握手内升级,同端口):
```python
# PostgreSQL: sslmode=require (加密, 不验证自签证书)
psycopg.connect("host=127.0.0.1 port=5435 user=root password=s3cret dbname=default sslmode=require")
# MySQL: 启用 SSL
mysql.connector.connect(host="127.0.0.1", port=5434, user="root", password="s3cret",
                        database="default", ssl_disabled=False, ssl_verify_cert=False)
```
- 底层:rustls 0.23(ring 后端),TLS 1.2/1.3。
- 未配置 TLS 的连接仍可明文接入(opt-in,向后兼容)。
- v1 边界:无客户端证书双向认证;无 SCRAM channel binding;单一 `sql_password`(暂无 per-user 账户体系)。

---

## 7. 配置参考

```toml
[server]
listen_addr = "0.0.0.0:5433"     # Binary 内部协议 (压测/测试)
redis_addr  = "0.0.0.0:6379"     # RESP; 空 = 禁用
sql_addr    = "0.0.0.0:5434"     # MySQL wire; 空 = 禁用
pg_addr     = "0.0.0.0:5435"     # PostgreSQL wire; 空 = 禁用
http_addr   = "0.0.0.0:6778"     # HTTP REST; 空 = 禁用
sql_password = ""                # SQL 登录密码 (空 = 免密; 非空 PG 走 SCRAM)
redis_password = ""              # RESP AUTH 密码
http_cors_origin = ""            # CORS Allow-Origin ("*"/具体 origin)
http_token = ""                  # REST Bearer token (空 = 免鉴权)
tls_cert = ""                    # TLS 证书 PEM (两项均非空才启用 TLS)
tls_key  = ""                    # TLS 私钥 PEM
sql_worker_count = 1             # SQL/PG 门面 worker 数 (并发连接池可调 2-8)
max_key_bytes = 1024             # key 上限
max_value_bytes = 1048576        # value 上限 (>4KB 自动溢出页)

[storage]
block_root = "./data"            # 数据目录
default_db = "default"
default_table = "default"
```

---

## 8. 性能实测

> 环境:release 构建,loopback,单机。数值为参考量级,非严格基准。

### RESP(Redis 门面,50 并发)
| 操作 | 无 pipeline | pipeline=16 |
|---|---|---|
| SET | ~119K qps (p50 0.31ms) | ~327K qps |
| GET | ~156K qps (p50 0.16ms) | ~498K qps |

### SQL 点操作(单连接,含驱动开销)
| 模式 | 点 SELECT | 点 INSERT |
|---|---|---|
| 明文 | ~26K qps (p50 0.036ms) | ~14K qps |
| TLS 加密 | ~18K qps (p50 0.051ms) | ~11K qps |

**关于 TLS 开销**:上表是单连接、串行、极小载荷的**最坏场景**——每个操作只需几十微秒,加解密占比被放大,故 qps 下降看似 ~25-30%,但绝对延迟仅增加约 15µs(0.036→0.051ms)。真实业务:
- 握手成本一次性(连接池长连接摊薄到接近零);
- 稳态对称加密在有 AES-NI 时是 GB/s 级,吞吐型负载 TLS 影响通常个位数百分比;
- **不配 TLS 则零开销**(与明文路径完全一致)。

---

## 9. 能力边界

已交付但存在 v1 限制,选型/使用时请注意:

- **权限**:单一 `sql_password`,暂无 per-user 用户/角色/权限体系。
- **TLS**:opt-in;无客户端证书双向认证、无 SCRAM channel binding。
- **事务**:单 shard 严格原子,跨 shard best-effort;DDL 在事务中被拒。
- **SQL**:LIKE 仅前缀模式;聚合暂不支持 GROUP_CONCAT/窗口函数;子查询剩余缺口(关联标量、多重相关 EXISTS、JOIN 右侧派生表);ORDER BY 全量排序(无 top-k)。
- **约束**:普通 UNIQUE 为本 shard best-effort;跨 shard 全局唯一需显式 `GLOBAL UNIQUE`。
- **备份 / HA**:暂无内置备份/PITR、复制/高可用。
- **JSON**:文本存储,单行 < 64KB,不建 JSON 路径索引。
- **时间**:统一 UTC 裸值,无时区转换。

> 完整 gap 清单见 [`README-CN.md`](../README-CN.md) 的 "SQL gap" 段与 [`CHANGELOG.md`](./CHANGELOG.md)。
