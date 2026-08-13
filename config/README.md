# `config/` — 配置文件

所有 `.toml` 配置集中在这里,避免散落在仓库根或 `scripts/` 里。

| 文件 | 用途 | 默认查找顺序 |
|---|---|---|
| `nexusdb.toml` | **主配置** (生产/开发)。五协议全开 (`io_uring` 后端),可作为 `nexusdb --config` 的默认。 | Linux: `./nexusdb.toml` → `./config/nexusdb.toml`; Windows: `./config/nexusdb.toml` |
| `nexusdb-test.toml` | **Windows M2 验证配置**。`io_backend = "stdfs"`、RESP 走 6380 (避让 SYSTEM 账户的 redis-server 6379)、SQL/PG/HTTP facade 关闭。 | Windows 默认路径不存在时回退到这里 (老项目兼容); 显式 `--config` 不会回退 |
| `bench.toml` | **memtier 压测配置**。`wal_mode = "off"` 避免 fsync 干扰吞吐; 数据目录 `./bench_tmp/data`。 | `scripts/run_memtier_bench.sh` 显式指定 |
| `smoke.toml` | **smoke 端到端测试**。`redis_addr = "127.0.0.1:16379"`, 带 AUTH 密码 `smokepass`。 | `scripts/smoke_client.py` 显式指定 |

## 启动方式

```bash
# 主配置 (Linux 默认会找 ./nexusdb.toml, 找不到时自动落 ./config/nexusdb.toml)
./target/release/NexusDB --config config/nexusdb.toml

# Windows 默认就查这里
./target/release/NexusDB.exe --config config/nexusdb.toml
```

**`--config` 显式时不会做静默回退** — 路径不存在直接报错,避免"以为加载了某个配置结果用了默认值"的坑。
