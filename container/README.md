# `container/` — 容器化部署

镜像构建 + compose 编排 + 容器内默认配置,全部收在这里。

| 文件 | 用途 |
|---|---|
| `Dockerfile` | 多阶段构建: Rust 1-bookworm builder → debian-slim runtime。运行时非 root (`nexus:10001`), `RUST_MIN_STACK=64MB` |
| `docker-compose.yml` | 一键起库 (RESP 6379 / MySQL 5434 / PG 5435 / HTTP 6778) + 命名卷 `nexusdb-data` 持久化 + 内置 `HEALTHCHECK` |
| `.dockerignore` | 构建上下文排除: `target/` `.git/` `*.pem` 等 |
| `docker.toml` | **容器默认配置** (io_uring 后端)。COPY 进镜像 `/etc/nexusdb/nexusdb.toml` |
| `docker-stdfs.toml` | **stdfs 变体** (Docker 默认 seccomp 拦截 io_uring 时用)。`-v $PWD/container/docker-stdfs.toml:/etc/nexusdb/nexusdb.toml` 覆盖 |
| `run.sh` | 启停脚本: 自动 build (若镜像不在) + 跑容器 (含 `--security-opt seccomp=unconfined` + `--cap-add SYS_ADMIN` 让 io_uring 跑起来) |
| `seccomp-io_uring.json` | io_uring 专用 seccomp profile。**实际未使用** — `run.sh` 用 `seccomp=unconfined` 更省事 (详注见 `run.sh` 头部) |

## 用法

```bash
# 方式 1: docker compose (推荐)
docker compose -f container/docker-compose.yml up -d
docker compose -f container/docker-compose.yml logs -f
docker compose -f container/docker-compose.yml down

# 方式 2: 脚本 (包含 io_uring 完整 seccomp + cap)
./container/run.sh                # 后台启动
./container/run.sh logs           # 看日志
./container/run.sh stop           # 停止

# 方式 3: 纯 docker run
docker build -f container/Dockerfile -t nexusdb:latest .
docker run -d --name nexusdb \
  -p 6379:6379 -p 5434:5434 -p 5435:5435 -p 6778:6778 \
  -v nexusdb-data:/data \
  nexusdb:latest
```

**build context 始终是仓库根** (`..`)。`Dockerfile` 路径用 `-f container/Dockerfile` 显式指定。

## io_uring 说明

容器内默认用 `io_backend = "io_uring"`(项目核心设计)。Docker 默认 seccomp 拦截 io_uring syscall 时:
- **首选**: `run.sh` 已自动加 `--security-opt seccomp=unconfined` + `--cap-add SYS_ADMIN`(io_uring_register 需要)
- **备选**: 改用 `container/docker-stdfs.toml`(标准文件 IO, 任何内核都跑)

详细历史: [`docs/AGENTS.md`](../docs/AGENTS.md) 的服务化章节 + [`docs/CHANGELOG.md`](../docs/CHANGELOG.md) io_uring 调试记录。
