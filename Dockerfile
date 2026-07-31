# syntax=docker/dockerfile:1
# ============================================================================
# NexusDB 容器镜像 — 多阶段构建 (Rust builder -> debian-slim runtime)
# ============================================================================
#
# 构建:  docker build -t nexusdb:latest .
# 运行:  docker run --rm -p 6379:6379 -p 5434:5434 -p 5435:5435 -p 6778:6778 \
#              -v nexusdb-data:/data nexusdb:latest
#
# io_uring 说明: 默认配置用 io_uring 后端 (本项目核心设计)。若宿主内核较老
# 或 Docker 默认 seccomp 拦截 io_uring 系统调用, 出现 I/O 报错时二选一:
#   1) 挂载自定义配置把 io_backend 改成 "stdfs" (兼容任何内核):
#        -v $PWD/my.toml:/etc/nexusdb/nexusdb.toml
#   2) 放宽 seccomp: docker run --security-opt seccomp=unconfined ...
# ----------------------------------------------------------------------------

# ---- Stage 1: builder ----
ARG RUST_IMAGE=rust:1-bookworm
FROM ${RUST_IMAGE} AS builder

# ring (rustls 加密后端) 需要 C 编译器 + perl; debian bookworm 基础镜像已带 gcc/perl,
# 这里显式补齐以防精简镜像缺失。
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# ⭐ 关键: 仓库 .cargo/config.toml 里有开发机专用的 rust-lld 链接器绝对路径,
#    容器内不存在, 会导致链接失败 — 构建镜像时移除该开发期优化 (仅影响链接速度)。
RUN rm -f .cargo/config.toml

# 发布构建 (只构建服务器二进制)。存储层 async 帧较大, 构建期无特殊要求;
# 运行期通过 RUST_MIN_STACK 提升线程栈。
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin NexusDB && \
    cp target/release/NexusDB /usr/local/bin/NexusDB

# ---- Stage 2: runtime ----
FROM debian:bookworm-slim AS runtime

# 运行期仅需 glibc (ring 静态链接, rustls 纯 Rust, io_uring 走裸系统调用无需 liburing)。
# curl 用于容器健康检查 (HTTP /v1/status)。
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /data --shell /usr/sbin/nologin nexus \
    && mkdir -p /data /etc/nexusdb \
    && chown -R nexus:nexus /data

COPY --from=builder /usr/local/bin/NexusDB /usr/local/bin/NexusDB
COPY deploy/nexusdb.docker.toml /etc/nexusdb/nexusdb.toml

# 存储层 async fn poll frame 较大 (含多个 16KB page buffer), 提升线程栈避免溢出。
ENV RUST_MIN_STACK=67108864

# 门面端口: 6379 RESP / 5434 MySQL wire / 5435 PostgreSQL wire / 6778 HTTP REST / 5433 Binary(内部)
EXPOSE 6379 5434 5435 6778 5433

# 数据目录 (block_root); 生产请挂命名卷或绑定目录持久化。
VOLUME ["/data"]

# 健康检查: HTTP REST /v1/status (免鉴权恒可访问)。
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
    CMD curl -fsS http://127.0.0.1:6778/v1/status || exit 1

USER nexus
WORKDIR /data

# 优雅退出: 进程处理 SIGTERM/SIGINT; docker stop 默认发 SIGTERM。
ENTRYPOINT ["/usr/local/bin/NexusDB"]
CMD ["--config", "/etc/nexusdb/nexusdb.toml"]
