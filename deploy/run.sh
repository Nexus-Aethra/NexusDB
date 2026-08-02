#!/usr/bin/env bash
# ============================================================================
# NexusDB 容器启动脚本 — 支持 io_uring
# ============================================================================
# io_uring 需要两个条件 (缺一不可):
#   1) seccomp 放行 io_uring syscall (Docker 默认 profile 会拦截)
#   2) SYS_ADMIN capability (io_uring 的 fd 固定/注册 io_uring_register 需要)
#
# 为什么用 seccomp=unconfined 而非自定义 profile:
#   - Docker daemon 的 seccomp 参数有长度限制, 自定义 io_uring profile (5KB) 内联会
#     触发 "file name too long"。
#   - 精简自研 profile 逐个补齐 syscall 易漏 (已踩坑: clock_nanosleep/clock_gettime 缺失
#     导致 std::thread::sleep panic, acceptor 仍 Operation not permitted)。
#   - 该容器只运行可信的自研 NexusDB 二进制, 无其它进程, unconfined + SYS_ADMIN
#     风险可控, 且已验证完全健康。
#   如对安全要求极高, 可退回 io_backend=stdfs (见 deploy/nexusdb.stdfs.toml)。
#
# 用法:
#   ./deploy/run.sh           # 后台启动
#   ./deploy/run.sh -d        # 后台启动
#   ./deploy/run.sh -f        # 前台运行
#   ./deploy/run.sh stop      # 停止
#   ./deploy/run.sh logs      # 查看日志
# ============================================================================
set -euo pipefail

NAME="nexusdb"
IMAGE="nexusdb:local"
VOLUME="nexusdb-data"
PORTS=(-p 6379:6379 -p 5434:5434 -p 5435:5435 -p 6778:6778)

log() { echo "[nexusdb] $*"; }

case "${1:-}" in
  stop)
    log "stopping..."
    docker rm -f "$NAME" 2>/dev/null || true
    exit 0
    ;;
  logs)
    docker logs -f "$NAME"
    exit 0
    ;;
  -f)
    detach=()
    ;;
  -d|"")
    detach=(-d)
    ;;
  *)
    echo "usage: $0 [-d|-f|stop|logs]" >&2
    exit 1
    ;;
esac

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  log "image $IMAGE not found, building..."
  docker build -t "$IMAGE" "$(dirname "$0")/.."
fi

log "starting with io_uring support (seccomp=unconfined + SYS_ADMIN)..."

docker rm -f "$NAME" 2>/dev/null || true

docker run "${detach[@]}" \
  --name "$NAME" \
  --restart unless-stopped \
  "${PORTS[@]}" \
  -v "$VOLUME":/data \
  --security-opt seccomp=unconfined \
  --cap-add SYS_ADMIN \
  "$IMAGE"

log "started (name=$NAME). ports: 6379 RESP / 5434 MySQL / 5435 PG / 6778 HTTP"
log "health: curl -fsS http://localhost:6778/v1/status (RESP: redis-cli ping)"
