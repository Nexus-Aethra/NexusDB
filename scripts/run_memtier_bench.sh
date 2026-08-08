#!/usr/bin/env bash
# Reproducible local RESP benchmark.  Each invocation owns its data directory,
# port and server PID; it never touches nexusdb.toml or bench_tmp.
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=${NEXUSDB_BIN:-"$root_dir/target/release/NexusDB"}
workers=4
shards=4
duration=30
repeats=3
port=16379
wal_mode=off
keep_data=0
put_batch=0
leaf_cache=1
probe=0
workload=all
log_level=warn

usage() {
    cat <<'EOF'
Usage: scripts/run_memtier_bench.sh [options]

  --workers N       network worker count (default: 4)
  --shards N        storage shard count (default: 4)
  --duration SEC    seconds per measured run (default: 30)
  --repeats N       repetitions per workload (default: 3)
  --port PORT       loopback RESP port (default: 16379)
  --wal-mode MODE   off, periodic, or strict (default: off)
  --disable-put-batch
                    do not set the experimental NEXUS_PUT_BATCH=1 switch
  --enable-put-batch
                    enable the experimental Put micro-batch path
  --disable-leaf-cache
                    run with NEXUS_LEAF_CACHE=0 for cache A/B comparison
  --probe           enable NLOG_PROBE=1 and retain stage histograms in server.log
  --workload NAME   overwrite, fresh-write, mixed, read-heavy, hot-read, or all (default: all)
  --keep-data       preserve the generated temporary directory
EOF
}

while (($#)); do
    case "$1" in
        --workers) workers=$2; shift 2 ;;
        --shards) shards=$2; shift 2 ;;
        --duration) duration=$2; shift 2 ;;
        --repeats) repeats=$2; shift 2 ;;
        --port) port=$2; shift 2 ;;
        --wal-mode) wal_mode=$2; shift 2 ;;
        --disable-put-batch) put_batch=0; shift ;;
        --enable-put-batch) put_batch=1; shift ;;
        --disable-leaf-cache) leaf_cache=0; shift ;;
        --probe) probe=1; shift ;;
        --workload) workload=$2; shift 2 ;;
        --keep-data) keep_data=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if ((probe)); then
    log_level=info
fi

case "$wal_mode" in off|periodic|strict) ;; *) echo "invalid --wal-mode: $wal_mode" >&2; exit 2 ;; esac
case "$workload" in overwrite|fresh-write|mixed|read-heavy|hot-read|all) ;; *) echo "invalid --workload: $workload" >&2; exit 2 ;; esac
[[ -x "$binary" ]] || { echo "NexusDB binary not found: $binary" >&2; exit 2; }
command -v memtier_benchmark >/dev/null || { echo "memtier_benchmark is required" >&2; exit 2; }
command -v redis-cli >/dev/null || { echo "redis-cli is required" >&2; exit 2; }

bench_dir=$(mktemp -d "${TMPDIR:-/tmp}/nexus-memtier.XXXXXX")
config="$bench_dir/nexusdb.toml"
server_log="$bench_dir/server.log"
server_pid=""

cleanup() {
    local rc=$?
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$server_pid" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$server_pid" 2>/dev/null; then
            echo "server did not exit within 5s; force stopping pid $server_pid" >&2
            kill -KILL "$server_pid" 2>/dev/null || true
            rc=1
        fi
        wait "$server_pid" 2>/dev/null || true
    fi
    if ((keep_data)); then
        echo "benchmark artifacts: $bench_dir"
    else
        rm -rf "$bench_dir"
    fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

cat >"$config" <<EOF
[server]
listen_addr = "127.0.0.1:15433"
worker_count = $workers
redis_addr = "127.0.0.1:$port"
sql_addr = ""
pg_addr = ""
http_addr = ""
redis_password = ""

[storage]
block_root = "$bench_dir/data"
num_shards = $shards
io_backend = "io_uring"
chunk_cache_size = 8
wal_mode = "$wal_mode"

[log]
level = "$log_level"
dir = "$bench_dir/logs"
stderr = false
EOF

if ((put_batch)); then
    NEXUS_PUT_BATCH=1 NEXUS_LEAF_CACHE="$leaf_cache" NLOG_PROBE="$probe" "$binary" --config "$config" >"$server_log" 2>&1 &
else
    NEXUS_LEAF_CACHE="$leaf_cache" NLOG_PROBE="$probe" "$binary" --config "$config" >"$server_log" 2>&1 &
fi
server_pid=$!
for _ in $(seq 1 100); do
    if redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        cat "$server_log" >&2
        exit 1
    fi
    sleep 0.1
done
redis-cli -h 127.0.0.1 -p "$port" ping >/dev/null

threads=4
clients_per_thread=8
total_clients=$((threads * clients_per_thread))
preload_total=100000
# memtier's --requests is per client, not global.  Round up so preload stays
# close to the documented 100k-key warmup instead of silently issuing 3.2M.
preload_per_client=$(((preload_total + total_clients - 1) / total_clients))
common=(--server=127.0.0.1 --port="$port" --protocol=redis --pipeline=16
        --threads="$threads" --clients="$clients_per_thread" --data-size=64
        --key-maximum=100000 --hide-histogram)

echo "[preload] $((preload_per_client * total_clients)) parallel-sequential SETs"
# P assigns each client a disjoint sequential slice, so the preload covers the
# configured key range instead of merely sampling it at random.
memtier_benchmark "${common[@]}" --key-pattern=P:P --ratio=1:0 \
    --requests="$preload_per_client" >/dev/null
sleep 5

workloads=(
    'overwrite|1:0|1|100000'
    # Keep fresh writes outside the preloaded range. The 100M range makes
    # collisions negligible during short runs while preserving random routing.
    'fresh-write|1:0|100001|100000000'
    'mixed|1:1|1|100000'
    'read-heavy|1:9|1|100000'
    # A 1% subset of the preloaded keyspace. This isolates the intended
    # repeated-leaf traversal workload from the random-key read baseline.
    'hot-read|1:99|1|1000'
)
for workload_spec in "${workloads[@]}"; do
    IFS='|' read -r name ratio key_min key_max <<<"$workload_spec"
    if [[ "$workload" != all && "$workload" != "$name" ]]; then
        continue
    fi
    for repeat in $(seq 1 "$repeats"); do
        output="$bench_dir/${name}-${repeat}.txt"
        echo "[run] $name ($repeat/$repeats)"
        memtier_benchmark "${common[@]}" --ratio="$ratio" --key-minimum="$key_min" \
            --key-maximum="$key_max" --test-time="$duration" | tee "$output"
    done
done
