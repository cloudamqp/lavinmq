#!/usr/bin/env bash
# Runs a 3-node raft cluster on 127.0.0.2-4 for rehearsing the demo on one
# machine. Stop a node with `kill -STOP` to simulate a pulled plug (no RST),
# `kill -CONT` to plug it back in.
#
#   dev/local-cluster.sh start [path/to/lavinmq]   # default: lavinmq in PATH
#   dev/local-cluster.sh stop
set -euo pipefail

DATA=${DATA:-/tmp/booth-cluster}
CMD=${1:-start}
LAVINMQ=${2:-lavinmq}

case "$CMD" in
start)
  mkdir -p "$DATA"
  for n in 2 3 4; do
    bootstrap=""
    # Only on the very first start, before any node has election state
    if [[ $n == 2 && ! -e "$DATA/node$n/.raft_state" ]]; then bootstrap="--clustering-bootstrap"; fi
    "$LAVINMQ" --data-dir="$DATA/node$n" --bind=127.0.0.$n --metrics-http-bind=127.0.0.$n \
      --clustering --clustering-bind=127.0.0.$n \
      --clustering-seeds=127.0.0.2,127.0.0.3,127.0.0.4 \
      --clustering-password=booth-local-only $bootstrap \
      >"$DATA/node$n.log" 2>&1 &
    echo $! >"$DATA/node$n.pid"
    echo "node$n: pid $!, log $DATA/node$n.log"
  done
  echo "Waiting for a leader…"
  for _ in $(seq 60); do
    for n in 2 3 4; do
      if curl -fs "http://127.0.0.$n:15692/metrics" 2>/dev/null | grep -q '^lavinmq_raft_is_leader 1'; then
        echo "Leader: 127.0.0.$n"
        # The default guest user only works from localhost; the demo uses an admin user
        ctl=("$(dirname "$LAVINMQ")/lavinmqctl" --uri "http://guest:guest@127.0.0.$n:15672")
        "${ctl[@]}" add_user booth booth >/dev/null 2>&1 || true
        "${ctl[@]}" set_user_tags booth administrator
        "${ctl[@]}" set_permissions booth ".*" ".*" ".*"
        exit 0
      fi
    done
    sleep 0.5
  done
  echo "No leader after 30 s, see the logs" >&2
  exit 1
  ;;
stop)
  for f in "$DATA"/node*.pid; do
    kill -CONT "$(cat "$f")" 2>/dev/null || true
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  ;;
*)
  echo "usage: $0 start|stop [path/to/lavinmq]" >&2
  exit 2
  ;;
esac
