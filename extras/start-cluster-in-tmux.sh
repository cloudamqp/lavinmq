#!/bin/bash
set -e

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  echo "Usage: $0 [NODES]"
  echo "Start a LavinMQ VR cluster in tmux panes"
  echo ""
  echo "  NODES  Number of nodes to start (default: 3, max: 254)"
  echo ""
  echo "Environment:"
  echo "  LAVINMQ_CLUSTERING_SECRET     Shared VR secret (default: lavinmq-vr-local-cluster)"
  echo "  LAVINMQ_CLUSTER_REUSE_DATA=1  Reuse existing /tmp/amqpN data dirs (default: reset)"
  exit 0
fi

NODES=${1:-3}
SECRET=${LAVINMQ_CLUSTERING_SECRET:-lavinmq-vr-local-cluster}
REUSE_DATA=${LAVINMQ_CLUSTER_REUSE_DATA:-0}

if ! [[ "$NODES" =~ ^[1-9][0-9]*$ ]] || ((NODES > 254)); then
  echo "NODES must be a number between 1 and 254" >&2
  exit 1
fi

node_host() {
  local n=$1
  echo "127.0.0.$n"
}

members() {
  local entries=()
  local i
  for ((i=1; i<=NODES; i++)); do
    entries+=("$i=tcp://$(node_host "$i"):5679")
  done
  local IFS=,
  echo "${entries[*]}"
}

MEMBERS=$(members)

prepare_data_dirs() {
  local i
  local data_dir

  if [[ "$REUSE_DATA" == "1" ]]; then
    return
  fi

  echo "Resetting /tmp/amqp1..$NODES for a clean local VR cluster"
  echo "Set LAVINMQ_CLUSTER_REUSE_DATA=1 to reuse existing data dirs"
  for ((i=1; i<=NODES; i++)); do
    data_dir="/tmp/amqp$i"
    if [[ -e "$data_dir/.lock" ]] && command -v flock >/dev/null && ! flock -n "$data_dir/.lock" true; then
      echo "$data_dir is locked by a running LavinMQ node; stop it before resetting the local cluster" >&2
      exit 1
    fi
    rm -rf "$data_dir"
    mkdir -p "$data_dir"
  done
}

node_cmd() {
  local n=$1
  local host
  host=$(node_host "$n")
  echo "bin/lavinmq --data-dir=/tmp/amqp$n --bind=$host --metrics-http-bind=$host --control-unix-path=/tmp/lavinmqctl$n.sock --clustering --clustering-backend=vr --clustering-members=$MEMBERS --clustering-node-id=$n --clustering-secret=$SECRET --clustering-bind=$host --clustering-advertised-uri=tcp://$host:5679"
}

prepare_data_dirs

# Create new window with first node
tmux new-window -n lavinmq-cluster "$(node_cmd 1); read"

# Add remaining nodes as panes
for ((i=2; i<=NODES; i++)); do
  tmux split-window -v "$(node_cmd $i); read"
done

tmux select-layout even-vertical
