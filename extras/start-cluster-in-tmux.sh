#!/bin/bash
set -e

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  echo "Usage: $0 [NODES]"
  echo "Start a LavinMQ cluster in tmux panes"
  echo ""
  echo "  NODES  Number of nodes to start (default: 3)"
  exit 0
fi

NODES=${1:-3}

node_cmd() {
  local n=$1
  echo "bin/lavinmq --data-dir=/tmp/amqp$n --bind=127.$n --metrics-http-bind=127.$n --clustering --clustering-bind=127.$n --clustering-advertised-uri=tcp://127.$n:5679"
}

# Create new window with first node
tmux new-window -n lavinmq-cluster "$(node_cmd 1); read"

# Add remaining nodes as panes
for ((i=2; i<=NODES; i++)); do
  tmux split-window -v "$(node_cmd $i); read"
done

tmux select-layout even-vertical
