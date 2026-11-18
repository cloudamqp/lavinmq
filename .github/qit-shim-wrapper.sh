#!/usr/bin/env bash
# Wraps a QIT (Apache Qpid Interop Test) shim for running against LavinMQ.
#
# QIT addresses queues by bare names such as "qit.test.int.a.b" and expects
# the broker to create them. LavinMQ addresses queues as /queues/<name> and
# does not create them on attach, so this wrapper declares the queue over the
# HTTP API and rewrites the address before running the original shim, which
# is expected next to this script as shim.orig.sh.
#
# The HTTP API is expected on the AMQP port plus 10000 (5672 -> 15672), so
# each of the brokers the large content tests use is reached by its own.
set -euo pipefail

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

amqp_port=5672
prev=
for arg in "$@"; do
  if [ "$prev" = --broker ] && [[ $arg =~ :([0-9]+)/?$ ]]; then
    amqp_port=${BASH_REMATCH[1]}
  fi
  prev=$arg
done
http=http://127.0.0.1:$((amqp_port + 10000))

args=()
while [ $# -gt 0 ]; do
  if [ "$1" = --queue ] && [ $# -gt 1 ]; then
    queue=$2
    encoded=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$queue")
    curl -fsS -o /dev/null -u guest:guest -X PUT -H 'content-type: application/json' \
      -d '{"durable":false}' "$http/api/queues/%2F/$encoded"
    args+=(--queue "/queues/$queue")
    shift 2
  else
    args+=("$1")
    shift
  fi
done

exec "$dir/shim.orig.sh" "${args[@]}"
