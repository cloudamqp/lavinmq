#!/usr/bin/env bash
# extras/benchmark.sh
#
# Quick performance smoke test across common AMQP scenarios. Useful before/after
# a change to spot regressions.
#
# Assumes a LavinMQ broker is already listening on amqp://guest:guest@localhost.
# Override with PERF=, CTL=, URI=, DURATION= env vars.
#
#   make bin/lavinmqperf bin/lavinmqctl
#   bin/lavinmq --data-dir ./tmp/data &
#   extras/benchmark.sh

set -u

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PERF=${PERF:-bin/lavinmqperf}
CTL=${CTL:-bin/lavinmqctl}
DURATION=${DURATION:-10}
URI=${URI:-amqp://guest:guest@localhost}
CONNS=${CONNS:-64}

if [ ! -x "$PERF" ]; then
    echo "$PERF not found or not executable. Build it with: make $PERF" >&2
    exit 1
fi

if [ ! -x "$CTL" ]; then
    echo "$CTL not found or not executable. Build it with: make $CTL" >&2
    exit 1
fi

run() {
    local label="$1"; shift
    echo
    echo "=============================================================="
    echo "  $label"
    echo "  args: $*"
    echo "=============================================================="
    "$PERF" amqp throughput --uri "$URI" -z "$DURATION" -q "$@"
}

churn() {
    local subcmd="$1" label="$2"
    echo
    echo "=============================================================="
    echo "  $label"
    echo "  cmd:  lavinmqperf amqp $subcmd"
    echo "=============================================================="
    "$PERF" amqp "$subcmd" --uri "$URI"
}

# --- baseline ---------------------------------------------------------------

run "Baseline 1-to-1 (transient, 16 B)" \
    -u perf-baseline -x 1 -y 1

run "1-to-1 with properties (headers={})" \
    -u perf-props -x 1 -y 1 --properties '{"headers": {}}'

# --- one-sided --------------------------------------------------------------

run "Publish only (no consumer)" \
    -u perf-pubonly -x 1 -y 0

echo
echo "Pre-filling perf-consonly (${DURATION}s of publishes) for the consume-only test..."
# Declare the queue first — with no consumer, the publisher never declares it
# and default-exchange publishes would be dropped.
"$CTL" create_queue --durable perf-consonly >/dev/null
"$PERF" amqp throughput --uri "$URI" -z "$DURATION" -q -u perf-consonly -x 1 -y 0 >/dev/null

run "Consume only (drain pre-filled queue)" \
    -u perf-consonly -x 0 -y 1

# --- multiple queues (fanout) ----------------------------------------------

run "Fanout: 1 publisher -> 4 queues -> 4 consumers" \
    -x 1 -y 4 \
    --exchange amq.fanout \
    --queue-pattern 'perf-fan-%' \
    --queue-pattern-from 1 --queue-pattern-to 4

# --- larger messages -------------------------------------------------------

run "Large messages (100 KB body)" \
    -u perf-large -x 1 -y 1 -s 100000

# --- prefetch / ack tuning -------------------------------------------------

run "Prefetch 1000, ack every 100" \
    -u perf-prefetch -x 1 -y 1 -P 1000 -a 100

# --- publish confirm -------------------------------------------------------

run "Publish confirm (max 1000 unconfirmed)" \
    -u perf-confirm -x 1 -y 1 -c 1000

# --- transactions ----------------------------------------------------------

run "Publish in tx (commit every 100 msgs)" \
    -u perf-txpub -x 1 -y 1 -t 100

run "Ack in tx (prefetch 100, ack+commit every 100)" \
    -u perf-txack -x 1 -y 1 -P 100 -a 1 -T 100

# --- stream queues ---------------------------------------------------------

run "Stream queue 1-to-1 (offset=first, prefetch 1000)" \
    -u perf-stream -x 1 -y 1 \
    --queue-args '{"x-queue-type": "stream"}' \
    --consumer-args '{"x-stream-offset": "first"}' \
    -P 1000 -a 1

run "Stream queue + properties (headers={})" \
    -u perf-stream-props -x 1 -y 1 \
    --queue-args '{"x-queue-type": "stream"}' \
    --consumer-args '{"x-stream-offset": "first"}' \
    --properties '{"headers": {}}' \
    -P 1000 -a 1

# --- multi-threading -------------------------------------------------------
# Many concurrent connections, exercising broker scheduling across threads.

run "Fan-in: $CONNS publishers, 1 consumer (single queue)" \
    -u perf-mt-fanin -x "$CONNS" -y 1

run "Competing consumers: 1 publisher, $CONNS consumers (single queue)" \
    -u perf-mt-compete -x 1 -y "$CONNS"

run "Symmetric: $CONNS publishers, $CONNS consumers (single queue)" \
    -u perf-mt-sym -x "$CONNS" -y "$CONNS"

run "Sharded: $CONNS publishers, $CONNS consumers across $CONNS queues" \
    -x "$CONNS" -y "$CONNS" \
    --queue-pattern 'perf-mt-shard-%' \
    --queue-pattern-from 1 --queue-pattern-to "$CONNS"

run "Stream multi-consumer: 1 publisher, $CONNS consumers" \
    -u perf-mt-stream -x 1 -y "$CONNS" \
    --queue-args '{"x-queue-type": "stream"}' \
    --consumer-args '{"x-stream-offset": "first"}' \
    -P 1000 -a 1

# --- churn -----------------------------------------------------------------

churn bind-churn       "Bind churn (transient + durable queues)"
churn queue-churn      "Queue churn (create/delete transient + durable)"
churn connection-churn "Connection churn (open/close conn + channel)"

echo
echo "Cleaning up test queues..."
for q in \
    perf-baseline \
    perf-props \
    perf-pubonly \
    perf-consonly \
    perf-fan-1 perf-fan-2 perf-fan-3 perf-fan-4 \
    perf-large \
    perf-prefetch \
    perf-confirm \
    perf-txpub \
    perf-txack \
    perf-stream \
    perf-stream-props \
    perf-mt-fanin \
    perf-mt-compete \
    perf-mt-sym \
    perf-mt-stream
do
    "$CTL" delete_queue "$q" >/dev/null 2>&1 || true
done
for i in $(seq 1 "$CONNS"); do
    "$CTL" delete_queue "perf-mt-shard-$i" >/dev/null 2>&1 || true
done
echo "Done."
