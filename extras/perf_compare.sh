#!/usr/bin/env bash
# extras/perf_compare.sh
#
# A/B performance comparison of two lavinmq binaries on the same machine.
# Each scenario is run ROUNDS times per binary, interleaved (B,H / H,B / ...),
# with a fresh broker and data dir per run, and the paired head/base ratios
# are compared. Exits 1 if any scenario regresses.
#
# A metric is a regression when the median head/base ratio is below
# 1 - THRESHOLD/100 AND head was slower in at least ROUNDS-1 rounds, so a
# single noisy round can't fail the check on its own.
#
#   extras/perf_compare.sh old/bin/lavinmq bin/lavinmq
#
# Override with PERF=, ROUNDS=, DURATION=, WARMUP=, THRESHOLD=, OUT_DIR=,
# SCENARIO_FILTER= (regex on scenario name) env vars.

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 BASE_BIN HEAD_BIN" >&2
    exit 2
fi

BASE_BIN=$(realpath "$1")
HEAD_BIN=$(realpath "$2")

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PERF=${PERF:-bin/lavinmqperf}
ROUNDS=${ROUNDS:-5}
DURATION=${DURATION:-10}
WARMUP=${WARMUP:-3}
THRESHOLD=${THRESHOLD:-5}
OUT_DIR=${OUT_DIR:-tmp/perf}
SCENARIO_FILTER=${SCENARIO_FILTER:-}

for bin in "$BASE_BIN" "$HEAD_BIN" "$PERF"; do
    if [ ! -x "$bin" ]; then
        echo "$bin not found or not executable" >&2
        exit 2
    fi
done
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

# Pin the broker and the load generator to separate halves of the CPUs so
# they don't compete for the same cores.
NCPU=$(nproc)
if [ "$NCPU" -ge 2 ] && command -v taskset >/dev/null; then
    HALF=$((NCPU / 2))
    BROKER_CPUS="0-$((HALF - 1))"
    CLIENT_CPUS="$HALF-$((NCPU - 1))"
    BROKER_PIN=(taskset -c "$BROKER_CPUS")
    CLIENT_PIN=(taskset -c "$CLIENT_CPUS")
else
    BROKER_PIN=()
    CLIENT_PIN=()
fi

# name|protocol|gated metrics|lavinmqperf args
# Only the publish rate is gated where publishers outpace the consumer, as the
# consume rate then depends on backlog dynamics and is too noisy.
SCENARIOS=(
    "transient-1to1|amqp|pub,consume|-u perf-baseline -x 1 -y 1"
    "persistent-1to1|amqp|pub,consume|-u perf-persistent -x 1 -y 1 -p"
    "confirm-1to1|amqp|pub,consume|-u perf-confirm -x 1 -y 1 -c 1000"
    "large-100kb|amqp|pub,consume|-u perf-large -x 1 -y 1 -s 100000"
    "fanout-1to4|amqp|pub,consume|-x 1 -y 4 --exchange amq.fanout --queue-pattern perf-fan-% --queue-pattern-from 1 --queue-pattern-to 4"
    "stream-1to1|amqp|pub|-u perf-stream -x 1 -y 1 --queue-args {\"x-queue-type\":\"stream\"} --consumer-args {\"x-stream-offset\":\"first\"} -P 1000 -a 1"
    "fanin-16to1|amqp|pub|-u perf-fanin -x 16 -y 1"
    "mqtt-1to1|mqtt|pub|-x 1 -y 1 --clean-session"
)

mkdir -p "$OUT_DIR"
RESULTS="$OUT_DIR/runs.jsonl"
: > "$RESULTS"
BROKER_PID=

stop_broker() {
    [ -n "$BROKER_PID" ] || return 0
    kill "$BROKER_PID" 2>/dev/null || true
    # The broker is our child, so wait for it (kill -0 would see the zombie)
    # and SIGKILL it from a watchdog if it doesn't exit in time.
    (sleep 10 && echo "Broker did not exit within 10s, sending SIGKILL" >&2 &&
        kill -9 "$BROKER_PID" 2>/dev/null) &
    local watchdog=$!
    wait "$BROKER_PID" 2>/dev/null || true
    kill "$watchdog" 2>/dev/null || true
    wait "$watchdog" 2>/dev/null || true
    BROKER_PID=
}
trap stop_broker EXIT

start_broker() {
    local bin="$1" data_dir="$2"
    "${BROKER_PIN[@]}" "$bin" --data-dir "$data_dir" >"$data_dir.log" 2>&1 &
    BROKER_PID=$!
    if ! curl -s -o /dev/null --retry 30 --retry-delay 1 --retry-connrefused http://localhost:15672/; then
        echo "Broker $bin failed to start, log:" >&2
        cat "$data_dir.log" >&2
        exit 2
    fi
}

# run_one SCENARIO PROTO METRICS ARGS SIDE BIN ROUND
run_one() {
    local name="$1" proto="$2" metrics="$3" args="$4" side="$5" bin="$6" round="$7"
    local data_dir uri out
    data_dir=$(mktemp -d "${TMPDIR:-/tmp}/perf-data.XXXXXX")
    start_broker "$bin" "$data_dir"
    if [ "$proto" = mqtt ]; then
        uri="mqtt://guest:guest@localhost:1883"
    else
        uri="amqp://guest:guest@localhost"
    fi
    # shellcheck disable=SC2086 # args are intentionally word-split
    timeout $((WARMUP + 60)) "${CLIENT_PIN[@]}" "$PERF" "$proto" throughput --uri "$uri" -z "$WARMUP" -q -j $args >/dev/null
    # The JSON summary is the last line; mqtt throughput prints more before it
    # shellcheck disable=SC2086
    out=$(timeout $((DURATION + 60)) "${CLIENT_PIN[@]}" "$PERF" "$proto" throughput --uri "$uri" -z "$DURATION" -q -j $args | tail -n 1)
    stop_broker
    rm -rf "$data_dir" "$data_dir.log"
    jq -c --arg s "$name" --arg m "$metrics" --arg side "$side" --argjson r "$round" \
        '{scenario: $s, metrics: ($m | split(",")), side: $side, round: $r,
          pub: .avg_pub_rate, consume: .avg_consume_rate}' \
        <<<"$out" | tee -a "$RESULTS"
}

echo "Base: $BASE_BIN"
echo "Head: $HEAD_BIN"
echo "Rounds: $ROUNDS, duration: ${DURATION}s, warmup: ${WARMUP}s, threshold: ${THRESHOLD}%"
echo "CPUs: $NCPU, broker: ${BROKER_CPUS:-unpinned}, client: ${CLIENT_CPUS:-unpinned}"

for scenario in "${SCENARIOS[@]}"; do
    IFS='|' read -r name proto metrics args <<<"$scenario"
    if [ -n "$SCENARIO_FILTER" ] && ! [[ $name =~ $SCENARIO_FILTER ]]; then
        continue
    fi
    echo
    echo "=== $name ($proto throughput $args)"
    for round in $(seq 1 "$ROUNDS"); do
        # Alternate which side runs first to cancel out drift over time
        if [ $((round % 2)) -eq 1 ]; then
            run_one "$name" "$proto" "$metrics" "$args" base "$BASE_BIN" "$round"
            run_one "$name" "$proto" "$metrics" "$args" head "$HEAD_BIN" "$round"
        else
            run_one "$name" "$proto" "$metrics" "$args" head "$HEAD_BIN" "$round"
            run_one "$name" "$proto" "$metrics" "$args" base "$BASE_BIN" "$round"
        fi
    done
done

jq -s --argjson threshold "$THRESHOLD" --argjson rounds "$ROUNDS" '
  def median: sort | if length % 2 == 1 then .[length / 2 | floor]
                     else (.[length / 2 - 1] + .[length / 2]) / 2 end;
  group_by(.scenario) | map(
    . as $runs | ($runs[0].scenario) as $s |
    $runs[0].metrics | map(. as $m |
      ($runs | map(select(.side == "base")) | sort_by(.round)) as $base |
      ($runs | map(select(.side == "head")) | sort_by(.round)) as $head |
      ($base | map(.[$m])) as $b |
      ([range(0; $base | length)] | map(if $base[.][$m] > 0 then $head[.][$m] / $base[.][$m] else 0 end)) as $ratios |
      ($ratios | median) as $ratio |
      {
        scenario: $s,
        metric: $m,
        base: ($b | median),
        head: ($head | map(.[$m]) | median),
        change_pct: (($ratio - 1) * 100),
        slower_rounds: ($ratios | map(select(. < 1)) | length),
        range_pct: [(($ratios | min) - 1) * 100, (($ratios | max) - 1) * 100],
        broken: (($b | median) == 0 or ($head | map(.[$m]) | median) == 0),
        regression: ($ratio < (1 - $threshold / 100)
                     and ($ratios | map(select(. < 1)) | length) >= ([$rounds - 1, 1] | max))
      })
  ) | flatten
' "$RESULTS" > "$OUT_DIR/results.json"

{
    echo "### Performance comparison"
    echo
    echo "Median of $ROUNDS interleaved rounds, ${DURATION}s each. Regression threshold: -${THRESHOLD}%."
    echo
    echo "| Scenario | Metric | Base msgs/s | Head msgs/s | Change | Round range | Slower rounds | |"
    echo "|---|---|---:|---:|---:|---:|---:|---|"
    jq -r --argjson rounds "$ROUNDS" '.[] |
      "| \(.scenario) | \(.metric) | \(.base | round) | \(.head | round) | \(.change_pct * 10 | round / 10)% | \(.range_pct | map(. * 10 | round / 10 | tostring + "%") | join(" … ")) | \(.slower_rounds)/\($rounds) | \(if .broken then ":warning: no data" elif .regression then ":x:" else ":white_check_mark:" end) |"' \
      "$OUT_DIR/results.json"
} > "$OUT_DIR/summary.md"

echo
cat "$OUT_DIR/summary.md"

if jq -e 'any(.broken)' "$OUT_DIR/results.json" >/dev/null; then
    echo
    echo "A scenario reported no throughput, the benchmark is broken" >&2
    exit 1
fi
if jq -e 'any(.regression)' "$OUT_DIR/results.json" >/dev/null; then
    echo
    echo "Performance regression detected" >&2
    exit 1
fi
