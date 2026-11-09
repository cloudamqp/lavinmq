#!/usr/bin/env bash
# extras/perf_compare.sh
#
# A/B performance comparison of two lavinmq binaries on the same machine.
# Each scenario is run ROUNDS times per binary, interleaved (B,H / H,B / ...),
# with a fresh broker and data dir per run, and the paired head/base ratios
# are compared. Exits 1 if any scenario regresses.
#
# A metric regresses when the median head/base ratio is worse than the
# threshold AND head was worse in at least ROUNDS-1 rounds, so a single noisy
# round can't fail the check on its own.
#
#   extras/perf_compare.sh old/bin/lavinmq bin/lavinmq
#   extras/perf_compare.sh --validate scenarios.json
#
# Scenarios are read from SCENARIOS_FILE (default extras/perf_scenarios.json),
# a JSON list of at most 12 scenarios:
#
#   name        unique, ^[a-z0-9-]{1,40}$
#   protocol    "amqp" or "mqtt" for a measured `lavinmqperf <protocol>
#               throughput <args>` run, or "none" to measure only the
#               background load and HTTP API
#   args        lavinmqperf throughput arguments (empty for "none")
#   metrics     the gated metrics, from:
#                 pub, consume  msgs/s of the measured run (higher is better)
#                 http          HTTP API requests/s (higher is better)
#                 rss           broker peak RSS in MB (lower is better)
#                 write         bytes the broker dirtied per published message,
#                               from /proc/PID/io. Pages are counted each time
#                               they go from clean to dirty, so syncing pages
#                               that are then written again shows up here
#                               (lower is better)
#                 disk          bytes written to the disk devices per published
#                               message, including journal and metadata
#                               (lower is better)
#   background  optional, up to 4 lavinmqperf processes running alongside the
#               measured run: {protocol, command, args}, where command is
#               throughput, queue-count or connection-count (amqp only). The
#               count commands keep their queues/connections open.
#   http        optional, {paths: ["/api/..."], concurrency: 1-4}: GET loops
#               against the HTTP API during the measured window
#   why         one sentence
#
# Published messages for write/disk are counted over all traffic, from
# /api/overview. In CI the list is written by Claude, so it's validated before
# anything runs.
#
# Override with PERF=, ROUNDS=, DURATION=, WARMUP=, THRESHOLD= (pub, consume,
# http), RESOURCE_THRESHOLD= (rss, write, disk), BG_SETTLE=, OUT_DIR=,
# SCENARIOS_FILE=, SCENARIO_FILTER= (regex on scenario name) env vars.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }

# Args may not set the options the script controls for throughput (-z, -j,
# -q) or point any client at another broker (--uri, and -u for mqtt).
validate_scenarios() {
    local file="$1"
    if ! jq -e '
      def arg_ok: type == "string" and test("^[A-Za-z0-9_%{}\":,.=@/+-]+$")
                  and (test("^--uri") | not);
      def throughput_args($proto): type == "array" and all(.[];
            arg_ok and (test("^(-z|-j|-q|--time|--json|--quiet)") | not)
            and (($proto == "mqtt" and test("^-u")) | not));
      type == "array" and length >= 1 and length <= 12
      and ([.[].name] | unique | length) == length
      and all(.[];
        .protocol as $proto
        | (.name | type == "string" and test("^[a-z0-9-]{1,40}$"))
        and ($proto | IN("amqp", "mqtt", "none"))
        and (.metrics | type == "array" and length > 0
             and all(.[]; IN("pub", "consume", "http", "rss", "write", "disk")))
        and (if $proto == "none"
             then (.args | type == "array" and length == 0)
                  and (.metrics | all(.[]; IN("pub", "consume") | not))
                  and (((.background // []) | length) > 0 or .http != null)
             else (.args | throughput_args($proto)) end)
        and ((.background // []) | type == "array" and length <= 4 and all(.[];
              .protocol as $bp | .command as $cmd
              | ($bp | IN("amqp", "mqtt"))
              and ($cmd | IN("throughput", "queue-count", "connection-count"))
              and ($cmd == "throughput" or $bp == "amqp")
              and (if $cmd == "throughput" then (.args | throughput_args($bp))
                   else (.args | type == "array" and all(.[]; arg_ok)) end)))
        and (if (.metrics | index("http")) or .http != null
             then (.http | type == "object"
                   and (.paths | type == "array" and length >= 1 and length <= 5
                        and all(.[]; type == "string" and test("^/api/[A-Za-z0-9_%/.?=&-]*$")))
                   and ((.concurrency // 1) | type == "number" and . >= 1 and . <= 4))
                  and (.metrics | index("http") != null)
             else true end))
    ' "$file" >/dev/null 2>&1; then
        echo "Invalid scenarios in $file, see the format at the top of $0" >&2
        return 1
    fi
}

if [ "${1:-}" = --validate ] && [ $# -eq 2 ]; then
    validate_scenarios "$2" || exit 2
    echo "$2: $(jq length "$2") valid scenarios"
    exit 0
fi

if [ $# -ne 2 ]; then
    echo "Usage: $0 BASE_BIN HEAD_BIN" >&2
    echo "       $0 --validate SCENARIOS_FILE" >&2
    exit 2
fi

BASE_BIN=$(realpath "$1")
HEAD_BIN=$(realpath "$2")
SCENARIOS_FILE=$(realpath "${SCENARIOS_FILE:-$SCRIPT_DIR/perf_scenarios.json}")
validate_scenarios "$SCENARIOS_FILE" || exit 2

cd "$SCRIPT_DIR/.."

PERF=${PERF:-bin/lavinmqperf}
ROUNDS=${ROUNDS:-5}
DURATION=${DURATION:-10}
WARMUP=${WARMUP:-3}
THRESHOLD=${THRESHOLD:-5}
RESOURCE_THRESHOLD=${RESOURCE_THRESHOLD:-10}
BG_SETTLE=${BG_SETTLE:-3}
OUT_DIR=${OUT_DIR:-tmp/perf}
SCENARIO_FILTER=${SCENARIO_FILTER:-}

for bin in "$BASE_BIN" "$HEAD_BIN" "$PERF"; do
    if [ ! -x "$bin" ]; then
        echo "$bin not found or not executable" >&2
        exit 2
    fi
done

# Pin the broker and the load generators to separate halves of the CPUs so
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

mkdir -p "$OUT_DIR"
RESULTS="$OUT_DIR/runs.jsonl"
: > "$RESULTS"
BROKER_PID=
BG_PIDS=()
HTTP_PIDS=()

# queue-count and connection-count wait for input after creating their
# queues/connections, and create more on EOF. Feed them a FIFO that this
# script holds open and never writes to, so they block and keep what they made.
HOLD=$(mktemp -u "${TMPDIR:-/tmp}/perf-hold.XXXXXX")
mkfifo "$HOLD"
exec {HOLD_FD}<>"$HOLD"

stop_pids() {
    local pid
    for pid in "$@"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "$@"; do
        wait "$pid" 2>/dev/null || true
    done
}

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

cleanup() {
    stop_pids "${HTTP_PIDS[@]}" "${BG_PIDS[@]}"
    stop_broker
    rm -f "$HOLD"
}
trap cleanup EXIT

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

uri_for() {
    if [ "$1" = mqtt ]; then
        echo "mqtt://guest:guest@localhost:1883"
    else
        echo "amqp://guest:guest@localhost"
    fi
}

broker_write_bytes() { awk '/^write_bytes:/ { print $2 }' "/proc/$BROKER_PID/io"; }
broker_peak_rss_mb() { awk '/^VmHWM:/ { printf "%.1f", $2 / 1024 }' "/proc/$BROKER_PID/status"; }
disk_bytes() {
    awk '$3 ~ /^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme[0-9]+n[0-9]+)$/ { s += $10 } END { printf "%.0f", s * 512 }' /proc/diskstats
}
published() {
    curl -sf -u guest:guest http://localhost:15672/api/overview | jq '.message_stats.publish // 0'
}

# start_background INDEX
start_background() {
    local i="$1" n j proto cmd extra args
    n=$(jq ".[$i].background // [] | length" "$SCENARIOS_FILE")
    for ((j = 0; j < n; j++)); do
        proto=$(jq -r ".[$i].background[$j].protocol" "$SCENARIOS_FILE")
        cmd=$(jq -r ".[$i].background[$j].command" "$SCENARIOS_FILE")
        mapfile -t args < <(jq -r ".[$i].background[$j].args[]" "$SCENARIOS_FILE")
        extra=()
        if [ "$cmd" = throughput ]; then
            extra=(-q)
        fi
        timeout $((WARMUP + DURATION + BG_SETTLE + 120)) "${CLIENT_PIN[@]}" \
            "$PERF" "$proto" "$cmd" --uri "$(uri_for "$proto")" "${extra[@]}" "${args[@]}" \
            0<&"$HOLD_FD" >/dev/null 2>&1 &
        BG_PIDS+=($!)
    done
    if [ "$n" -gt 0 ]; then
        sleep "$BG_SETTLE"
        for j in "${BG_PIDS[@]}"; do
            if ! kill -0 "$j" 2>/dev/null; then
                echo "A background lavinmqperf process exited early" >&2
                exit 2
            fi
        done
    fi
}

# start_http INDEX OUT: GET loops until DURATION has passed, one
# "status seconds" line per request
start_http() {
    local i="$1" out="$2" concurrency k paths end
    concurrency=$(jq ".[$i].http.concurrency // 1" "$SCENARIOS_FILE")
    mapfile -t paths < <(jq -r ".[$i].http.paths[]" "$SCENARIOS_FILE")
    end=$(($(date +%s) + DURATION))
    : > "$out"
    for ((k = 0; k < concurrency; k++)); do
        (
            while [ "$(date +%s)" -lt "$end" ]; do
                for p in "${paths[@]}"; do
                    curl -s -o /dev/null --max-time 10 -u guest:guest -w '%{http_code} %{time_total}\n' \
                        "http://localhost:15672$p" >> "$out.$k" || true
                done
            done
        ) &
        HTTP_PIDS+=($!)
        if [ -n "${CLIENT_CPUS:-}" ]; then
            taskset -pc "$CLIENT_CPUS" "$!" >/dev/null
        fi
    done
}

# run_one INDEX SIDE BIN ROUND
run_one() {
    local i="$1" side="$2" bin="$3" round="$4"
    local name proto metrics data_dir uri out args has_http
    local w0 w1 d0 d1 p0 p1 rss http_file http_count http_errors http_p99
    name=$(jq -r ".[$i].name" "$SCENARIOS_FILE")
    proto=$(jq -r ".[$i].protocol" "$SCENARIOS_FILE")
    metrics=$(jq -r ".[$i].metrics | join(\",\")" "$SCENARIOS_FILE")
    has_http=$(jq ".[$i].http != null" "$SCENARIOS_FILE")
    mapfile -t args < <(jq -r ".[$i].args[]" "$SCENARIOS_FILE")
    # Keep the data dir on a real disk, not tmpfs: fsync/syncfs are free on
    # tmpfs, which would hide regressions in when and how often we sync.
    data_dir=$(mktemp -d "${TMPDIR:-/tmp}/perf-data.XXXXXX")
    http_file="$data_dir.http"
    start_broker "$bin" "$data_dir"
    start_background "$i"
    uri=$(uri_for "$proto")
    if [ "$proto" != none ]; then
        timeout $((WARMUP + 60)) "${CLIENT_PIN[@]}" "$PERF" "$proto" throughput --uri "$uri" -z "$WARMUP" -q -j "${args[@]}" >/dev/null
    fi

    w0=$(broker_write_bytes)
    d0=$(disk_bytes)
    p0=$(published)
    if [ "$has_http" = true ]; then
        start_http "$i" "$http_file"
    fi
    if [ "$proto" != none ]; then
        # The JSON summary is the last line; mqtt throughput prints more before it
        out=$(timeout $((DURATION + 60)) "${CLIENT_PIN[@]}" "$PERF" "$proto" throughput --uri "$uri" -z "$DURATION" -q -j "${args[@]}" | tail -n 1)
    else
        sleep "$DURATION"
        out='{"avg_pub_rate": 0, "avg_consume_rate": 0}'
    fi
    stop_pids "${HTTP_PIDS[@]}"
    HTTP_PIDS=()
    w1=$(broker_write_bytes)
    d1=$(disk_bytes)
    p1=$(published)
    rss=$(broker_peak_rss_mb)
    http_count=0
    http_errors=0
    http_p99=0
    if [ "$has_http" = true ]; then
        # Only 2xx responses count; any other response or a timeout (000)
        # marks the scenario as broken
        http_count=$(cat "$http_file".* 2>/dev/null | awk '$1 ~ /^2/' | wc -l)
        http_errors=$(cat "$http_file".* 2>/dev/null | awk '$1 !~ /^2/' | wc -l)
        http_p99=$(cat "$http_file".* 2>/dev/null | awk '$1 ~ /^2/ { print $2 }' | sort -n |
            awk '{ a[NR] = $1 } END { if (NR) printf "%.1f", a[int((NR - 1) * 0.99) + 1] * 1000; else print 0 }')
    fi

    stop_pids "${BG_PIDS[@]}"
    BG_PIDS=()
    stop_broker
    rm -rf "$data_dir" "$data_dir.log" "$http_file" "$http_file".*
    jq -c --arg s "$name" --arg m "$metrics" --arg side "$side" --argjson r "$round" \
        --argjson msgs "$((p1 - p0))" --argjson wb "$((w1 - w0))" --argjson db "$((d1 - d0))" \
        --argjson rss "$rss" --argjson http "$http_count" --argjson dur "$DURATION" \
        --argjson p99 "$http_p99" --argjson http_errors "$http_errors" \
        '{scenario: $s, metrics: ($m | split(",")), side: $side, round: $r,
          pub: .avg_pub_rate, consume: .avg_consume_rate,
          http: ($http / $dur), http_errors: $http_errors, http_p99_ms: $p99, rss: $rss,
          write: ($wb / ([$msgs, 1] | max)), disk: ($db / ([$msgs, 1] | max)),
          published: $msgs}' \
        <<<"$out" | tee -a "$RESULTS"
}

echo "Base: $BASE_BIN"
echo "Head: $HEAD_BIN"
echo "Rounds: $ROUNDS, duration: ${DURATION}s, warmup: ${WARMUP}s, threshold: ${THRESHOLD}% (rss/write/disk: ${RESOURCE_THRESHOLD}%)"
echo "CPUs: $NCPU, broker: ${BROKER_CPUS:-unpinned}, client: ${CLIENT_CPUS:-unpinned}"
echo "Scenarios: $SCENARIOS_FILE"

for i in $(seq 0 $(($(jq length "$SCENARIOS_FILE") - 1))); do
    name=$(jq -r ".[$i].name" "$SCENARIOS_FILE")
    if [ -n "$SCENARIO_FILTER" ] && ! [[ $name =~ $SCENARIO_FILTER ]]; then
        continue
    fi
    echo
    echo "=== $name: $(jq -c ".[$i] | del(.name, .why)" "$SCENARIOS_FILE")"
    for round in $(seq 1 "$ROUNDS"); do
        # Alternate which side runs first to cancel out drift over time
        if [ $((round % 2)) -eq 1 ]; then
            run_one "$i" base "$BASE_BIN" "$round"
            run_one "$i" head "$HEAD_BIN" "$round"
        else
            run_one "$i" head "$HEAD_BIN" "$round"
            run_one "$i" base "$BASE_BIN" "$round"
        fi
    done
done

jq -s --argjson threshold "$THRESHOLD" --argjson rthreshold "$RESOURCE_THRESHOLD" --argjson rounds "$ROUNDS" '
  def median: sort | if length % 2 == 1 then .[length / 2 | floor]
                     else (.[length / 2 - 1] + .[length / 2]) / 2 end;
  def lower_is_better: IN("rss", "write", "disk");
  group_by(.scenario) | map(
    . as $runs | ($runs[0].scenario) as $s |
    $runs[0].metrics | map(. as $m |
      ($m | lower_is_better) as $lower |
      ($runs | map(select(.side == "base")) | sort_by(.round)) as $base |
      ($runs | map(select(.side == "head")) | sort_by(.round)) as $head |
      ($base | map(.[$m])) as $b |
      ($head | map(.[$m])) as $h |
      # Bytes per message can be ~0 on base (nothing flushed within the run),
      # so divide by at least 1 B/msg and report those changes in bytes
      ($m | IN("write", "disk")) as $bytes |
      ([range(0; $base | length)] | map(
        if $bytes then $h[.] / ([$b[.], 1] | max)
        elif $b[.] > 0 then $h[.] / $b[.]
        elif $lower then (if $h[.] > 0 then 2 else 1 end)
        else 0 end)) as $ratios |
      ($ratios | median) as $ratio |
      ($ratios | map(select(if $lower then . > 1 else . < 1 end)) | length) as $worse |
      {
        scenario: $s,
        metric: $m,
        lower_is_better: $lower,
        base: ($b | median),
        head: ($h | median),
        change_pct: (($ratio - 1) * 100),
        change_bytes: (if $bytes and ($b | median) < 1 then ($h | median) - ($b | median) else null end),
        worse_rounds: $worse,
        range_pct: [(($ratios | min) - 1) * 100, (($ratios | max) - 1) * 100],
        broken: ((if $lower then ($m == "rss" and (($b | median) == 0 or ($h | median) == 0))
                  else (($b | median) == 0 or ($h | median) == 0) end)
                 or ($m == "http" and ($runs | any(.http_errors > 0)))),
        regression: ((if $lower then $ratio > (1 + $rthreshold / 100)
                      else $ratio < (1 - $threshold / 100) end)
                     and $worse >= ([$rounds - 1, 1] | max))
      })
  ) | flatten
' "$RESULTS" > "$OUT_DIR/results.json"

{
    echo "### Performance comparison"
    echo
    echo "Median of $ROUNDS interleaved rounds, ${DURATION}s each. Regression threshold: ${THRESHOLD}% for throughput, ${RESOURCE_THRESHOLD}% for rss, write and disk, where lower is better."
    echo
    echo "| Scenario | Metric | Base | Head | Change | Round range | Worse rounds | |"
    echo "|---|---|---:|---:|---:|---:|---:|---|"
    jq -r --argjson rounds "$ROUNDS" '
      def metric_label: {pub: "pub (msgs/s)", consume: "consume (msgs/s)", http: "http (req/s)",
                  rss: "rss (MB)", write: "write (B/msg)", disk: "disk (B/msg)"}[.];
      def num: if . >= 100 then round else . * 10 | round / 10 end;
      .[] |
      "| \(.scenario) | \(.metric | metric_label) | \(.base | num) | \(.head | num) | \(if .change_bytes != null then "\(if .change_bytes >= 0 then "+" else "" end)\(.change_bytes | num) B/msg | base ~0" else "\(.change_pct * 10 | round / 10)% | \(.range_pct | map(. * 10 | round / 10 | tostring + "%") | join(" … "))" end) | \(.worse_rounds)/\($rounds) | \(if .broken then ":warning: no data" elif .regression then ":x:" else ":white_check_mark:" end) |"' \
      "$OUT_DIR/results.json"
} > "$OUT_DIR/summary.md"

echo
cat "$OUT_DIR/summary.md"

if jq -e 'any(.broken)' "$OUT_DIR/results.json" >/dev/null; then
    echo
    echo "A scenario reported no throughput or got HTTP errors, the benchmark is broken" >&2
    exit 1
fi
if jq -e 'any(.regression)' "$OUT_DIR/results.json" >/dev/null; then
    echo
    echo "Performance regression detected" >&2
    exit 1
fi
