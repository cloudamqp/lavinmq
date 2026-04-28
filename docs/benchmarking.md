# Benchmarking (lavinmqperf)

`lavinmqperf` is a built-in benchmarking tool for measuring LavinMQ performance.

## Usage

```
lavinmqperf [protocol] [scenario] [options]
```

- `protocol` — `amqp` (default) or `mqtt`
- `scenario` — one of the scenarios listed below
- `options` — global options plus scenario-specific options

If no protocol is given, `amqp` is assumed. Run `lavinmqperf` with no arguments to see the banner with available scenarios.

Examples:

```
lavinmqperf amqp throughput --publishers=4 --consumers=4 --size=1024 --time=60
lavinmqperf mqtt throughput --qos=1 --time=30 --uri=mqtt://localhost:1883
lavinmqperf amqp queue-count --queues=10000 --durable
```

## Global Options

These apply to every scenario.

| Flag | Description |
|------|-------------|
| `--uri=URI` | AMQP or MQTT URI. Default: `amqp://guest:guest@localhost` for AMQP scenarios, `mqtt://localhost:1883` for MQTT scenarios. |
| `-h`, `--help` | Show help |
| `-v`, `--version` | Show version |
| `--build-info` | Show build information |

## AMQP Scenarios

### `throughput` — publish and consume throughput

Spawns publishers and consumers and measures sustained publish and consume rates. Optionally measures end-to-end latency.

| Flag | Description |
|------|-------------|
| `-x`, `--publishers=N` | Number of publishers (default 1) |
| `-y`, `--consumers=N` | Number of consumers (default 1) |
| `-s`, `--size=BYTES` | Message body size (default 16) |
| `-V`, `--verify` | Verify the message body on consume |
| `-a`, `--ack=N` | Ack every N consumed messages (default 0 = no-ack mode) |
| `-c`, `--confirm=N` | Confirm publishes; max N unconfirmed in flight |
| `-t`, `--transaction=N` | Publish in transactions, committing every N messages |
| `-T`, `--transaction=N` | Ack in transactions, committing every N messages |
| `-u`, `--queue=NAME` | Queue name (default `perf-test`). Also used as the routing key. |
| `-k`, `--routing-key=KEY` | Routing key (default queue name) |
| `-e`, `--exchange=NAME` | Exchange to publish to (default `""`, i.e. the default exchange) |
| `-r`, `--rate=N` | Max publish rate per publisher in msgs/s (default 0 = unlimited) |
| `-R`, `--consumer-rate=N` | Max consume rate per consumer in msgs/s (default 0 = unlimited) |
| `-p`, `--persistent` | Mark messages as persistent (`delivery_mode=2`) |
| `-P`, `--prefetch=N` | Consumer prefetch count |
| `-g`, `--poll` | Use `basic.get` polling instead of `basic.consume` |
| `-j`, `--json` | Print final summary as JSON |
| `-z`, `--time=SECONDS` | Stop after this many seconds |
| `-q`, `--quiet` | Only print the final summary |
| `-C`, `--pmessages=N` | Stop publishers after publishing N messages total |
| `-D`, `--cmessages=N` | Stop consumers after consuming N messages total |
| `--queue-args=JSON` | Queue declaration arguments as a JSON object |
| `--consumer-args=JSON` | Consumer arguments as a JSON object |
| `--properties=JSON` | Message properties as a JSON object |
| `--random-bodies` | Each message body is randomized |
| `--queue-pattern=PATTERN` | Use multiple queues. `%` is replaced with a number (e.g. `queue-%`). |
| `--queue-pattern-from=N` | Start index for `--queue-pattern` (default 1) |
| `--queue-pattern-to=N` | End index for `--queue-pattern` (default 1) |
| `-l`, `--measure-latency` | Measure end-to-end latency. Requires `--size>=8`. The first 8 bytes of each message carry a timestamp. |

### `queue-count` — performance with many queues

Declares many queues and reports declaration time.

| Flag | Description |
|------|-------------|
| `-q`, `--queues=N` | Number of queues (default 100) |
| `-D`, `--durable` | Declare the queues as durable |
| `--arguments=JSON` | Queue arguments as a JSON object |
| `-n`, `--no-wait` | Use `nowait` so the client does not wait for declaration confirmation |

### `connection-count` — performance with many connections

Opens many connections (and optionally channels and consumers per connection).

| Flag | Description |
|------|-------------|
| `-x`, `--count=N` | Number of connections (default 100) |
| `-c`, `--channels=N` | Channels per connection (default 1) |
| `-C`, `--consumers=N` | Consumers per channel (default 0) |
| `-u`, `--queue=NAME` | Queue name to consume from |
| `-l`, `--localhost` | Connect to a random `127.0.0.0/16` address (useful to spread source ports) |
| `-k`, `--keepalive=IDLE:COUNT:INTERVAL` | TCP keepalive parameters |

### `bind-churn` — binding create/delete rate

Measures `queue.bind` throughput against `amq.direct`. Uses `Benchmark.ips`. No scenario-specific flags; only the global options apply.

### `queue-churn` — queue create/delete rate

Measures transient and durable queue create/delete throughput. Uses `Benchmark.ips`. No scenario-specific flags.

### `connection-churn` — connection open/close rate

Measures connection open/close throughput. Uses `Benchmark.ips`. No scenario-specific flags.

### `channel-churn` — channel open/close rate

Measures channel open/close throughput on a single connection. Uses `Benchmark.ips`. No scenario-specific flags.

### `consumer-churn` — consumer subscribe/cancel rate

Measures `basic.consume` / `basic.cancel` throughput. Uses `Benchmark.ips`. No scenario-specific flags.

## MQTT Scenarios

### `throughput` — MQTT publish and subscribe throughput

| Flag | Description |
|------|-------------|
| `-u`, `--uri=URI` | MQTT broker URI (default `mqtt://localhost:1883`). Overrides the global `--uri`. |
| `-x`, `--publishers=N` | Number of publishers (default 1) |
| `-y`, `--consumers=N` | Number of consumers (default 1) |
| `-s`, `--size=BYTES` | Message body size (default 16) |
| `-V`, `--verify` | Verify the message body on consume |
| `-q`, `--qos=LEVEL` | QoS level: 0 or 1 |
| `-t`, `--topic=NAME` | Topic name (default `perf-test`) |
| `-r`, `--rate=N` | Max publish rate per publisher in msgs/s (default 0 = unlimited) |
| `-R`, `--consumer-rate=N` | Max consume rate per consumer in msgs/s (default 0 = unlimited) |
| `-j`, `--json` | Print final summary as JSON |
| `-z`, `--time=SECONDS` | Stop after this many seconds |
| `-q`, `--quiet` | Only print the final summary |
| `-C`, `--pmessages=N` | Stop publishers after publishing N messages total |
| `-D`, `--cmessages=N` | Stop consumers after consuming N messages total |
| `--random-bodies` | Each message body is randomized |
| `--retain` | Set the retain flag on published messages |
| `--clean-session` | Connect with `clean_session=true` |

## Output

By default, the throughput scenarios print one line per second with current publish/consume rates and (if `--measure-latency` is set) a percentile snapshot. On stop, they print a summary with average rates and aggregate latency percentiles.

Use `-j` / `--json` for machine-readable output:

```json
{
  "elapsed_seconds": 30.0,
  "avg_pub_rate": 125000,
  "avg_consume_rate": 124998,
  "latency_min_ms": 0.412,
  "latency_median_ms": 0.987,
  "latency_p75_ms": 1.215,
  "latency_p95_ms": 2.043,
  "latency_p99_ms": 4.871,
  "latency_count": 3749940,
  "latency_sample_size": 131072
}
```

The latency reservoir holds up to 131,072 samples; `latency_count` is the total number of measured deliveries.

## Stopping a Run

Send `SIGINT` (Ctrl+C) once for a graceful stop with a final summary. Send it twice to abort immediately.
