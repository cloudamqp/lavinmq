# Monitoring

## Prometheus Metrics

LavinMQ exposes metrics in Prometheus format on a dedicated HTTP endpoint at `http://<bind>:<port>/metrics`.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `metrics_http_bind` | `[main]` | `127.0.0.1` | Bind address for the metrics endpoint |
| `metrics_http_port` | `[main]` | `15692` | Port for the metrics endpoint |

Metrics are prefixed with `lavinmq_`. Detailed per-resource metrics are exposed on `/metrics/detailed` and selected via the `family` query parameter (repeatable). The endpoint also accepts a `vhost` parameter to filter by vhost, and both `/metrics` and `/metrics/detailed` accept a `prefix` parameter (defaults to `lavinmq`).

The following metric families are available:

| Family | Description |
|--------|-------------|
| `connection_churn_metrics` | Connection open/close totals |
| `connection_coarse_metrics` | Per-connection bytes in/out and channel count (also accepted as `connection_metrics`) |
| `channel_metrics` | Per-channel metrics |
| `queue_coarse_metrics` | Per-queue ready, unacked, and total message counts; deduplication cache size |
| `queue_consumer_count` | Per-queue consumer count |
| `exchange_metrics` | Per-exchange deduplication cache size |

## Event Types

LavinMQ tracks the following internal events:

| Event | Description |
|-------|-------------|
| `ChannelCreated` | A channel was opened |
| `ChannelClosed` | A channel was closed |
| `ConnectionCreated` | A client connected |
| `ConnectionClosed` | A client disconnected |
| `QueueDeclared` | A queue was declared |
| `QueueDeleted` | A queue was deleted |
| `ClientPublish` | A message was published |
| `ClientPublishConfirm` | A publish was confirmed |
| `ClientDeliver` | A message was delivered to a consumer |
| `ClientDeliverNoAck` | A message was delivered (auto-ack) |
| `ClientGet` | A message was fetched via basic.get |
| `ClientGetNoAck` | A message was fetched via basic.get (auto-ack) |
| `ClientAck` | A message was acknowledged |
| `ClientReject` | A message was rejected |
| `ClientRedeliver` | A message was redelivered |
| `ConsumerAdded` | A consumer was registered |
| `ConsumerRemoved` | A consumer was cancelled |

When `log_exchange` is enabled, server log entries are published to an internal topic exchange named `amq.lavinmq.log`, with the log severity (e.g., `Info`, `Warn`, `Error`) as the routing key. The events listed above are tracked internally for statistics and the management API; they are not published to `amq.lavinmq.log`.

## Statistics

LavinMQ collects rate statistics at a configurable interval:

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `stats_interval` | `[main]` | `5000` | Collection interval (ms) |
| `stats_log_size` | `[main]` | `120` | Number of samples to retain (10 min at 5s interval) |

## Logging

| Config Key | Section | Description |
|-----------|---------|-------------|
| `log_level` | `[main]` | Log level: `trace`, `debug`, `info`, `notice`, `warn`, `error`, `fatal`, `none` |
| `log_file` | `[main]` | Log file path (stdout if not set) |

## Log Streaming

The management API provides live log streaming via Server-Sent Events at `GET /api/livelog`. The `GET /api/logs` endpoint downloads recent server logs as a plain text file.

## Signal Handling

| Signal | Behavior |
|--------|----------|
| `SIGTERM` / `SIGINT` | Graceful shutdown: stop accepting connections, close existing connections, flush to disk, exit |
| `SIGUSR1` | Print GC statistics and fiber dump to stdout |
| `SIGUSR2` | Run garbage collection |
| `SIGHUP` | Reload server configuration |
