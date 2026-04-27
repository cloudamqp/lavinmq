# Monitoring

## Prometheus Metrics

LavinMQ exposes metrics in Prometheus format on a dedicated HTTP endpoint.

- Default URL: `http://localhost:15692/metrics`
- Configurable via `metrics_http_bind` and `metrics_http_port`

### Key Metrics

Metrics are prefixed with `lavinmq_` and include:

**Server-level:**
- Connection count, channel count, queue count
- Memory usage, disk space
- Message rates (publish, deliver, ack, confirm, reject, redeliver)

**Per-queue:**
- Message count (ready, unacked, total)
- Message rates
- Consumer count

**Per-exchange:**
- Message rates (publish in, publish out, unroutable)

**Per-connection:**
- Data rates, channel count

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

When `log_exchange` is enabled, these events are published to the `amq.topic` exchange with routing keys matching the event name.

## Statistics

LavinMQ collects rate statistics at a configurable interval:

| Config Key | Default | Description |
|-----------|---------|-------------|
| `stats_interval` | `5000` | Collection interval (ms) |
| `stats_log_size` | `120` | Number of samples to retain (10 min at 5s interval) |

## Logging

| Config Key | Description |
|-----------|-------------|
| `log_level` | Log level: debug, info, warn, error |
| `log_file` | Log file path (stdout if not set) |

## Log Streaming

The management API provides live log streaming via Server-Sent Events at `GET /api/logs`.

## Signal Handling

| Signal | Behavior |
|--------|----------|
| `SIGTERM` / `SIGINT` | Graceful shutdown: stop accepting connections, close existing connections, flush to disk, exit |
| `SIGUSR1` | Print GC statistics and fiber dump to stderr |
