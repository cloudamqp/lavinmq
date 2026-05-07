# Monitoring

## Prometheus Metrics

LavinMQ exposes metrics in Prometheus format on a dedicated HTTP endpoint at `http://<bind>:<port>/metrics`.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `metrics_http_bind` | `[main]` | `127.0.0.1` | Bind address for the metrics endpoint |
| `metrics_http_port` | `[main]` | `15692` | Port for the metrics endpoint |

Metrics are prefixed with `lavinmq_`. Both `/metrics` and `/metrics/detailed` accept a `prefix` query parameter (defaults to `lavinmq`) and a `vhost` parameter (repeatable) to filter by vhost.

### `/metrics`

Aggregate broker, queue, runtime, and clustering metrics.

#### Broker

| Metric | Type | Description |
|--------|------|-------------|
| `identity_info` | gauge | Server version and hostname as labels |
| `connections_opened_total` | counter | Total number of connections opened |
| `connections_closed_total` | counter | Total number of connections closed or terminated |
| `channels_opened_total` | counter | Total number of channels opened |
| `channels_closed_total` | counter | Total number of channels closed |
| `queues_declared_total` | counter | Total number of queues declared |
| `queues_deleted_total` | counter | Total number of queues deleted |
| `process_open_fds` | gauge | Open file descriptors |
| `process_open_tcp_sockets` | gauge | Open TCP sockets |
| `process_resident_memory_bytes` | gauge | Resident memory in bytes |
| `process_max_fds` | gauge | File descriptor limit |
| `resident_memory_limit_bytes` | gauge | Memory high watermark in bytes |
| `disk_space_available_bytes` | gauge | Disk space available in bytes |

#### Queues

| Metric | Type | Description |
|--------|------|-------------|
| `connections` | gauge | Connections currently open |
| `channels` | gauge | Channels currently open |
| `consumers` | gauge | Consumers currently connected |
| `queues` | gauge | Queues available |
| `queue_messages_ready` | gauge | Messages ready to be delivered to consumers |
| `queue_messages_unacked` | gauge | Messages delivered to consumers but not yet acknowledged |
| `queue_messages` | gauge | Sum of ready and unacknowledged messages (total queue depth) |

#### Global message stats

| Metric | Type | Description |
|--------|------|-------------|
| `global_messages_delivered_total` | counter | Total messages delivered to consumers |
| `global_messages_redelivered_total` | counter | Total messages redelivered to consumers |
| `global_messages_acknowledged_total` | counter | Total messages acknowledged by consumers |
| `global_messages_confirmed_total` | counter | Total messages confirmed to publishers |

#### Runtime and clustering

| Metric | Type | Description |
|--------|------|-------------|
| `uptime` | counter | Server uptime in seconds |
| `cpu_system_time_total` | counter | Total CPU system time |
| `cpu_user_time_total` | counter | Total CPU user time |
| `stats_collection_duration_seconds_total` | gauge | Total time to collect metrics (`stats_loop`) |
| `stats_rates_collection_duration_seconds` | gauge | Time to update stats rates |
| `stats_system_collection_duration_seconds` | gauge | Time to collect system metrics |
| `total_connected_followers` | gauge | Number of follower nodes connected |
| `follower_lag_in_bytes` | gauge | Bytes not yet synchronized to a follower (labeled by `id`) |
| `mfile_count` | gauge | Number of memory-mapped files (`MFile` instances) currently open |

#### Garbage collection

Crystal GC stats, prefixed with `<prefix>_gc_`.

| Metric | Type | Description |
|--------|------|-------------|
| `gc_heap_size_bytes` | gauge | Heap size in bytes (including the area unmapped to OS) |
| `gc_free_bytes` | gauge | Total bytes contained in free and unmapped blocks |
| `gc_unmapped_bytes` | gauge | Amount of memory unmapped to OS |
| `gc_since_recent_collection_allocated_bytes` | gauge | Bytes allocated since the recent GC |
| `gc_before_recent_collection_allocated_bytes_total` | counter | Bytes allocated before the recent GC (value may wrap) |
| `gc_non_candidate_bytes` | gauge | Bytes not considered candidates for GC |
| `gc_cycles_total` | counter | Garbage collection cycle number (value may wrap) |
| `gc_marker_threads` | gauge | Marker threads (excluding the initiating one) |
| `gc_since_recent_collection_reclaimed_bytes` | gauge | Approximate reclaimed bytes after recent GC |
| `gc_before_recent_collection_reclaimed_bytes_total` | counter | Approximate reclaimed bytes before the recent GC (value may wrap) |
| `gc_since_recent_collection_explicitly_freed_bytes` | counter | Bytes freed explicitly since the recent GC |
| `gc_from_os_obtained_bytes_total` | counter | Total memory obtained from OS, in bytes |

#### Scrape telemetry

Self-metrics about the metrics endpoint itself, always prefixed with `telemetry_` regardless of the `prefix` query parameter.

| Metric | Type | Description |
|--------|------|-------------|
| `telemetry_scrape_duration_seconds` | gauge | Duration of metrics collection in seconds |
| `telemetry_scrape_mem` | gauge | Memory used for metrics collection in bytes |

### `/metrics/detailed`

Per-resource metrics selected via the `family` query parameter (repeatable). Without a `family` parameter the response is empty.

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
