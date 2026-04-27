# Streams

Streams are append-only log queues designed for multiple independent consumers. Unlike standard queues, messages are not removed when consumed — each consumer maintains its own read position.

## Declaration

Declare a stream by setting `x-queue-type: stream`:

```
x-queue-type: stream
```

Streams are always durable. They cannot be exclusive or auto-delete.

## Restrictions

The following arguments are not supported on streams:

- `x-dead-letter-exchange`
- `x-dead-letter-routing-key`
- `x-expires`
- `x-delivery-limit`
- `x-overflow`
- `x-single-active-consumer`
- `x-max-priority`

## Consumer Offsets

Each consumer can specify where to start reading from using the `x-stream-offset` argument on `basic.consume`:

| Value | Description |
|-------|-------------|
| `first` | Start from the beginning of the stream |
| `last` | Start from the last available chunk |
| `next` | Start from new messages only (default) |
| (timestamp) | Start from the first message after the given timestamp |
| (integer) | Start from a specific offset number |

Delivered messages include an `x-stream-offset` header with the current offset position.

### Automatic Offset Tracking

Set `x-stream-automatic-offset-tracking: true` on `basic.consume` to have the server automatically persist the consumer's offset position. On reconnect, the consumer resumes from where it left off without needing to specify the offset manually.

## Stream Filtering

Consumers can filter messages based on headers, avoiding unnecessary delivery of unwanted messages.

### Simple String Filter

Publish messages with the `x-stream-filter-value` header set to a string value. Consumers set `x-stream-filter` to match against this value. Only messages with a matching filter value are delivered.

### Key-Value Filtering

Filter on arbitrary message header key-value pairs. Set `x-stream-filter` to a table of key-value pairs. Only messages whose headers contain all (or any, depending on match type) of the specified pairs are delivered.

### GIS Filtering

Geospatial filtering allows location-based message delivery. Published messages must include `x-geo-lat` and `x-geo-lon` headers with coordinates.

Three filter types are available:

- **geo-within-radius** — matches messages within a given distance from a point. Specify `lat`, `lon`, and `radius` (in meters).
- **geo-bbox** — matches messages within a bounding box. Specify `min_lat`, `max_lat`, `min_lon`, `max_lon`.
- **geo-polygon** — matches messages within a polygon. Specify an array of `[lat, lon]` coordinate pairs.

### Filter Configuration

| Argument | Description |
|----------|-------------|
| `x-stream-filter` | Filter criteria (string, table, or array) |
| `x-filter-match-type` | `all` (default) — all filters must match. `any` — at least one filter must match. |
| `x-stream-match-unfiltered` | If `true`, also deliver messages that have no filter headers |

## Retention

Streams support message retention policies to limit storage:

| Mechanism | Description |
|-----------|-------------|
| `x-max-age` | Delete segments older than this duration (e.g., `7D`, `1h`, `30m`). Set as queue argument or `max-age` policy. |
| `x-max-length` | Maximum number of messages. Oldest segments are dropped. |
| `x-max-length-bytes` | Maximum total bytes. Oldest segments are dropped. |

## Differences from Standard Queues

| Feature | Standard Queue | Stream |
|---------|---------------|--------|
| Message removal | On ack | Never (retention-based) |
| Consumer count | Shared consumption | Independent consumers |
| Acknowledgments | Required for manual ack | Not tracked per-consumer |
| Requeue | Supported | Not applicable |
| Exclusive | Supported | Not supported |
| Auto-delete | Supported | Not supported |
| Dead-lettering | Supported | Not supported |
| Priority | Supported | Not supported |
