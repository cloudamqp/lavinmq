# Streams

Streams are append-only log queues designed for multiple independent consumers. Unlike standard queues, messages are not removed when consumed — each consumer maintains its own read position.

## Declaration

Declare a stream by setting `x-queue-type: stream`:

```
x-queue-type: stream
```

Streams are always durable. They cannot be exclusive or auto-delete.

## Consumer Offsets

Each consumer can specify where to start reading from using the `x-stream-offset` argument on `basic.consume`:

| Value | Description |
|-------|-------------|
| `first` | Start from the beginning of the stream |
| `last` | Start from the last available chunk |
| `next` | Start from new messages only |
| (timestamp) | Start from the first message after the given timestamp |
| (integer) | Start from a specific offset number |
| (negative integer) | Start `N` messages before the head (e.g. `-100` reads the last 100 messages). Clamped to the oldest available message when fewer than `N` are stored. `0` retains its meaning of "start from the beginning". |

Delivered messages include an `x-stream-offset` header with the current offset position.

When no `x-stream-offset` is specified, the consumer resumes from its last tracked offset (or starts from the beginning if no offset has been stored). If the consumer tag is not auto-generated (does not start with `amq.ctag-`), future acks also persist new offset positions.

### Automatic Offset Tracking

Automatic offset tracking persists the consumer's offset position on the server. It is enabled in two ways:

- Implicitly, when no `x-stream-offset` argument is provided and the consumer tag is not auto-generated (does not start with `amq.ctag-`)
- Explicitly, by setting `x-stream-automatic-offset-tracking: true` on `basic.consume` when an `x-stream-offset` is also specified

On reconnect, the consumer resumes from where it left off without needing to specify the offset manually.

## Stream Filtering

Consumers can filter messages based on headers, avoiding unnecessary delivery of unwanted messages.

### Simple String Filter

Publish messages with the `x-stream-filter-value` header set to a string value, or a comma-separated list of values to tag a single message with multiple filter values. Consumers set `x-stream-filter` to match against these values. Only messages whose filter values include the consumer's filter are delivered.

### Key-Value Filtering

Filter on arbitrary message header key-value pairs. Set `x-stream-filter` to a table of key-value pairs. Only messages whose headers contain all (or any, depending on match type) of the specified pairs are delivered.

### GIS Filtering

Geospatial filtering allows location-based message delivery. Published messages must include `x-geo-lat` and `x-geo-lon` headers with coordinates.

Three filter types are available:

- **geo-within-radius** — matches messages within a given distance from a point. Specify `lat`, `lon`, and `radius_km` (in kilometers).
- **geo-bbox** — matches messages within a bounding box. Specify `min_lat`, `max_lat`, `min_lon`, `max_lon`.
- **geo-polygon** — matches messages within a polygon. Specify a table with a `points` key containing an array of `[lat, lon]` coordinate pairs.

### Filter Configuration

| Argument | Description |
|----------|-------------|
| `x-stream-filter` | Filter criteria (string, table, or array) |
| `x-filter-match-type` | `all` (default) — all filters must match. `any` — at least one filter must match. |
| `x-stream-match-unfiltered` | If `true`, also deliver messages that have no `x-stream-filter-value` header. This check looks only at `x-stream-filter-value`, regardless of which filter types are configured. |

## Retention

Streams support message retention policies to limit storage:

| Mechanism | Description |
|-----------|-------------|
| `x-max-age` | Delete segments older than this duration. Format is `<number><unit>` where unit is one of `Y` (years), `M` (months), `D` (days), `h` (hours), `m` (minutes), `s` (seconds). Units are case-sensitive (e.g., `7D`, not `7d`). Set as queue argument or `max-age` policy. |
| `x-max-length` | Maximum number of messages. Oldest segments are dropped. |
| `x-max-length-bytes` | Maximum total bytes. Oldest segments are dropped. |

## Differences from Standard Queues

| Feature | Standard Queue | Stream |
|---------|---------------|--------|
| Message removal | On ack | Never (retention-based) |
| Consumer count | Shared consumption | Independent consumers |
| Acknowledgments | Required for manual ack | Required; persists the per-consumer offset when offset tracking is enabled |
| Requeue | Supported | Supported (in-memory) |
| Exclusive | Supported | Not supported |
| Auto-delete | Supported | Not supported |
| Dead-lettering | Supported | Not supported |
| Priority | Supported | Not supported |

## Restrictions

The following arguments are not supported on streams:

- `x-dead-letter-exchange`
- `x-dead-letter-routing-key`
- `x-expires`
- `x-delivery-limit`
- `x-overflow`
- `x-single-active-consumer`
- `x-max-priority`
