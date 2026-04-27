# Deduplication

LavinMQ supports message deduplication at the exchange and queue levels, preventing duplicate messages from being routed or enqueued based on a header value.

## Enabling Deduplication

Set the `x-message-deduplication` argument to `true` when declaring an exchange or queue:

```
x-message-deduplication: true
```

## Configuration

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `x-message-deduplication` | Bool | `false` | Enable deduplication |
| `x-cache-size` | Int | `128` | Maximum entries in the dedup cache |
| `x-cache-ttl` | Int | (none) | Default TTL for cache entries (milliseconds) |
| `x-deduplication-header` | String | `x-deduplication-header` | Header key used as the dedup identifier |

## How It Works

1. When a message arrives at a dedup-enabled exchange or queue, the server reads the dedup header from the message
2. If the header value already exists in the cache, the message is considered a duplicate and is dropped (not routed)
3. If the value is not in the cache, the message is routed normally and the value is added to the cache

## Cache Behavior

- The cache is an in-memory hash map with a maximum size
- When the cache is full, the oldest entry is evicted (FIFO)
- Entries can have a TTL. Expired entries are lazily removed on the next lookup
- Per-message TTL can be set via the `x-cache-ttl` message header (Int32, milliseconds), overriding the queue/exchange default

## Limitations

- The dedup cache is **not persisted** across server restarts
- The dedup cache is **not replicated** across cluster nodes
- Deduplication is best-effort: after a restart or failover, previously seen messages may be accepted again
- Deduplication can be configured at the exchange level, the queue level, or both
