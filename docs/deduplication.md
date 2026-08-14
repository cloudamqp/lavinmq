# Deduplication

LavinMQ supports message deduplication at the exchange and queue levels, preventing duplicate messages from being routed or enqueued based on a header value.

## Where to Deduplicate

Deduplication can be configured at the exchange level, the queue level, or both, with different effects:

- **Exchange-level** — duplicate messages are not forwarded to any bound queue. Backed by a bounded, TTL-based cache (see [Exchange-level cache](#exchange-level-cache)).
- **Queue-level** — the message is routed as usual but not stored in the queue if a message with the same dedup identifier is **already in the queue**. This avoids inflating the "unroutable message" statistic that exchange-level dedup would produce when a duplicate fails to reach any queue.

The two levels use different models. Exchange-level dedup remembers identifiers in a separate cache for a configurable time/size. Queue-level dedup has no separate cache: an identifier is considered "seen" exactly while a message bearing it is still in the queue, and is forgotten as soon as that message leaves.

## Enabling Deduplication

Set the `x-message-deduplication` argument to `true` when declaring an exchange or queue:

```
x-message-deduplication: true
```

## Configuration

| Argument | Type | Applies to | Default | Description |
|----------|------|------------|---------|-------------|
| `x-message-deduplication` | Bool | exchange, queue | `false` | Enable deduplication |
| `x-deduplication-header` | String | exchange, queue | `x-deduplication-header` | Message header that holds the dedup identifier |
| `x-cache-size` | Int | exchange only | `128` | Maximum entries in the exchange dedup cache |
| `x-cache-ttl` | Int | exchange only | (none) | Default TTL for exchange cache entries (milliseconds) |

> **Note:** `x-cache-size` and `x-cache-ttl` apply only to **exchange-level** deduplication. They are ignored on queues — declaring a queue with them still succeeds, but the values have no effect. For time-bounded queue deduplication, give the messages a lifetime with [`x-message-ttl`](ttl.md) (or a per-message `expiration`): when a message expires out of the queue, its dedup identifier is released automatically.

## How It Works

When a message arrives at a dedup-enabled exchange or queue, the server reads the dedup identifier from the configured header. Messages without that header are never deduplicated (they are always considered unique).

### Exchange-level

1. If the identifier exists in the exchange's cache, the message is dropped (not routed).
2. Otherwise the message is routed and the identifier is added to the cache.

### Queue-level

1. If a message with the same identifier is currently in the queue (waiting to be delivered, or delivered but not yet acknowledged), the incoming message is dropped.
2. Otherwise the message is enqueued and its identifier is tracked.
3. When the message leaves the queue — acknowledged, expired, dead-lettered, dropped by overflow, or purged — its identifier is released, so a future message with the same identifier is accepted again.

This means queue-level deduplication is bounded by the contents of the queue, not by a separate cache size or TTL. There is no FIFO eviction and no cache that can hold an identifier after its message is gone.

## Exchange-level cache

- The cache is an in-memory hash map with a maximum size (`x-cache-size`).
- When the cache is full, the oldest entry is evicted (FIFO).
- Entries can have a TTL. Expired entries are lazily removed on the next lookup.
- Per-message TTL can be set via the `x-cache-ttl` message header (Int32, milliseconds), overriding the exchange default.

## Limitations

- Deduplication state is in memory only and is not replicated across cluster nodes. It is lost on leadership transfer in a cluster.
- Deduplication is best-effort: after a restart or failover, previously seen messages may be accepted again.

### Queue-level restart behavior

The queue's dedup index is rebuilt from the persisted messages when the queue starts (on broker boot and on queue restart). The rebuild runs in the background so the broker starts quickly, which has two consequences:

- **Brief under-enforcement during rebuild.** While a queue's index is being rebuilt, duplicates of messages that the scan has not reached yet may slip through. The window is per-queue and lasts only until that queue's scan completes.
- **Transient over-suppression.** If a message is consumed while the rebuild is still scanning, its identifier can be re-added to the index even though the message is gone, briefly blocking that identifier. This clears on the next restart.

### Enabling on a non-empty queue

Turning on `x-message-deduplication` for a queue that already holds messages (for example, via a policy change at runtime) does **not** retroactively index the existing messages. Deduplication takes effect for newly published messages; the pre-existing messages are not protected until they leave the queue. To index existing messages, restart the queue.
