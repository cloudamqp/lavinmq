# Dead Lettering

Dead lettering routes messages that cannot be processed to a designated exchange for inspection, retry, or archival.

## Configuration

Set on the source queue via arguments or policies:

| Argument / Policy | Description |
|-------------------|-------------|
| `x-dead-letter-exchange` / `dead-letter-exchange` | Exchange to route dead-lettered messages to |
| `x-dead-letter-routing-key` / `dead-letter-routing-key` | Override the routing key (uses original if not set) |
| `x-delivery-limit` / `delivery-limit` | Max redelivery attempts before dead-lettering |

## Dead-Letter Reasons

Messages are dead-lettered for the following reasons:

| Reason | Trigger |
|--------|---------|
| `rejected` | `basic.reject` or `basic.nack` with `requeue=false` |
| `expired` | Message TTL or queue-level `x-message-ttl` exceeded |
| `maxlen` | Queue `x-max-length` exceeded (with `drop-head` overflow) |
| `maxlenbytes` | Queue `x-max-length-bytes` exceeded |
| `delivery_limit` | `x-delivery-limit` exceeded |

## Delivery Limit

The `x-delivery-limit` argument (or `delivery-limit` policy) sets the maximum number of times a message can be redelivered. Each time a message is requeued (via reject/nack with `requeue=true`), an internal delivery counter is incremented. When the limit is reached, the message is dead-lettered instead of requeued.

The delivery count is tracked in the `x-delivery-count` message header.

## x-death Header

Dead-lettered messages receive an `x-death` header array. Each entry contains:

| Field | Description |
|-------|-------------|
| `queue` | Queue the message was dead-lettered from |
| `reason` | Dead-letter reason (rejected, expired, maxlen, etc.) |
| `exchange` | Exchange the message was originally published to |
| `count` | Number of times the message was dead-lettered for this reason from this queue |
| `time` | Timestamp of the most recent dead-lettering |
| `routing-keys` | Original routing keys (including CC) |
| `original-expiration` | Original `expiration` property (if set; cleared on dead-lettering) |

The most recent death entry is always at the front of the array. If a message is dead-lettered again for the same reason from the same queue, the existing entry's `count` is incremented.

## First-Death Headers

On the first dead-lettering event, these headers are set:

- `x-first-death-reason` — the reason for the first dead-lettering
- `x-first-death-queue` — the queue the message was first dead-lettered from
- `x-first-death-exchange` — the exchange the message was first published to

These headers are never overwritten on subsequent dead-letterings.

## Cycle Detection

LavinMQ detects dead-letter cycles to prevent infinite loops. When routing a dead-lettered message, the server checks the `x-death` history. If the message has already been dead-lettered from the destination queue for the same reason, delivery to that queue is skipped.

A `rejected` reason in the death history breaks cycle detection — if a message was explicitly rejected at any point, it is not considered a cycle.

## Interaction with Overflow

- `drop-head` overflow: dropped messages are dead-lettered with reason `maxlen` or `maxlenbytes`
- `reject-publish`: new messages are rejected (nacked to publisher), not dead-lettered
- `reject-publish-dlx`: new messages are rejected and dead-lettered

## Routing

Dead-lettered messages are routed directly to the matching queues on the dead-letter exchange, bypassing exchange-level features like delayed delivery and consistent hashing. The DLX and DLRK headers (`x-dead-letter-exchange`, `x-dead-letter-routing-key`) are stripped from the message before routing.

If a `x-dead-letter-routing-key` is set, CC and BCC headers are ignored for routing (but maintained on the message).
