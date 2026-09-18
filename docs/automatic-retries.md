# Automatic Retries

Automatic retries let the broker redeliver rejected messages after a growing backoff delay, replacing the manual pattern of chaining queues with TTLs and dead letter exchanges.

## How It Works

1. Declare a queue with the `x-delayed-retry-min` argument to enable the feature
2. When a consumer rejects a message with `requeue=true` (via `basic.reject` or `basic.nack`), the broker parks it in an internal retry queue (`amq.retry-<queue>`) instead of requeuing it immediately
3. When the backoff delay expires, the message is redelivered to the queue
4. When the delivery count exceeds `x-delivery-limit`, the message is dead-lettered (or dropped without a dead letter exchange)

## Queue Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `x-delayed-retry-min` | integer (≥ 1, ms) | - | Initial delay before the first retry. Setting it enables the feature. |
| `x-delayed-retry-multiplier` | integer (≥ 1) | unset (linear) | Backoff shape: omit for linear (`delay × attempt`), `1` for constant, `≥ 2` for exponential (`delay × multiplier^(attempt-1)`). |
| `x-delayed-retry-max` | integer (≥ 1, ms) | unset (no cap) | Cap on a single retry delay. Only clamps the delay, never ends retries. |
| `x-delivery-limit` | integer (≥ 0) | 20 when retry is enabled | Maximum number of redeliveries: a message is delivered at most `x-delivery-limit` + 1 times before dead-lettering. An explicit value, including 0, is respected. |

Delays are additionally capped at the `UInt32` millisecond range (~49.7 days) regardless of `x-delayed-retry-max`.

## Example

```text
# Up to 6 deliveries, backoff starting at 500 ms, doubling each time, capped at 30 s
x-delivery-limit:           5
x-delayed-retry-min:        500
x-delayed-retry-multiplier: 2
x-delayed-retry-max:        30000
x-dead-letter-exchange:     ""
x-dead-letter-routing-key:  failed-messages
```

Retry delays are 500 ms, 1 s, 2 s, 4 s, 8 s. When the 6th delivery is also rejected, the message is routed to the `failed-messages` queue with `x-death` reason `delivery_limit`.

## What Triggers a Retry

| Consumer action | Behavior |
|-----------------|----------|
| `basic.reject(requeue=true)` / `basic.nack(requeue=true)` | Parked in the retry queue, redelivered after the backoff. |
| `basic.reject(requeue=false)` / `basic.nack(requeue=false)` | Straight to the dead letter exchange, or dropped. |
| Channel/connection close, `basic.recover` | Instant requeue with no backoff, but the redelivery still counts towards `x-delivery-limit`. |

The `x-delivery-count` header on a delivery tells the consumer how many deliveries preceded it; it is absent on the first delivery. Client-supplied `x-delivery-count` is stripped when publishing to a retry-enabled queue, so neither publishers nor an upstream queue's dead-lettering can consume or reset the retry budget: a retry-enabled dead letter queue always starts with a fresh budget.

## Retry Queue

Each retry-enabled queue gets an internal companion queue named `amq.retry-<queue>`, holding parked messages ordered by redelivery time. It is created with the queue, deleted with it, and recreated automatically if it disappears. Like all internal queues it cannot be operated on by AMQP clients, but it is visible in the management UI and HTTP API, where the number of parked messages can be monitored.

## Notes and Limitations

- Retry delays are minimums, not exact schedules: under heavy load redelivery can lag behind the configured delay
- The message's original timestamp is preserved through retries, so `x-message-ttl` applies to the message's total age and can expire a message mid-retry
- Retry cannot be combined with `x-message-deduplication`: the declaration is refused, since a retried message would always be dropped as a duplicate
- Redeliveries caused by consumer disconnects consume the delivery budget, so pick `x-delivery-limit` with restart frequency in mind
- If the queue is full with `overflow=reject-publish` when a retry is due, the message stays parked and is retried again after the same delay
- The retry arguments can only be set at queue declaration, not via policies
- The queue name must leave room for the `amq.retry-` prefix within the 255 byte queue name limit
