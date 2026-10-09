# Automatic Retries

Automatic retries let the broker redeliver rejected messages after a growing backoff delay, replacing the manual pattern of chaining queues with TTLs and dead letter exchanges.

## How It Works

1. Declare a queue with the `x-delayed-retry-min` argument, or apply a policy with `delayed-retry-min`, to enable the feature
2. When a consumer rejects a message with `requeue=true` (via `basic.reject` or `basic.nack`), the broker delays it in an internal retry queue (`amq.retry-<queue>`) instead of requeuing it immediately
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

## Policies

Queue arguments cannot be changed after declaration, so a policy is the way to add retries to an existing queue. The policy keys mirror the arguments without the `x-` prefix:

| Policy key | Argument |
|------------|----------|
| `delayed-retry-min` | `x-delayed-retry-min` |
| `delayed-retry-multiplier` | `x-delayed-retry-multiplier` |
| `delayed-retry-max` | `x-delayed-retry-max` |
| `delivery-limit` | `x-delivery-limit` |

Values must be integers of at least 1. Like the other numeric policy keys, each key applies when the queue has no matching argument, or when the policy value is lower than the argument. `delayed-retry-multiplier` and `delayed-retry-max` only take effect when retries are enabled by `delayed-retry-min` or `x-delayed-retry-min`. When retries are enabled and neither an argument nor a policy sets a delivery limit, the limit defaults to 20.

A policy cannot refuse a queue the way a declaration can, so on a queue with `x-message-deduplication`, or with a name that leaves no room for the `amq.retry-` prefix, the retry keys are ignored and a warning is logged. The other keys of the policy still apply.

Changing the retry values of a policy affects subsequent retries only: messages already waiting in the retry queue keep the delay they were given.

When retries are disabled by removing or changing the policy, messages already waiting in the retry queue are not lost or released early: they are published back to the queue when their delay expires, and the retry queue is deleted once it is empty. Messages rejected with `requeue=true` after the policy is removed are requeued instantly. The delivery limit returns to the value set by argument or the remaining policy, or no limit when neither sets one.

## What Triggers a Retry

| Consumer action | Behavior |
|-----------------|----------|
| `basic.reject(requeue=true)` / `basic.nack(requeue=true)` | Delayed in the retry queue, redelivered after the backoff. |
| `basic.reject(requeue=false)` / `basic.nack(requeue=false)` | Straight to the dead letter exchange, or dropped. |
| Channel/connection close, `basic.recover` | Instant requeue with no backoff, but the redelivery still counts towards `x-delivery-limit`. |

The `x-delivery-count` header on a delivery tells the consumer how many deliveries preceded it; it is absent on the first delivery. `x-delivery-limit` is always compared against this one counter, which increments on every redelivery regardless of cause, so delayed retries and instant broker-initiated requeues consume the same budget. The counter is maintained by the broker alone: an `x-delivery-count` header supplied on a publish to a retry-enabled queue is removed. A message dead-lettered into a retry-enabled queue therefore always starts with a fresh retry budget.

## Retry Queue

Each retry-enabled queue gets an internal companion queue named `amq.retry-<queue>`, holding delayed messages ordered by redelivery time. It is created with the queue, deleted with it, and recreated automatically if it disappears. Like all internal queues it cannot be operated on by AMQP clients, but it is visible in the management UI and HTTP API, where the number of delayed messages can be monitored.

## Notes and Limitations

- Retry delays are minimums, not exact schedules: under heavy load redelivery can lag behind the configured delay
- The message's original timestamp is preserved through retries, so `x-message-ttl` applies to the message's total age and can expire a message mid-retry
- Retry cannot be combined with `x-message-deduplication`: the declaration is refused, since a retried message would always be dropped as a duplicate
- Redeliveries caused by consumer disconnects consume the delivery budget, so pick `x-delivery-limit` with restart frequency in mind
- If the queue is full with `overflow=reject-publish` when a retry is due, the message stays in the retry queue and is delayed again for one more backoff period
- The retry arguments and policy keys are refused on streams
- If the retry queue cannot store a message, for example on a disk error, the message is requeued instantly without backoff and the retry queue is recreated on the next reject
- The queue name must leave room for the `amq.retry-` prefix within the 255 byte queue name limit
