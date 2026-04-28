# TTL (Time-To-Live)

LavinMQ supports TTL for both messages and queues.

## Message TTL

Message TTL controls how long a message can remain in a queue before it is discarded or dead-lettered.

### Setting Message TTL

Message TTL can be set in two places:

- **Per-message** — set the `expiration` property on the message (string value in milliseconds, e.g., `"60000"` for 60 seconds)
- **Per-queue** — set the `x-message-ttl` queue argument or apply the `message-ttl` policy

When both per-message and per-queue TTL are set, the lower of the two applies. The effective deadline is calculated as `(msg.timestamp + ttl) // 100 * 100` — that is, the publish time of the message plus the smaller TTL, truncated to the nearest 100 ms. Because the deadline is computed from `msg.timestamp`, changing the queue-level TTL retroactively affects messages already in the queue (they are re-evaluated against the new value at the next expiration check).

### Zero TTL

A TTL of `0` means the message should be delivered immediately or expired. If at least one consumer is attached when the message arrives the message is delivered, otherwise it is expired (and dead-lettered if a DLX is configured) on the first expiration check.

## Queue TTL (Expiration)

Queue TTL controls how long a queue can remain unused before it is automatically deleted.

### Setting Queue TTL

1. **Per-queue** — set the `x-expires` argument (integer in milliseconds, minimum 1)
2. **Via policy** — set the `expires` policy key

### Behavior

A per-queue background fiber (`queue_expire_loop`) sleeps until the queue's consumer count drops to zero, then starts a timer of `x-expires` milliseconds. The timer races against three signals:

- a consumer attaches (the timer is reset by waiting for "no consumers" again),
- `x-expires` is reconfigured (the new value is picked up on the next loop iteration), or
- a `basic.get` is performed (the loop is poked and re-evaluates).

If none of those happen before the timer fires, the queue is closed and deleted along with all its messages. Note that "unused" only considers consumers — publishing to the queue does not by itself reset the expiration timer.

## Interaction with Dead-Lettering

Expired messages (both from message TTL and queue overflow) can be dead-lettered if a dead letter exchange is configured. The dead-letter reason is `expired`. See [Dead Lettering](dead-lettering.md).
