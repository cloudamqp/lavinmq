# TTL (Time-To-Live)

LavinMQ supports TTL for both messages and queues.

## Message TTL

Message TTL controls how long a message can remain in a queue before it is discarded or dead-lettered.

### Setting Message TTL

Message TTL can be set in two places:

- **Per-message** — set the `expiration` property on the message (string value in milliseconds, e.g., `"60000"` for 60 seconds)
- **Per-queue** — set the `x-message-ttl` queue argument or apply the `message-ttl` policy

When both per-message and per-queue TTL are set, the lower of the two applies.

### Enforcement

LavinMQ enforces message TTL in two ways:

- **Active expiration** — a background fiber checks messages at the head of the queue and removes expired ones
- **Lazy expiration** — messages are checked on delivery and discarded (or dead-lettered if a DLX is configured) if expired

Messages are expired from the head of the queue. A message with a longer TTL behind a message with a shorter TTL will not be expired until the shorter-TTL message is processed first.

### Precision

TTL is truncated to 100ms precision internally: `(timestamp + ttl) // 100 * 100`.

### Zero TTL

A TTL of 0 means the message should be delivered immediately or discarded/dead-lettered. If no consumer is available for immediate delivery, the message expires right away.

## Queue TTL (Expiration)

Queue TTL controls how long a queue can remain unused before it is automatically deleted.

### Setting Queue TTL

1. **Per-queue** — set the `x-expires` argument (integer in milliseconds, minimum 1)
2. **Via policy** — set the `expires` policy key

### Behavior

- A queue is considered unused when it has no consumers
- The expiration timer starts when the last consumer unsubscribes
- The timer is reset when a consumer subscribes or when `basic.get` is called on the queue
- When the queue expires, it is deleted along with all its messages

## Interaction with Dead-Lettering

Expired messages (both from message TTL and queue overflow) can be dead-lettered if a dead letter exchange is configured. The dead-letter reason is `expired`. See [Dead Lettering](dead-lettering.md).
