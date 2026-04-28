# Transactions

AMQP transactions let a client batch publishes and acknowledgments on a single channel and apply (or discard) the whole batch with one server call. They are useful when a workflow needs to publish derived messages and ack the source message together, without exposing intermediate state to other consumers.

## Methods

| Method | Description |
|--------|-------------|
| `tx.select` | Enable transaction mode on the channel |
| `tx.commit` | Commit the current transaction |
| `tx.rollback` | Roll back the current transaction |

## How It Works

Once `tx.select` succeeds, the channel switches into transaction mode. From that point on:

- Each `basic.publish` is captured into an in-memory list of pending messages on the channel. The message body is streamed into a per-channel temporary file so the server doesn't keep all transactional payloads in RAM. No exchange routing happens yet.
- Each `basic.ack`, `basic.nack`, or `basic.reject` is captured into a separate list of pending acks (each entry remembers the delivery tag, the `multiple` flag, whether it is positive or negative, and the `requeue` flag).
- Other channel state (consumer registrations, declarations, prefetch) is unaffected and applies immediately as usual.

`tx.commit` drains both lists in order:

1. Pending acks/nacks/rejects are applied to their queues.
2. Pending publishes are routed through their exchanges (mandatory and immediate flags are honored, including `basic.return` for unroutable mandatory messages).
3. A filesystem sync (`syncfs` on Linux, `sync` elsewhere) is issued so the durable parts of the batch are committed to disk before the server replies with `tx.commit-ok`.

`tx.rollback` simply clears both lists and rewinds the body temp file. Neither the publishes nor the acks ever take effect, and no consumer or exchange observes any of the rolled-back work.

## Scope and Atomicity

Transaction state is per channel — each channel has its own pending-publishes and pending-acks lists, and `tx.commit` only operates on that channel.

Atomicity covers the client-visible surface: until `tx.commit` runs, no transactional publish reaches a queue and no transactional ack changes a queue's unacked state, so other consumers and publishers never see partial work. Inside `tx.commit` the operations are applied sequentially, so a queue may briefly observe acks before the matching publishes; the boundary is the commit reply, not the individual server-side writes.

## Mutual Exclusivity with Publisher Confirms

Transactions and publisher confirms cannot be enabled on the same channel. Calling `tx.select` on a confirm-mode channel (or `confirm.select` on a transaction-mode channel) returns a `precondition_failed` channel error. Use one channel for transactional work and a different channel for confirm-mode publishing if both are needed in the same connection.

## Performance Considerations

Transactions add round-trip latency (the publisher must wait for `tx.commit-ok` after every batch) and force a filesystem sync at commit. For straightforward "did the broker accept this?" guarantees, publisher confirms achieve similar reliability with much higher throughput. Transactions are still the right tool when you need the all-or-nothing semantics across publishes and acks together.
