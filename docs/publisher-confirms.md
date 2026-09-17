# Publisher Confirms

Publisher confirms provide a mechanism for the publisher to know that the server has received and handled a message.

## Enabling Confirms

Send `confirm.select` on a channel to enable publisher confirm mode. The server responds with `confirm.select-ok`.

Once enabled, confirms cannot be disabled on that channel.

## How It Works

After `confirm.select`, every message published on the channel is assigned a monotonically increasing sequence number (starting from 1). After the server processes the message, it sends `basic.ack` or `basic.nack` back to the publisher with the corresponding delivery tag.

- `basic.ack` with `multiple=false` — confirms a single message
- `basic.ack` with `multiple=true` — confirms all messages up to and including the delivery tag
- `basic.nack` — the server failed to process the message (e.g., internal error). The publisher should handle this and potentially retry.

## Disk-durable confirms

Since LavinMQ 2.9.0, a confirmed message is guaranteed to be on disk: the broker flushes pending writes with `syncfs(2)` before emitting `Basic.Ack`, so any confirmed message survives a sudden power loss or kernel crash.

- Concurrent in-flight confirms coalesce into a single `syncfs` call, keeping throughput high without an artificial flush delay.
- The blocking syscall runs on a dedicated isolated thread, so it never stalls client connection handling.
- If `syncfs` fails, the broker logs fatal and exits with status 1, so a follower in a clustered deployment can take over before any acked-but-unpersisted data is at risk.
- In a clustered deployment a publish is confirmed once every in-sync follower has the data; the local `syncfs` flush is used as a fallback when there are no in-sync followers or the node is standalone.

The flush can be turned off with the [`sync`](configuration.md) setting (`sync = false`, the `--no-sync` flag, or `LAVINMQ_SYNC=false`), which leaves durability to the OS. This is faster but unsafe — a confirmed message can be lost on crash or power loss — and is intended for CI and local development only.

## Mutual Exclusivity with Transactions

Publisher confirms and transactions (`tx.select`) are mutually exclusive on a channel. Enabling one after the other results in a channel error.

## basic.return

When a message is published with `mandatory: true` and cannot be routed to any queue, the server sends `basic.return` before the `basic.ack`:

| Reply Code | Meaning |
|-----------|---------|
| 312 | `NO_ROUTE` — no matching queue found |

The publisher receives `basic.return` first, then `basic.ack`. The ack confirms the server processed the message, even though it was returned.

If the mandatory flag is not set, unroutable messages are silently dropped (or sent to the alternate exchange if configured).
