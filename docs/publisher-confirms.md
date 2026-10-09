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

## Durability and Synchronization

With synchronization enabled (the default), the broker syncs the data written to durable queues before confirming the publish. A confirm does not make a transient queue survive a restart or mean that a consumer has received the message.

Publish confirms sync the message segments the confirmed publishes were written to and the directories of newly created files. They do not normally flush unrelated writes from other queues. Pending confirms are batched; if the batch's files and directories exceed `syncfs_threshold` in `[main]` (default `64`), the broker instead syncs the whole filesystem containing the data directory. Transaction commits also use a whole-filesystem sync to persist their acknowledgments as well as publishes. See [Configuration](configuration.md) and [Transactions](transactions.md).

In a cluster, the broker waits for every in-sync follower to acknowledge the replicated writes after the requested synchronization. If a follower is removed from the ISR while waiting, that membership change must be committed before the broker confirms. See [Clustering](clustering.md#replication-durability).

Setting `sync=false` in `[main]` (or using `--no-sync`) skips local disk synchronization. Confirms still wait for replication in a cluster, but do not guarantee that data is durable on the leader. Each follower's own `sync` setting determines whether it synchronizes before acknowledging.

## Mutual Exclusivity with Transactions

Publisher confirms and transactions (`tx.select`) are mutually exclusive on a channel. Enabling one after the other results in a channel error.

## basic.return

When a message is published with `mandatory: true` and cannot be routed to any queue, the server sends `basic.return` before the `basic.ack`:

| Reply Code | Meaning |
|-----------|---------|
| 312 | `NO_ROUTE` — no matching queue found |

The publisher receives `basic.return` first, then `basic.ack`. The ack confirms the server processed the message, even though it was returned.

If the mandatory flag is not set, unroutable messages are silently dropped (or sent to the alternate exchange if configured).
