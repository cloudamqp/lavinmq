# Publisher Confirms

Publisher confirms provide a mechanism for the publisher to know that the server has received and handled a message.

## Enabling Confirms

Send `confirm.select` on a channel to enable publisher confirm mode. The server responds with `confirm.select-ok`.

Once enabled, confirms cannot be disabled on that channel.

## How It Works

After `confirm.select`, every message published on the channel is assigned a monotonically increasing sequence number (starting from 1). After the server processes the message, it sends `basic.ack` back to the publisher with the corresponding delivery tag.

- `basic.ack` with `multiple=false` — confirms a single message
- `basic.ack` with `multiple=true` — confirms all messages up to and including the delivery tag

## Mutual Exclusivity with Transactions

Publisher confirms and transactions (`tx.select`) are mutually exclusive on a channel. Enabling one after the other results in a channel error.

## basic.return

When a message is published with `mandatory: true` and cannot be routed to any queue, the server sends `basic.return` before the `basic.ack`:

| Reply Code | Meaning |
|-----------|---------|
| 312 | `NO_ROUTE` — no matching queue found |

The publisher receives `basic.return` first, then `basic.ack`. The ack confirms the server processed the message, even though it was returned.

If the mandatory flag is not set, unroutable messages are silently dropped (or sent to the alternate exchange if configured).
