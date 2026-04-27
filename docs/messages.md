# Messages

A message consists of a routing key, properties (headers and metadata), and a body.

## Publishing

Messages are published via `basic.publish`, specifying:

- **Exchange** — the exchange to publish to (empty string for the default exchange)
- **Routing key** — used by the exchange for routing decisions
- **Mandatory flag** — if set and the message cannot be routed to any queue, it is returned to the publisher via `basic.return`
- **Properties** — message metadata
- **Body** — the message payload (up to `max_message_size`, default 128MB)

## Message Properties

| Property | Type | Description |
|----------|------|-------------|
| `content_type` | String | MIME type of the body (e.g., `application/json`) |
| `content_encoding` | String | Encoding of the body (e.g., `gzip`) |
| `headers` | Table | Arbitrary key-value headers |
| `delivery_mode` | UInt8 | 1 = transient, 2 = persistent |
| `priority` | UInt8 | Message priority (0-255, used by priority queues) |
| `correlation_id` | String | Correlation identifier for RPC |
| `reply_to` | String | Reply queue name (or `amq.rabbitmq.reply-to` for direct reply-to) |
| `expiration` | String | Per-message TTL in milliseconds (string, not integer) |
| `message_id` | String | Application message identifier |
| `timestamp` | Int64 | Message creation timestamp |
| `type` | String | Application message type |
| `user_id` | String | Publishing user (validated by the server) |
| `app_id` | String | Application identifier |

## Delivery Mode

- **Transient (1)** — the message may be lost on server restart. Still written to disk as part of normal queue storage, but not guaranteed to survive crashes.
- **Persistent (2)** — the message is durably stored. Combined with a durable queue, the message survives server restart.

## Mandatory Publishing and basic.return

When a message is published with `mandatory: true` and no queue matches, the server returns the message to the publisher via `basic.return` with a reply code:

- `312 NO_ROUTE` — no matching bindings found

If the mandatory flag is not set, unroutable messages are silently discarded (or routed to the alternate exchange if one is configured).

## CC and BCC Headers

Messages can be routed to additional queues via the `CC` and `BCC` headers:

- **CC** — an array of additional routing keys. The message is routed through the exchange with each CC routing key in addition to the primary routing key. The CC header is preserved in the delivered message.
- **BCC** — same as CC, but the BCC header is stripped from the delivered message.

Both headers must be arrays of strings.

## Direct Reply-To (RPC)

For request-response patterns, a consumer can subscribe to the pseudo-queue `amq.rabbitmq.reply-to`. When a publisher sets `reply_to: "amq.rabbitmq.reply-to"`, responses are routed directly to the consuming channel without declaring a temporary queue.

The reply consumer and the publisher must be on the same connection. See [AMQP](amqp.md) for more details.

## Server-Set Timestamp

If `set_timestamp` is enabled in the config, the server sets the `timestamp` property on received messages (overwriting any client-set value).

## LavinMQ-Added Headers

LavinMQ may add the following headers to delivered messages:

| Header | Description |
|--------|-------------|
| `x-death` | Dead-letter history. See [Dead Lettering](dead-lettering.md). |
| `x-first-death-reason` | Reason for the first dead-lettering event |
| `x-first-death-queue` | Queue the message was first dead-lettered from |
| `x-first-death-exchange` | Exchange the message was first published to |
| `x-delivery-count` | Number of times the message has been redelivered (when delivery limit tracking is active) |
| `x-stream-offset` | Current stream offset position (on messages delivered from streams) |

## Stream Filter Headers

When publishing messages intended for stream filtering:

| Header | Description |
|--------|-------------|
| `x-stream-filter-value` | String value used for simple stream filtering |
| `x-geo-lat` | Latitude for GIS stream filtering |
| `x-geo-lon` | Longitude for GIS stream filtering |
