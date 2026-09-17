# AMQP 1.0

LavinMQ accepts AMQP 1.0 connections on the same listener as AMQP 0-9-1. The protocol is detected from the 8-byte protocol header, so AMQP 1.0 clients connect to the regular `amqp_port` (or `amqps_port` for TLS) and need no extra configuration. Messages published over one protocol can be consumed over the other. For the full protocol specification, see the [AMQP 1.0 spec](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html).

## Ports

| Config Key | Default Port |
|------------|-------------|
| `amqp_port` | 5672 |
| `amqps_port` | 5671 |

## Connection Setup

- **SASL is required.** A client that sends the bare AMQP 1.0 protocol header is answered with the SASL protocol header and disconnected; clients must start with the SASL header (`AMQP\x03\x01\x00\x00`).
- **Mechanisms:** `PLAIN` only. Credentials are validated against the configured [authentication chain](authentication.md).
- **Virtual host selection:** set the `hostname` field of the `open` frame to `vhost:<name>`. Any other value, or no hostname, selects the default vhost `/`. The user needs permissions on the vhost.
- If the vhost does not exist, the user lacks access, or the vhost's `max-connections` limit is reached, the server replies with `open` followed by `close` carrying `amqp:not-found`, `amqp:unauthorized-access` or `amqp:not-allowed`.

## Negotiated Parameters

| Parameter | Value |
|-----------|-------|
| `max-frame-size` | The lower of the client's value and `frame_max` (default 131,072 bytes). Deliveries larger than that are split over several `transfer` frames. |
| `channel-max` | `channel_max` (default 2,048). A `begin` on a channel above it closes the connection. `0` in the config means unlimited. |
| `idle-time-out` | `heartbeat` in seconds, converted to milliseconds (default 300 s). `0` disables it. |

Both peers' idle timeouts are honoured: LavinMQ sends an empty frame once half of the client's `idle-time-out` has passed without it sending anything, and closes the connection when nothing has been received for one and a half times its own.

## Addressing

Addresses follow the [RabbitMQ AMQP 1.0 address format](https://www.rabbitmq.com/docs/amqp#addresses). Queues and exchanges must already exist; they are not created on attach. Address components are percent-decoded.

| Address | Target (publish) | Source (consume) |
|---------|------------------|------------------|
| `/queues/{queue}` | Publish directly to the queue | Consume from the queue |
| `/exchanges/{exchange}` | Publish to the exchange with an empty routing key | — |
| `/exchanges/{exchange}/{routing-key}` | Publish to the exchange with the routing key | — |
| *(no address)* | Anonymous terminus: each message carries its target in the `to` property | — |
| *(dynamic)* | An exclusive, auto-delete queue is created and its `/queues/...` address returned in the `attach` | Same |

Dynamic queues live as long as the connection that created them. Publishing to internal exchanges, `$management` addresses, durable termini, `dynamic-node-properties` and source filters are refused with a `detach` carrying `amqp:precondition-failed`.

Publishing requires write permission on the exchange (an empty exchange name for queue targets), consuming requires read permission on the queue. A `user-id` property must match the authenticated user unless the user may impersonate others.

## Publishing

Incoming transfers are settled by LavinMQ as soon as they are processed (`rcv-settle-mode` first). Pre-settled transfers (`snd-settle-mode` settled) get no disposition.

| Outcome | When |
|---------|------|
| `accepted` | Routed to at least one queue |
| `released` | Unroutable, or publishing is currently blocked by [low disk space](connections.md#low-disk-space) |
| `rejected` | Refused by a queue with `reject-publish` overflow, invalid address, `user-id` mismatch, message larger than `max_message_size`, or a message that could not be decoded |

Each link is granted 65,535 credits at attach; the credit is topped up as deliveries complete. The `message-format` must be `0`. Multi-frame deliveries are reassembled before publishing; an aborted delivery is discarded.

## Consuming

A receiving link acts as a consumer on the queue. Messages are delivered as link credit is granted with `flow`; `drain` and `echo` are supported. Deliveries are unsettled unless the link was attached with `snd-settle-mode` settled, in which case they are acknowledged on delivery (like `no-ack` in 0-9-1). `rcv-settle-mode` second is honoured: an unsettled disposition from the client is answered with a settled one.

| Disposition | Effect |
|-------------|--------|
| `accepted` | Acknowledged and removed from the queue |
| `released`, `modified` | Requeued |
| `rejected` | Dropped, or dead-lettered if the queue has a dead-letter exchange |

AMQP 1.0 consumers take part in [single active consumer](consumers.md), respect [paused queues](queues.md) and yield to 0-9-1 consumers with a higher priority (AMQP 1.0 consumers have priority 0). When the oldest unacknowledged delivery exceeds the [consumer timeout](consumers.md), the link is detached with `amqp:precondition-failed`; deleting the queue detaches it with `amqp:resource-deleted`. Sessions appear as channels and links as consumers in the management UI and API.

## Message Mapping

Messages are stored in the AMQP 0-9-1 format. Sections and properties map as follows in both directions.

| AMQP 1.0 | AMQP 0-9-1 |
|----------|------------|
| `header.durable` | `delivery-mode` (2 when durable) |
| `header.priority` | `priority` |
| `header.ttl` | `expiration` (milliseconds) |
| `properties.message-id` | `message-id` (numeric and UUID ids as strings) |
| `properties.user-id` | `user-id` |
| `properties.to` | Publish target for anonymous links; not stored |
| `properties.subject` | `type` |
| `properties.reply-to` | `reply-to` |
| `properties.correlation-id` | `correlation-id` |
| `properties.content-type` | `content-type` |
| `properties.content-encoding` | `content-encoding` |
| `properties.absolute-expiry-time` | `expiration`, as the remaining time when published |
| `properties.creation-time` | `timestamp` (milliseconds to seconds) |
| `application-properties` | `headers` |
| `data` | Body. Several `data` sections are concatenated. |
| `amqp-value` | Body. String and binary values become the body as-is; other values are stored in their AMQP 1.0 encoding. |
| `delivery-annotations`, `message-annotations`, `footer` | Ignored |

String properties are limited to 255 bytes, as in 0-9-1. Messages delivered to AMQP 1.0 consumers always carry the body in a single `data` section.

## Not Supported

- Link recovery (the `unsettled` map on attach), durable termini and source filters
- Transactions
- Management (`$management`) links
- Direct reply-to via `amq.direct.reply-to` addresses
- SASL mechanisms other than `PLAIN`

## Further Reading

- [AMQP 0-9-1](amqp.md) — the protocol messages are stored and interoperate in
- [Connections](connections.md) — heartbeats, proxy protocol, connection limits
- [Consumers](consumers.md) — consumer timeout, single active consumer, priorities
- [Queues](queues.md) — queue types and arguments
