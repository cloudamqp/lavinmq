# AMQP 1.0

LavinMQ accepts AMQP 1.0 connections on the same listener as AMQP 0-9-1. The protocol is detected from the 8-byte protocol header, so AMQP 1.0 clients connect to the regular `amqp_port` (or `amqps_port` for TLS) and need no extra configuration. Messages published over one protocol can be consumed over the other. For the full protocol specification, see the [AMQP 1.0 spec](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html).

## Ports

| Config Key | Default Port |
|------------|-------------|
| `amqp_port` | 5672 |
| `amqps_port` | 5671 |

## Connection Setup

- **SASL is required** except on loopback connections (not proxied ones), where a client that sends the bare AMQP 1.0 protocol header is logged in as with `ANONYMOUS`. Elsewhere it is answered with the SASL protocol header and disconnected; clients must start with the SASL header (`AMQP\x03\x01\x00\x00`).
- **Mechanisms:** `PLAIN`, with credentials validated against the configured [authentication chain](authentication.md), and `ANONYMOUS` on loopback connections only (not proxied ones). `ANONYMOUS` logs in as the default user with the default password (`guest`), so it grants what a local client logging in with those credentials would get, and stops working once the default user's password is changed.
- **Virtual host selection:** set the `hostname` field of the `open` frame to `vhost:<name>`. Any other value, or no hostname, selects the default vhost `/`. The user needs permissions on the vhost.
- If the vhost does not exist, the user lacks access, or the vhost's `max-connections` limit is reached, the server replies with `open` followed by `close` carrying `amqp:not-found`, `amqp:unauthorized-access` or `amqp:not-allowed`.

## Negotiated Parameters

| Parameter | Value |
|-----------|-------|
| `max-frame-size` | `frame_max` (default 131,072 bytes), the largest frame LavinMQ accepts. Frames it sends fit the lower of that and the client's value; deliveries larger than that are split over several `transfer` frames. |
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

Dynamic queues live as long as the connection that created them. A refused attach is answered with an `attach` without a terminus followed by a `detach` carrying the reason:

| Condition | When |
|-----------|------|
| `amqp:not-found` | The queue or exchange does not exist, or the address is not one of the formats above |
| `amqp:unauthorized-access` | The user lacks the permission, or the exchange is internal |
| `amqp:resource-locked` | The queue is exclusive to another connection |
| `amqp:resource-limit-exceeded` | A dynamic queue cannot be created: the vhost queue limit is reached or disk space is low |
| `amqp:not-implemented` | `$management` addresses, durable termini, `dynamic-node-properties` or source filters |
| `amqp:invalid-field` | A dynamic terminus with an address, or a receiving link without a source address |

Publishing requires write permission on the exchange (an empty exchange name for queue targets), consuming requires read permission on the queue. A `user-id` property must match the authenticated user unless the user may impersonate others.

## Publishing

Incoming transfers are settled by LavinMQ as soon as they are processed (`rcv-settle-mode` first), or, on links attached with `rcv-settle-mode` second, answered with an unsettled disposition, leaving the sender to settle them. Pre-settled transfers (`snd-settle-mode` settled) get no disposition.

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
| `released` | Requeued |
| `modified` | Requeued; its `message-annotations` are merged into the message's annotations for later deliveries. The merge is kept in memory, like delivery counts, and lost on restart. `undeliverable-here` is not honoured. |
| `rejected` | Dropped, or dead-lettered if the queue has a dead-letter exchange |

A redelivered message carries a `header` section whose `delivery-count` is the number of earlier deliveries on queues with a `delivery-limit`, and 1 on other queues, which do not count them.

AMQP 1.0 consumers take part in [single active consumer](consumers.md), respect [paused queues](queues.md) and yield to 0-9-1 consumers with a higher priority (AMQP 1.0 consumers have priority 0). When the oldest unacknowledged delivery exceeds the [consumer timeout](consumers.md), the link is detached with `amqp:precondition-failed`; deleting the queue detaches it with `amqp:resource-deleted`. Sessions appear as channels and links as consumers in the management UI and API.

## Message Mapping

Messages are stored in the AMQP 0-9-1 format. Sections and properties map as follows in both directions.

| AMQP 1.0 | AMQP 0-9-1 |
|----------|------------|
| `header.durable` | `delivery-mode` (2 when durable) |
| `header.priority` | `priority` |
| `header.ttl` | `expiration` (milliseconds) |
| `properties.message-id` | `message-id`, as a string; `ulong`, `uuid` and `binary` ids are delivered to AMQP 1.0 consumers with their original type |
| `properties.user-id` | `user-id` |
| `properties.to` | Publish target for anonymous links; not stored |
| `properties.subject` | `type` |
| `properties.reply-to` | `reply-to` |
| `properties.correlation-id` | `correlation-id`, typed like `message-id` |
| `properties.content-type` | `content-type` |
| `properties.content-encoding` | `content-encoding` |
| `properties.absolute-expiry-time` | `expiration`, as the remaining time when published |
| `properties.creation-time` | `timestamp` (milliseconds to seconds) |
| `application-properties` | `headers` |
| `data` | Body. Several `data` sections are concatenated. |
| `amqp-value` | Body. String and binary values become the body as-is; other values are stored in their AMQP 1.0 encoding. |
| `message-annotations` | Kept in the `x-amqp10-message-annotations` header and delivered unchanged to AMQP 1.0 consumers |
| `delivery-annotations`, `footer` | Ignored |

String properties are limited to 255 bytes, as in 0-9-1. AMQP 1.0 consumers receive the body in the section it was published in: an `amqp-value` body comes back as the same `amqp-value`, `data` sections, or a body published over 0-9-1, come back as a single `data` section, and a message published without a body section, as Proton does for a null body, comes back without one.

Details the 0-9-1 format has no field for are kept in headers prefixed `x-amqp10-`, such as `x-amqp10-body-type` for an `amqp-value` body and `x-amqp10-message-id-type` for a non-string message-id. 0-9-1 consumers see these headers; AMQP 1.0 consumers do not receive them as application-properties, and AMQP 1.0 publishers cannot set them.

## Not Supported

- Link recovery (the `unsettled` map on attach), durable termini and source filters
- Transactions
- Management (`$management`) links
- Direct reply-to via `amq.direct.reply-to` addresses
- SASL mechanisms other than `PLAIN` and `ANONYMOUS`

## Further Reading

- [AMQP 0-9-1](amqp.md) — the protocol messages are stored and interoperate in
- [Connections](connections.md) — heartbeats, proxy protocol, connection limits
- [Consumers](consumers.md) — consumer timeout, single active consumer, priorities
- [Queues](queues.md) — queue types and arguments
