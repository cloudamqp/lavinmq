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
| `/exchanges/{exchange}` | Publish to the exchange with an empty routing key | Subscribe to the exchange with an empty binding key, see [Subscriptions](#subscriptions) |
| `/exchanges/{exchange}/{routing-key}` | Publish to the exchange with the routing key | Subscribe to the exchange with the routing key as binding key |
| *(no address)* | Anonymous terminus: each message carries its target in the `to` property | — |
| *(dynamic)* | An exclusive, auto-delete queue is created and its `/queues/...` address returned in the `attach` | Same |

A dynamic queue is deleted when the link that created it detaches or its connection closes, which is the `delete-on-close` lifetime policy; that policy, as Qpid JMS asks for when creating a temporary queue, is accepted in `dynamic-node-properties`. Any connection may publish to a dynamic queue, but only the one that created it may consume from it.

A refused attach is answered with an `attach` without a terminus followed by a `detach` carrying the reason:

| Condition | When |
|-----------|------|
| `amqp:not-found` | The queue or exchange does not exist, the address is not one of the formats above, or a receiving link without a source names no durable subscription |
| `amqp:unauthorized-access` | The user lacks the permission, or the exchange is internal |
| `amqp:resource-locked` | The queue is exclusive to another connection, or a subscription is in use (see [Subscriptions](#subscriptions)) |
| `amqp:resource-limit-exceeded` | A dynamic or subscription queue cannot be created: the vhost queue limit is reached or disk space is low |
| `amqp:not-implemented` | `$management` addresses, durable termini other than subscriptions, `dynamic-node-properties` other than `lifetime-policy: delete-on-close`, or source filters |
| `amqp:invalid-field` | A dynamic terminus with an address, a receiving link with a source without an address, or a subscription name that is too long or not printable ASCII |

Publishing requires write permission on the exchange (an empty exchange name for queue targets), consuming requires read permission on the queue. A `user-id` property must match the authenticated user unless the user may impersonate others.

## Publishing

Incoming transfers are settled by LavinMQ once they are processed (`rcv-settle-mode` first). `accepted` is only sent once the message is written to disk and, in a cluster, replicated to the in-sync followers, as publisher confirms are in AMQP 0-9-1; `rejected` and `released` are sent at once. On links attached with `rcv-settle-mode` second, the disposition is sent unsettled, leaving the sender to settle the delivery. Pre-settled transfers (`snd-settle-mode` settled) get no disposition.

| Outcome | When |
|---------|------|
| `accepted` | Routed to at least one queue |
| `released` | Unroutable, or publishing is currently blocked by [low disk space](connections.md#low-disk-space) |
| `rejected` | Refused by a queue with `reject-publish` overflow, invalid address, `user-id` mismatch, message larger than `max_message_size`, or a message that could not be decoded |

Each link is granted 65,535 credits at attach; the credit is topped up as deliveries complete. The `message-format` must be `0`. Multi-frame deliveries are reassembled before publishing; an aborted delivery is discarded.

LavinMQ offers the `ANONYMOUS-RELAY` capability in its `open`, so clients such as Qpid JMS publish messages for any destination over one link without a target address, each routed by its `to` property.

## Request-Reply

The requester sets `reply-to` to the address of a dynamic queue it consumes from (a JMS temporary queue), or of a pre-declared queue, and the responder publishes the reply to that address, over a link to it or an anonymous link with `to` set. Direct reply-to (`amq.direct.reply-to`) is not supported.

## Subscriptions

A receiving link whose source is an `/exchanges/` address subscribes to the exchange: LavinMQ binds a queue to the exchange, with the address's routing key as binding key, and the link consumes from it. This is how JMS topic subscribers work with Qpid JMS, given `jms.topicPrefix=/exchanges/amq.topic/` in the connection URI. Creating the queue needs configure permission on it, binding needs read permission on the exchange and write permission on the queue, and consuming needs read permission on the queue.

The source's `expiry-policy` and `capabilities` select the kind of subscription, following the conventions of Qpid JMS:

| Subscription | Source | Queue |
|---|---|---|
| Non-durable | any other `expiry-policy` | Server-named, exclusive to the connection, deleted with the link |
| Durable | `expiry-policy` `never` | `amqp10.sub.{container-id}.{link name}`, durable |
| Shared durable | `expiry-policy` `never`, capability `shared` | `amqp10.sub.{container-id}.{name}`, durable |
| Global shared durable | `expiry-policy` `never`, capabilities `shared` and `global` | `amqp10.gsub.{name}`, durable |
| Shared non-durable | capability `shared` | `amqp10.vsub.{container-id}.{name}`, deleted when its last link detaches |
| Global shared non-durable | capabilities `shared` and `global` | `amqp10.gvsub.{name}`, deleted when its last link detaches |

The container-id is the one the client sent in its `open` (the JMS client ID with Qpid JMS). The name of a shared subscription is its link name up to the first `|`, as Qpid JMS names the links of a shared subscription `{name}`, `{name}|2`, `{name}|global`, `{name}|volatile1` and so on. A global subscription is the same for every container in the vhost. The source LavinMQ answers with has `durable` set to `configuration` at most, as unsettled state is not kept across links.

- A durable subscription keeps receiving messages while no link is attached. It is kept when its link detaches without closing, the session ends or the connection closes, and also across restarts.
- A link that detaches with `closed` set unsubscribes: the subscription's queue is deleted, or, if other links still consume from it, the detach is answered with `amqp:resource-locked` and it is kept.
- An unshared durable subscription takes one link at a time; another gets `amqp:resource-locked`, as does an unshared link to a subscription shared links consume from, and vice versa.
- Attaching to a subscription with a different address replaces it, dropping its messages, as a JMS subscription changing its topic does. While links consume from it, that is refused with `amqp:resource-locked`.
- A receiving link without a source whose link name is a durable subscription's (`{name}`, or `{name}|global` for a global one) attaches to it, and LavinMQ answers with its source. Qpid JMS looks up a subscription this way to unsubscribe from it, then detaches with `closed` set.

LavinMQ offers the `SHARED-SUBS` capability in its `open`, as Qpid JMS requires to create shared subscriptions. Selectors (source filters), `no-local` and `distribution-mode` `copy` are not supported. Qpid JMS asks for `copy` on durable subscriptions; it is ignored, and makes no difference, since each subscription has a queue of its own.

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

## Transactions

Local transactions are supported, as JMS transacted sessions use them: a link whose target is a `coordinator` declares and discharges transactions, and transfers and dispositions that carry a `transactional-state` with a transaction's id take part in it. Distributed transactions are refused with `amqp:not-implemented`.

- A message published in a transaction is answered with a `transactional-state` holding `accepted` at once, and routed when the transaction is discharged without failure. A message unroutable by then is dropped.
- An outcome applied to a delivery in a transaction takes effect when the transaction is discharged without failure.
- Discharging with `fail` set, or detaching the coordinator link or closing the connection with the transaction open, rolls it back: its messages are dropped, and deliveries the client acknowledged and settled in it are requeued, as the client no longer holds them. Deliveries it left unsettled stay unacknowledged.
- A transaction id that is not open is answered with `amqp:transaction:unknown-id`: a discharge or transfer is rejected with it, and a disposition ends the session with it.

As with transactions in AMQP 0-9-1, a commit is not atomic across queues. Transactional acquisition (a `txn-id` in a flow's properties) is not supported; deliveries are acquired outside the transaction.

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

- Link recovery (the `unsettled` map on attach), durable termini other than [subscriptions](#subscriptions), and source filters (JMS selectors)
- Management (`$management`) links
- Direct reply-to via `amq.direct.reply-to` addresses
- SASL mechanisms other than `PLAIN` and `ANONYMOUS`

## Further Reading

- [AMQP 0-9-1](amqp.md) — the protocol messages are stored and interoperate in
- [Connections](connections.md) — heartbeats, proxy protocol, connection limits
- [Consumers](consumers.md) — consumer timeout, single active consumer, priorities
- [Queues](queues.md) — queue types and arguments
