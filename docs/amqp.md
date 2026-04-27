# AMQP 0-9-1

LavinMQ implements the AMQP 0-9-1 protocol. This page covers LavinMQ's specific defaults, capabilities, and extensions. For the full protocol specification, see the [AMQP 0-9-1 spec](https://www.amqp.org/specification/0-9-1/amqp-org-download).

## Ports

| Protocol | Default Port | Config Key |
|----------|-------------|------------|
| AMQP | 5672 | `amqp_port` |
| AMQPS | 5671 | `amqps_port` |
| AMQP over WebSocket | via HTTP port (15672) | `http_port` |

Unix domain sockets are also supported via `unix_path` in the `[amqp]` config section.

## Server Defaults

| Parameter | Default | Config Key |
|-----------|---------|------------|
| `frame_max` | 131,072 bytes | `frame_max` |
| `channel_max` | 2,048 | `channel_max` |
| `heartbeat` | 300 seconds | `heartbeat` |
| `max_message_size` | 128 MB | `max_message_size` |
| `default_consumer_prefetch` | 65,535 | `default_consumer_prefetch` |

The server proposes these values during connection negotiation. The client may negotiate lower (but not higher) for `frame_max` and `channel_max`. For heartbeat, the lower non-zero value is used.

## Capabilities

LavinMQ advertises the following extensions beyond the base AMQP 0-9-1 spec:

- **Publisher confirms** — acknowledgment that the server received a message. See [Publisher Confirms](publisher-confirms.md).
- **Consumer cancel notify** — server sends `basic.cancel` when a queue is deleted. See [Consumers](consumers.md).
- **Exchange-to-exchange bindings** — bind exchanges to other exchanges. See [Bindings](bindings.md).
- **basic.nack** — negative acknowledgment with multi-message support. See [Consumers](consumers.md).
- **Per-consumer QoS** — prefetch scoped to individual consumers. See [Channels](channels.md).
- **Direct reply-to** — RPC without temporary queues. See [Messages](messages.md).
- **Authentication failure close** — server closes the connection with a reason on auth failure.
- **Consumer priorities** — consumers can declare priority to influence delivery order. See [Consumers](consumers.md).

## Authentication Mechanisms

- **PLAIN** — username and password
- **AMQPLAIN** — AMQP-specific encoding of username and password

Credentials are validated against the configured [authentication chain](authentication.md).

## LavinMQ-Specific Behavior

- **Blocked publishing**: when free disk space drops below `3 * segment_size` or below `free_disk_min`, `basic.publish` returns a `precondition_failed` channel error until resources recover.
- **Consumer timeout**: a consumer's channel is closed if its oldest unacknowledged message exceeds the configured timeout. See [Consumers](consumers.md).
- **Channel flow**: clients can pause their own delivery via `channel.flow`. See [Channels](channels.md).
- **Transactions**: supported but mutually exclusive with publisher confirms on the same channel. See [Transactions](transactions.md).

## Further Reading

- [Connections](connections.md) — connection lifecycle, heartbeats, proxy protocol
- [Channels](channels.md) — channel multiplexing, prefetch, flow control
- [Exchanges](exchanges.md) — exchange types and routing
- [Queues](queues.md) — queue types and arguments
- [Messages](messages.md) — publishing, properties, CC/BCC, direct reply-to
- [Consumers](consumers.md) — acknowledgment, prefetch, single active consumer
