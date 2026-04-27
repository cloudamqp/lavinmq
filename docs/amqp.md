# AMQP 0-9-1

LavinMQ implements the AMQP 0-9-1 protocol. This page covers LavinMQ's specific defaults, capabilities, and extensions. For the full protocol specification, see the [AMQP 0-9-1 spec](https://www.amqp.org/specification/0-9-1/amqp-org-download).

## Ports

| Protocol | Default Port | Config Key |
|----------|-------------|------------|
| AMQP | 5672 | `amqp_port` |
| AMQPS | 5671 | `amqps_port` |
| AMQP over WebSocket | via HTTP port (15672) | `http_port` |

Unix domain sockets are also supported via `amqp_unix_path`.

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
- **connection.blocked** — server notifies publishers when resources are low. See [Connections](connections.md).
- **connection.update-secret** — refresh OAuth2 tokens on live connections. See [Authentication](authentication.md).

## Authentication Mechanisms

- **PLAIN** — username and password
- **AMQPLAIN** — AMQP-specific encoding of username and password

Credentials are validated against the configured [authentication chain](authentication.md).

## LavinMQ-Specific Behavior

- **Blocked connections**: when disk space drops below `free_disk_min`, publishing is paused via `connection.blocked` until resources recover.
- **Consumer timeout**: idle consumers holding unacknowledged messages can be disconnected. See [Consumers](consumers.md).
- **Channel flow**: the server can pause delivery on a channel via `channel.flow`. See [Channels](channels.md).
- **Transactions**: supported but mutually exclusive with publisher confirms on the same channel. See [Transactions](transactions.md).

## Further Reading

- [Connections](connections.md) — connection lifecycle, heartbeats, proxy protocol
- [Channels](channels.md) — channel multiplexing, prefetch, flow control
- [Exchanges](exchanges.md) — exchange types and routing
- [Queues](queues.md) — queue types and arguments
- [Messages](messages.md) — publishing, properties, CC/BCC, direct reply-to
- [Consumers](consumers.md) — acknowledgment, prefetch, single active consumer
