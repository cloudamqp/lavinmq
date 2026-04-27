# Shovels

Shovels move messages from a source to one or more destinations. They are useful for bridging brokers, forwarding messages to HTTP endpoints, or moving messages between queues.

## Components

A shovel consists of:

- **Source** — an AMQP queue to consume from
- **Destination** — one or more targets to publish to (AMQP exchange or HTTP endpoint)

## Source Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `src-uri` | (required) | AMQP URI of the source broker |
| `src-queue` | (required) | Queue to consume from |
| `src-prefetch-count` | `1000` | Prefetch count |
| `src-delete-after` | `never` | Delete shovel after transfer: `never` or `queue-length` |

## AMQP Destination

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dest-uri` | (required) | AMQP URI of the destination broker |
| `dest-exchange` | (none) | Exchange to publish to |
| `dest-exchange-key` | (none) | Routing key to use |
| `dest-queue` | (none) | Queue to publish to (via default exchange) |

## HTTP Destination

Shovels can POST messages to an HTTP endpoint:

| Parameter | Description |
|-----------|-------------|
| `dest-uri` | HTTP/HTTPS URL to POST to |

The message body is sent as the request body.

## Multi-Destination

A shovel can publish to multiple destinations simultaneously. Each consumed message is forwarded to all configured destinations.

## Acknowledgment Modes

| Mode | Description |
|------|-------------|
| `on-confirm` (default) | Ack source after destination confirms receipt |
| `on-publish` | Ack source after publishing to destination (before confirm) |
| `no-ack` | No acknowledgment (fastest, may lose messages) |

## Shovel States

| State | Description |
|-------|-------------|
| `starting` | Initializing connections |
| `running` | Actively shoveling messages |
| `stopped` | Stopped (e.g., `delete-after: queue-length` completed) |
| `paused` | Temporarily paused |
| `terminated` | Permanently terminated |
| `error` | Failed (will attempt reconnection) |

## Reconnection

Shovels automatically reconnect on failure with a default delay of 5 seconds between attempts.

## Management

Shovels are configured as parameters (component: `shovel`) and can be managed via the HTTP API or CLI.
