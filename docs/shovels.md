# Shovels

Shovels move messages from a source to one or more destinations. They are useful for bridging brokers, forwarding messages to HTTP endpoints, or moving messages between queues.

## How It Works

Each shovel runs as an independent fiber owned by its vhost. When started, it opens an AMQP connection to the source URI and a connection (or HTTP client) to the destination URI:

1. **Source setup.** If `src-queue` is set, the shovel consumes directly from that queue. If only `src-exchange` (and optionally `src-exchange-key`) is set, the shovel declares an anonymous, exclusive queue, binds it to that exchange, and consumes from the anonymous queue. The source channel uses `src-prefetch-count` for backpressure.
2. **Pull loop.** Messages from the source consumer are pushed one by one to the destination's `push` method. For AMQP destinations this becomes `basic.publish` to `dest-exchange` with `dest-exchange-key` (or to the default exchange when `dest-queue` is set). For HTTP destinations, the message body is POSTed to `dest-uri`.
3. **Acknowledgment.** Source acks are gated by the configured `ack-mode` (see [Acknowledgment Modes](#acknowledgment-modes)).
4. **Lifecycle.** A state machine moves the shovel between `starting`, `running`, `paused`, `error`, `stopped`, and `terminated` (see [Shovel States](#shovel-states)). Errors trigger an exponential-backoff reconnect; pause is persisted to disk so a paused shovel stays paused across server restarts.
5. **Self-deletion.** With `src-delete-after: queue-length`, the shovel deletes its own parameter (and stops itself) once the source queue has been drained.

## Components

A shovel consists of:

- **Source** — an AMQP queue or exchange to consume from
- **Destination** — one or more targets to publish to (AMQP exchange or HTTP endpoint)

## Source Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `src-uri` | (required) | AMQP URI of the source broker |
| `src-queue` | (none) | Queue to consume from |
| `src-exchange` | (none) | Exchange to bind to (creates a temporary queue) |
| `src-exchange-key` | (none) | Routing key for the exchange binding |
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

A shovel with an `http://` or `https://` `dest-uri` POSTs each consumed message to the endpoint instead of republishing it over AMQP. Useful for delivering broker traffic to webhook receivers, serverless handlers, or any HTTP service.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dest-uri` | (required) | HTTP/HTTPS URL to POST to. Userinfo (`user:password@host`) is sent as HTTP Basic Auth. |
| `dest-max-retries` | `0` | Number of times to retry a failed delivery (non-2xx response or timeout) before the delivery fails. `0` disables retries. Only applies to the `on-confirm` and `on-publish` ack modes. |
| `dest-backoff` | `2.0` | Base, in seconds, for the exponential backoff between retries; the delay before retry _n_ is `backoff`<sup>_n_</sup> seconds. |
| `dest-jitter` | `1.0` | Maximum random delay, in seconds, added to each backoff to spread out retries. |
| `dest-timeout` | `30.0` | Per-request HTTP read timeout, in seconds. |

The AMQP message is mapped to the HTTP request as follows:

| HTTP element | Source |
|--------------|--------|
| Method | `POST` |
| Path | The `dest-uri` path if set, else the message header `uri_path`, else `/` |
| Body | The raw AMQP message body |
| `Content-Type` | The message `content_type` property, if set |
| `X-Message-Id` | The message `message_id` property, if set |
| `X-Shovel` | The shovel name |
| `X-<header>` | One header per AMQP header on the message |
| `User-Agent` | `LavinMQ` |

For `on-confirm` and `on-publish` ack modes, a delivery is considered successful only when the endpoint returns a 2xx response. A non-2xx response (or a timeout) is retried in place up to `dest-max-retries` times with exponential backoff (`dest-backoff` plus `dest-jitter`) before the delivery fails and the shovel's reconnect path takes over; the full message body is resent on every attempt. `no-ack` makes a single POST and never retries, regardless of the response.

## Multi-Destination

A shovel can have multiple destinations configured. One destination is randomly selected when the shovel starts, and all consumed messages are forwarded to that single destination until the shovel restarts (e.g., on reconnection).

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

Shovels automatically reconnect on failure with a default base delay of 5 seconds. After 10 consecutive retries, the delay increases exponentially up to a maximum of 300 seconds.

## Management

Shovels are configured as parameters (component: `shovel`) and can be managed via the HTTP API or CLI.
