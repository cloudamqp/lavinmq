# Shovels

Shovels move messages from a source to one or more destinations. They are useful for bridging brokers, forwarding messages to HTTP endpoints, or moving messages between queues.

## How It Works

Each shovel runs as an independent fiber owned by its vhost. When started, it opens an AMQP connection to the source URI and a connection (or HTTP client) to the destination URI:

1. **Source setup.** If `src-queue` is set, the shovel consumes directly from that queue. If only `src-exchange` (and optionally `src-exchange-key`) is set, the shovel declares an anonymous, exclusive queue, binds it to that exchange, and consumes from the anonymous queue. The source channel uses `src-prefetch-count` for backpressure.
2. **Pull loop.** Messages from the source consumer are pushed one by one to the destination's `push` method. For AMQP destinations this becomes `basic.publish` to `dest-exchange` with `dest-exchange-key` (or to the default exchange when `dest-queue` is set). For HTTP destinations, the message body is POSTed to `dest-uri`.
3. **Acknowledgment.** The destination classifies each delivery into an [outcome](#delivery-outcomes), and the shovel acks, retries, dead-letters, or aborts the source message accordingly. The configured `ack-mode` controls *when* the outcome is reported (see [Acknowledgment Modes](#acknowledgment-modes)).
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

| Parameter | Description |
|-----------|-------------|
| `dest-uri` | HTTP/HTTPS URL to POST to. Userinfo (`user:password@host`) is sent as HTTP Basic Auth. |

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

For `on-confirm` and `on-publish` ack modes, the HTTP response status is classified into a [delivery outcome](#delivery-outcomes) rather than triggering a uniform retry:

- `2xx` acks the message.
- `408`, `429`, and `5xx` (and transport-level failures such as connection refused or read timeout) requeue the message and retry it with backoff.
- `400` and `422` reject the message without requeue, so the source queue's dead-letter exchange handles it.
- Any other status (e.g. `401`, `403`, `404`, `405`, `410`) is treated as an unusable endpoint; after repeated consecutive failures the shovel errors out for an operator to resolve.

`no-ack` skips the check. See [Shovel delivery outcomes](shovel-delivery-outcomes.md) for the full status-to-outcome mapping.

## Multi-Destination

A shovel can have multiple destinations configured. They form an **ordered failover list**, not a load-balanced or round-robin pool: one destination is active at a time, starting with the first one that can be reached. All consumed messages go to the active destination.

When the active destination is classified as unusable (an `Abort` [outcome](#delivery-outcomes)) or fails to start, the shovel advances to the next destination in the list and retries the message there. A successful — or otherwise non-abort — delivery resets the failover cycle. Only once *every* destination has failed in a row, with no successful delivery in between, does the shovel error out.

## Acknowledgment Modes

| Mode | Description |
|------|-------------|
| `on-confirm` (default) | Ack source after destination confirms receipt |
| `on-publish` | Ack source after publishing to destination (before confirm) |
| `no-ack` | No acknowledgment (fastest, may lose messages) |

## Delivery Outcomes

For every message, the destination classifies the delivery attempt into one **outcome**, and the shovel turns that outcome into an action on the source. This is what decides whether a message is acked, retried, dead-lettered, or treated as a fatal destination problem.

| Outcome | Source action | Meaning |
|---------|---------------|---------|
| `Confirmed` | ack | Delivered. Resets the failure and abort counters. |
| `Retry` | reject (requeue) | Transient failure. Retried on the source with capped exponential backoff; retries are unbounded. |
| `Reject` | reject (no requeue) | The message itself is unacceptable. Dead-lettered via the source queue's DLX (dropped if none configured); the shovel continues. |
| `Abort` | reject (requeue) | The destination is unusable. The message is kept; after 10 consecutive aborts the shovel errors out for an operator to resolve. |

How each destination type maps its native result (HTTP status, AMQP publisher confirm) to these outcomes, and how `ack-mode` affects *when* an outcome is reported, is documented in [Shovel delivery outcomes](shovel-delivery-outcomes.md).

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
