# Shovels

Shovels move messages from a source to one or more destinations. They are useful for bridging brokers, forwarding messages to HTTP endpoints, or moving messages between queues.

## How It Works

Each shovel runs as an independent fiber owned by its vhost. When started, it opens an AMQP connection to the source URI and a connection (or HTTP client) to the destination URI:

1. **Source setup.** If `src-queue` is set, the shovel consumes directly from that queue. If only `src-exchange` (and optionally `src-exchange-key`) is set, the shovel declares an anonymous, exclusive queue, binds it to that exchange, and consumes from the anonymous queue. The source channel uses `src-prefetch-count` for backpressure.
2. **Pull loop.** Messages from the source consumer are pushed one by one to the destination's `push` method. For AMQP destinations this becomes `basic.publish` to `dest-exchange` with `dest-exchange-key` (or to the default exchange when `dest-queue` is set). For HTTP destinations, the message body is POSTed to `dest-uri`.
3. **Acknowledgment.** The destination classifies each delivery into an [outcome](#delivery-outcomes), and the shovel acks, retries, dead-letters, or aborts the source message accordingly. The configured `ack-mode` controls *when* the outcome is reported (see [Acknowledgment Modes](#acknowledgment-modes)).
4. **Lifecycle.** A state machine moves the shovel between `starting`, `running`, `paused`, `error`, `aborted`, `stopped`, and `terminated` (see [Shovel States](#shovel-states)). Errors trigger an exponential-backoff reconnect; pause is persisted to disk so a paused shovel stays paused across server restarts.
5. **Self-deletion.** With `src-delete-after: queue-length`, the shovel deletes its own parameter (and stops itself) once it has moved as many messages as were in the source queue when it started (see [Queue-length runs](#queue-length-runs)).

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

Delivery is judged by the destination broker's publisher confirm; see [AMQP publisher-confirm classification](#amqp-publisher-confirm-classification).

## HTTP Destination

A shovel with an `http://` or `https://` `dest-uri` POSTs each consumed message to the endpoint instead of republishing it over AMQP. Useful for delivering broker traffic to webhook receivers, serverless handlers, or any HTTP service.

| Parameter | Description |
|-----------|-------------|
| `dest-uri` | HTTP/HTTPS URL to POST to. Userinfo (`user:password@host`) is sent as HTTP Basic Auth. |
| `dest-timeout` | Connect and read timeout for each HTTP attempt, in seconds (int or float). Defaults to `30`. |

The AMQP message is mapped to the HTTP request as follows:

| HTTP element | Source |
|--------------|--------|
| Method | `POST` |
| Path | The `dest-uri` path if set, else the message header `uri_path`, else `/` |
| Body | The raw AMQP message body, sent with a `Content-Length` header (not chunked) |
| `Content-Type` | The message `content_type` property, if set |
| `X-Message-Id` | The message `message_id` property, if set |
| `X-Shovel` | The shovel name |
| `X-<header>` | One header per AMQP header on the message |
| `User-Agent` | `LavinMQ` |

With `on-confirm` and `on-publish` alike, the response status decides the [delivery outcome](#delivery-outcomes): `2xx` acks the message, `408`/`429`/`5xx` and transport failures requeue it with backoff, statuses that describe the message itself (`400`, `413`, `415`, …) dead-letter it, and anything else marks the endpoint unusable. The full mapping is in [HTTP status classification](#http-status-classification). The two modes are the same for HTTP because the response is always awaited; there is no earlier "published" moment to ack at, and a failed POST is never acked. `no-ack` POSTs once and never settles the source.

An in-flight request cannot be cancelled. On pause, the current request drains under `dest-timeout` while no new deliveries are started, and the in-flight message is redelivered on resume (the shovel is at-least-once).

## Multi-Destination

A shovel can have multiple destinations configured. They form an **ordered failover list**, not a load-balanced or round-robin pool: one destination is active at a time, starting with the first one that can be reached. All consumed messages go to the active destination.

When the active destination is classified as unusable (an `Abort` [outcome](#delivery-outcomes)) or fails to start, the shovel advances to the next destination in the list and retries the message there. A destination that keeps failing transiently (three consecutive `Retry` outcomes, e.g. connection refused on a host that is down) is skipped in favour of the next one as well. A successful — or otherwise non-abort — delivery resets the failover cycle.

The switch itself happens on the next delivery, not inside the outcome that asked for it: the message is requeued on the source, and when it is redelivered the shovel stops the old destination and activates the next one before publishing. Stopping the old destination requeues every message that was still in flight on it (see [publisher-confirm classification](#amqp-publisher-confirm-classification)), so nothing published to a destination that never confirmed is lost. Only once *every* destination has aborted in a row, with no successful delivery in between, do the aborts count towards the shovel's abort threshold; even then each redelivery still goes to the next destination rather than hammering one. Every (re)start of the shovel begins again with the first destination in the list, and if no destination at all can be started the shovel reconnects with backoff exactly as it would for a single unreachable destination.

## Source Acknowledgments

Source messages are acked in batches for throughput: the shovel sends one cumulative ack (`multiple: true`) once half the prefetch window has been settled, or after a timeout of 3 seconds, whichever comes first. A cumulative ack only ever covers tags whose delivery has actually been settled (confirmed, or rejected). If a destination confirms out of order — RabbitMQ may confirm message 3 before message 2 — the ack stops at the lowest unconfirmed tag and the higher ones wait until the gap closes. Rejects (requeue or dead-letter) are sent individually and at once.

Pause, terminate and abort flush the pending batch before closing the source connection. A message in flight at that moment is not acked; it stays on the source and is redelivered on the next run, so the shovel is at-least-once.

### Queue-length runs

With `src-delete-after: queue-length` the shovel takes the queue's message count when it starts and finishes once that many messages have been settled for good, i.e. acked or dead-lettered. Then it deletes its own parameter.

- A message whose delivery fails transiently is requeued, redelivered and retried with backoff, so the run does not finish while such a message remains.
- A message published after the start can be delivered into a slot a settled message freed. It is moved like any other and counts towards the total, so a run moves *as many* messages as were on the queue at start, which in practice are the ones that were there: a requeued message goes back to its place ahead of anything newer. Nothing delivered is ever skipped and left unacked.
- If the broker drops a requeued message instead of redelivering it (a `x-delivery-limit` exceeded, or a TTL expiring), it can never be settled by the shovel. When a requeue leaves nothing in flight, the shovel checks the queue once the redelivery has had time to arrive, and finishes if the queue is empty.
- Every start of a run, including a resume and a reconnect, takes a fresh snapshot of what is left on the queue and counts from zero against it.
- Messages still in flight when the run finishes stay on the source; whether they were also delivered depends on the destination's confirm having arrived, so a very small number of duplicates is possible at the boundary (at-least-once).

## Acknowledgment Modes

| Mode | Source is settled | When the outcome is reported |
|------|-------------------|------------------------------|
| `on-confirm` (default) | After the destination confirms receipt | AMQP: on the asynchronous publisher-confirm callback (`Confirmed` or `Retry`). HTTP: after the response, classified by status. |
| `on-publish` | After publishing, before any confirm | AMQP: `Confirmed` right after `basic.publish`. HTTP: identical to `on-confirm`, the response status is classified. |
| `no-ack` | Never (fastest, may lose messages) | Nothing is reported. The source consumes with `no-ack`, so there is nothing to settle. |

## Delivery Outcomes

For every message, the destination classifies the delivery attempt into one **outcome**, and the shovel turns that outcome into an action on the source. Classification is fixed per destination type; the shovel owns the policy: acking, requeueing, backoff and the abort threshold. This is what decides whether a message is acked, retried, dead-lettered, or treated as a fatal destination problem.

| Outcome | Source action | Meaning |
|---------|---------------|---------|
| `Confirmed` | ack | Delivered. Resets the failure and abort counters. |
| `Retry` | reject (requeue) | Transient failure. Retried on the source with capped exponential backoff (0.5s doubling to 30s, per failing round); retries are unbounded. |
| `Reject` | reject (no requeue) | The message itself is unacceptable. Dead-lettered via the source queue's DLX; the shovel continues. |
| `Abort` | reject (requeue) | The destination is unusable. The message is kept; after 10 consecutive aborts the shovel moves to the `aborted` state for an operator to resolve (see [Shovel States](#shovel-states)). |

A `Reject` on a source queue with no dead-letter exchange drops the message silently. Surfacing a UI warning for this is tracked separately.

### HTTP status classification

| Condition | Outcome |
|-----------|---------|
| `200`–`299` | `Confirmed` |
| `408`, `429`, `500`–`599` | `Retry` |
| `400`, `411`, `413`, `414`, `415`, `422`, `431` — the request's body size, `Content-Type`, `uri_path` and headers all come from the message | `Reject` |
| Any other non-2xx (`401`, `403`, `404`, `405`, `410`, `3xx`, `418`, …) | `Abort` |
| Transport failure during the request (connection refused or reset, read timeout, TLS handshake error) | `Retry` |

There is no in-place retry of a failed request; a `Retry` goes straight back to the source with backoff. The one exception is a keep-alive connection the endpoint has silently closed, which only shows up as the next request dying on it with EOF or a reset. That request is retried once on a fresh connection before anything is reported. Timeouts, refused connections and failures on a fresh connection are not retried in place.

### AMQP publisher-confirm classification

| Condition | Outcome |
|-----------|---------|
| Publisher-confirm **ack** | `Confirmed` |
| Publisher-confirm **nack** (e.g. `x-overflow: reject-publish` on a full destination queue) | `Retry` |
| Connection or channel error mid-publish | Not an outcome. The error goes through the [reconnect](#reconnection) loop; a `404` channel-close ("queue deleted") stops the shovel. |
| Pending confirms voided by the destination connection closing | `Retry`. On a [failover](#multi-destination) the source is still open, so every message in flight on the old destination is requeued there. When the whole shovel is stopping (pause, terminate, abort) the source has already been closed, which requeued them, and the shovel ignores the reports: they count neither as retries nor towards the backoff. |

## Shovel States

| State | Description |
|-------|-------------|
| `starting` | Initializing connections |
| `running` | Actively shoveling messages |
| `stopped` | Stopped (e.g., `delete-after: queue-length` completed) |
| `paused` | Temporarily paused |
| `terminated` | Permanently terminated |
| `error` | Failed (will attempt reconnection) |
| `aborted` | The destination was classified unusable 10 times in a row (see [Delivery Outcomes](#delivery-outcomes)). The run is stopped the same way a pause stops it: both connections are closed cleanly and every unacked message stays on the source. The shovel stays here, with the reason in `error`, and does not reconnect. Resume it (`PUT /api/shovels/:vhost/:name/resume`, or the Resume button in the management UI) once the destination is fixed, or recreate its parameter. A resumed shovel starts with clean failure and abort counters. |

## Reconnection

Shovels automatically reconnect on failure with a default base delay of 5 seconds. After 10 consecutive retries, the delay increases exponentially up to a maximum of 300 seconds.

A reconnect is a fresh start: both the source and the destination are stopped before the delay, and a [multi-destination](#multi-destination) shovel begins with the first destination in its list again rather than staying on whatever it had failed over to.

## Management

Shovels are configured as parameters (component: `shovel`) and can be managed via the HTTP API or CLI.
