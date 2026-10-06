# Shovels

Shovels move messages from a source to one or more destinations. They are useful for bridging brokers, forwarding messages to HTTP endpoints, or moving messages between queues.

## How It Works

Each shovel runs as an independent fiber owned by its vhost. When started, it opens a session with the source URI and one with the destination URI (or an HTTP client), see [Endpoints](#endpoints):

1. **Source setup.** If `src-queue` is set, the shovel consumes directly from that queue. If only `src-exchange` (and optionally `src-exchange-key`) is set, the shovel declares an anonymous, exclusive queue, binds it to that exchange, and consumes from the anonymous queue. The source channel uses `src-prefetch-count` for backpressure.
2. **Pull loop.** Messages from the source consumer are pushed one by one to the destination's `push` method. For AMQP destinations this becomes `basic.publish` to `dest-exchange` with `dest-exchange-key` (or to the default exchange when `dest-queue` is set). For HTTP destinations, the message body is POSTed to `dest-uri`.
3. **Acknowledgment.** The destination classifies each delivery into an [outcome](#delivery-outcomes), and the shovel acks, retries, dead-letters, or aborts the source message accordingly. The configured `ack-mode` controls *when* the outcome is reported (see [Acknowledgment Modes](#acknowledgment-modes)).
4. **Lifecycle.** A state machine moves the shovel between `starting`, `running`, `paused`, `error`, `aborted`, `stopped`, and `terminated` (see [Shovel States](#shovel-states)). Errors trigger an exponential-backoff reconnect; pause is persisted to disk so a paused shovel stays paused across server restarts.
5. **Self-deletion.** With `src-delete-after: queue-length`, the shovel deletes its own parameter (and stops itself) once it has moved as many messages as were in the source queue when it started (see [Queue-length runs](#queue-length-runs)).

## Endpoints

An AMQP `src-uri` or `dest-uri` names either this broker or another one:

- **This broker, in-process.** A URI without host, `amqp://` (the `/` vhost) or `amqp:///vhost`, is this broker. The shovel works directly against the vhost: it is registered as a consumer of the source queue and publishes straight into the destination, without opening any AMQP connection and without logging in as any user. It works whatever ports the AMQP listeners use. Publishes to it are confirmed once they are durable (and replicated to in-sync followers), like a publisher confirm. Use `amqp:///other-vhost` to move messages between vhosts.
- **Another broker, over AMQP.** Any URI with a host, `localhost` included, is reached with an AMQP connection using the credentials in the URI, e.g. `amqps://user:password@broker.example.com/vhost`.

Because an in-process endpoint has no credentials of its own, the user creating the shovel must have the permissions the shovel needs there: `read` and `configure` on the source queue or exchange, `write` and `configure` on the destination exchange, and `configure` on the destination queue. A shovel parameter that names a vhost or resource the user can't access is refused. A remote broker checks the URI's credentials itself.

## Components

A shovel consists of:

- **Source** — an AMQP queue or exchange to consume from
- **Destination** — one or more targets to publish to (AMQP exchange or HTTP endpoint)

## Source Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `src-uri` | (required) | AMQP URI of the source, see [Endpoints](#endpoints). A list of URIs picks one at random on every start. |
| `src-queue` | (none) | Queue to consume from |
| `src-exchange` | (none) | Exchange to bind to (creates a temporary queue) |
| `src-exchange-key` | (none) | Routing key for the exchange binding |
| `src-prefetch-count` | `1000` | Prefetch count |
| `src-delete-after` | `never` | Delete shovel after transfer: `never` or `queue-length` |
| `src-consumer-args` | (none) | Consumer arguments, e.g. `{"x-stream-offset": "first"}` to shovel a stream from its start. String, integer and boolean values are passed on. |

## AMQP Destination

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dest-uri` | (required) | AMQP URI of the destination, see [Endpoints](#endpoints) |
| `dest-exchange` | (none) | Exchange to publish to |
| `dest-exchange-key` | (none) | Routing key to use |
| `dest-queue` | (none) | Queue to publish to (via default exchange) |

Delivery is judged by the destination broker's publisher confirm; see [AMQP publisher-confirm classification](#amqp-publisher-confirm-classification).

## HTTP Destination

A shovel with an `http://` or `https://` `dest-uri` POSTs each consumed message to the endpoint instead of republishing it over AMQP. Useful for delivering broker traffic to webhook receivers, serverless handlers, or any HTTP service.

| Parameter | Description |
|-----------|-------------|
| `dest-uri` | HTTP/HTTPS URL to POST to. Userinfo (`user:password@host`) is sent as HTTP Basic Auth. |
| `dest-timeout` | Connect and read timeout for each HTTP attempt, in seconds (int or float). Defaults to `30`. Must be a positive number; any other value is rejected when the parameter is created. Editable in the management UI as the destination's *Timeout* field, shown for HTTP URIs. |

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

A shovel can have multiple destinations configured. Each time the shovel starts it picks **one of them at random** and delivers every consumed message to it for the whole run. The list is neither an ordered preference nor a load-balanced pool: there is no failover between destinations.

The chosen destination is treated exactly like a single one. Its [delivery outcomes](#delivery-outcomes) count towards the shovel's backoff and abort threshold, and if it cannot be reached the shovel reconnects with backoff. Every start draws again, so a reconnect, or a pause followed by a resume, may land on a different destination.

## Source Acknowledgments

An in-process source acks each message as soon as it is settled. A source on another broker acks in batches for throughput: the shovel sends one cumulative ack (`multiple: true`) once half the prefetch window has been settled, as soon as nothing is left in flight (every message delivered to the shovel is settled, so nothing would grow the batch), or at most 3 seconds after the first ack of a batch, whichever comes first. Under load acks go out once per half prefetch window; when traffic stops, the last ack goes out immediately. A cumulative ack only ever covers tags whose delivery has actually been settled (confirmed, or rejected), and it names the highest *confirmed* tag in that range, never a rejected one: a reject has already settled its tag at the broker, and a cumulative ack for a tag the broker no longer holds is a channel error. If a destination confirms out of order — RabbitMQ may confirm message 3 before message 2 — the ack stops at the lowest unconfirmed tag and the higher ones wait until the gap closes. Rejects (requeue or dead-letter) are sent individually and at once.

Pause, terminate and abort flush the pending batch before closing the source. A message in flight at that moment is not acked; it stays on the source and is redelivered on the next run, so the shovel is at-least-once.

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
| Pending confirms voided by the destination connection closing | `Retry`. When the destination connection drops on its own the source is still open, so every message in flight on it is requeued there. When the whole shovel is stopping (pause, terminate, abort) the source has already been closed, which requeued them, and the shovel ignores the reports: they count neither as retries nor towards the backoff. |

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

A reconnect is a fresh start: both the source and the destination are stopped before the delay, and a [multi-destination](#multi-destination) shovel draws a destination at random again when it restarts.

## Management

Shovels are configured as parameters (component: `shovel`) and can be managed via the HTTP API or CLI.
