# Replay Queues

A replay queue is a special queue type aimed at operator-facing message review. Messages enter through broker-internal paths only (DLX routing, a planned live filter's `move_to`, manual broker publish), carry origin headers that identify where they came from, and are inspected, edited, or released through a dedicated HTTP + UI surface.

A replay queue is **not** a regular queue. It refuses unstampable messages, has a low expected volume, and is intended for manual operator workflows — fix-and-resend after a bug, audit a captured sample, inspect dead-lettered messages without writing a consumer.

## Declaring a replay queue

Replay queues are always durable. Declaring a non-durable replay queue is rejected with `PRECONDITION_FAILED`.

```http
PUT /api/queues/:vhost/:name
Content-Type: application/json

{
  "durable": true,
  "arguments": {
    "x-queue-type": "replay"
  }
}
```

## Acceptance policy

Every message arriving at a replay queue is stamped on intake:

* `x-replay-id` — UUID assigned by the broker. Operators address messages by this id.
* `x-source-queue`, `x-source-exchange`, `x-source-routing-key` — origin metadata used by the release endpoint to send the message back where it came from.
* `x-source-timestamp` — server time (ms since epoch) at intake.

If the inbound message does not already carry `x-source-queue` but does carry `x-first-death-queue` (DLX-routed message), the broker derives the `x-source-*` triplet from `x-first-death-*`. Missing `x-first-death-exchange` and `x-first-death-routing-key` fall back to the message's current exchange and routing-key — useful because LavinMQ's DLX layer does not stamp `x-first-death-routing-key` today.

If the message has neither `x-source-queue` nor `x-first-death-queue`, the broker refuses the push (returns `false` from `publish_internal`, logs a warning). The replay queue never holds messages it cannot replay.

## HTTP API

All endpoints are nested under `/api/replay`. Reads require the `management` tag; writes require `policymaker`.

### `GET /api/replay/:vhost`

Lists every queue with `x-queue-type: replay` in the vhost.

### `GET /api/replay/:vhost/:name`

Lists items in a replay queue with origin metadata, content type, payload size, and `x-replay-id`. Linear scan under the message store lock; intended for low-volume operator inspection, not high-throughput polling.

### `GET /api/replay/:vhost/:name/:id`

Returns the full envelope for the message with the given `x-replay-id`: origin metadata, properties block, and payload (utf-8 string when valid, base64 otherwise).

### `PATCH /api/replay/:vhost/:name/:id`

Replaces body and/or non-origin headers. Body keys:

* `body` — string payload replacement. Gated on the content-type allowlist (`text/*`, `application/json`, `application/xml`, `application/x-www-form-urlencoded`). Pass `?force=true` to override.
* `headers` — object replacing the user-authored header set. Any `x-source-*` / `x-replay-id` keys in the payload are silently dropped so an operator cannot tamper with origin metadata.

The edit is publish-new + delete-old since the message store has no in-place mutation. The new message gets a fresh `x-replay-id`; the old id becomes invalid.

### `POST /api/replay/:vhost/:name/:id/release`

Republishes to the original exchange and routing-key derived from `x-source-*`. `x-source-queue` is kept on the released message by default so any source-side filter can short-circuit (no replay-loop). Pass `?reset_replay=true` to strip `x-source-*` entirely so the source's filter evaluates the released message as if it were brand new.

`x-replay-id` and `x-source-timestamp` are always stripped — they only have meaning inside the replay queue.

### `DELETE /api/replay/:vhost/:name/:id`

Purges the single quarantined message.

## UI

Top-level `Replay` page under `Messaging` lists every replay queue in the active vhost with row-click drilldown. The drilldown shows one row per quarantined message with Release / Delete buttons. The short id link opens the per-message detail page (`/replay-message`) with read-only origin metadata, an editable body textarea (gated by content type with a Force checkbox), a JSON user-header editor, and three separate cards for Save (PATCH), Release (with reset-replay toggle), and Delete. The Release card shows the destination at a glance.

## Metrics

Per-queue counters:

* `replay_released` — bumped by the release endpoint.
* `replay_edited` — bumped by the PATCH endpoint.

Both flow through the standard `message_stats` surface, so the queue JSON, per-second rate calculation, and the queue's Rates chart all pick them up. Prometheus exposes them under `queue_coarse_metrics`:

* `detailed_queue_replay_released_total`
* `detailed_queue_replay_edited_total`

Counters are emitted only for `x-queue-type: replay` queues.

## Use cases

* **Fix-and-resend.** A producer bug sends a malformed message; the consumer crashes. Move the message to a replay queue (manually, via DLX, or via the planned `x-message-filter` `move_to` action), edit the body to repair the payload, release.
* **DLX inspection.** Bind a replay queue as a destination of an existing DLX flow and get the inspect/edit/release UI for dead-lettered messages with no changes to producers or consumers.
* **Audit inbox.** Pair with a future `duplicate_to` filter action to keep an inspectable archive of every message matching a predicate.
* **Tenant misrouting.** Capture wrong-tenant messages in a replay queue, repair the tenant header, release.

## Limitations

* **No consumers over AMQP.** Replay queues are operator-facing only. Use the HTTP API to consume.
* **No bindings.** Routing messages to a replay queue happens via DLX or direct broker publish.
* **Single-rule edit.** No batch operations on multiple replay messages (release-all, delete-all, etc.).
* **No backfill.** Existing messages on a queue that is later converted to replay are not stamped; you cannot retro-fit `x-source-*` on already-stored messages.
* **No per-message TTL on replay messages.** Audit inboxes need manual purging today.
