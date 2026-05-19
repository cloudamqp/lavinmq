# Poison-Pill Quarantine

Divert messages out of a queue when they hit a redelivery threshold or when a consumer rejects them outright. Diverted messages land on a separate, operator-managed target queue carrying `x-source-*` headers so they can be inspected, edited, and replayed.

## Configuration

Set on the source queue via arguments or policies:

| Argument / Policy | Type | Description |
|-------------------|------|-------------|
| `x-quarantine-after-redeliveries` / `quarantine-after-redeliveries` | Int | Max redeliveries before the message is diverted |
| `x-nack-to-quarantine` / `nack-to-quarantine` | Bool | Divert on `basic.nack` / `basic.reject` with `requeue=false` |
| `x-quarantine-target` / `quarantine-target` | String | Destination queue for diverted messages |
| `x-quarantine-action` / `quarantine-action` | String | One of `move` (default), `drop`, `tee` |

`x-quarantine-after-redeliveries` reuses the same per-message counter as `x-delivery-limit`; the two can be set on the same queue and evaluated together.

## Triggers

| Trigger | Fires when |
|---------|------------|
| Poison-pill | Redelivery count exceeds `x-quarantine-after-redeliveries` on the next `reject`/`nack` with `requeue=true` |
| Nack-to-quarantine | `basic.nack` or `basic.reject` with `requeue=false` arrives on a queue with `x-nack-to-quarantine: true` |

Both triggers share the same target queue and action.

## Actions

`x-quarantine-action` selects what the broker does when a trigger fires:

| Action | Behavior |
|--------|----------|
| `move` (default) | Stamp `x-source-*` headers on the message, publish it to `x-quarantine-target`, and delete the original from the source. If no target is configured, falls back to normal DLX disposition. |
| `drop` | Silently delete the message. No DLX routing, no target publish. |
| `tee` | Publish a stamped copy to `x-quarantine-target`, then let the normal disposition run (DLX or drop). The original message still leaves the source queue. |

If `x-quarantine-target` is unset and the action is `move`, the broker uses the queue's existing DLX configuration as a fallback, preserving backwards compatibility with `x-delivery-limit`-style setups.

If the configured target queue does not exist (or equals the source itself), `move` and `tee` both fall back to the normal disposition — the broker logs a warning and routes via DLX if configured.

## Diverted Message Headers

Diverted and tee'd messages receive these headers (the originals are preserved):

| Header | Value |
|--------|-------|
| `x-source-queue` | Source queue name |
| `x-source-exchange` | Exchange the message was originally published to |
| `x-source-routing-key` | Original routing key |
| `x-source-timestamp` | Diversion time (unix ms) |
| `x-delivery-count` | Number of deliveries before diversion (poison-pill trigger only) |

The header schema matches the cross-feature `x-source-*` set, so a diverted message can land directly on an `x-queue-type: replay` queue for inspection or fix-and-resend.

## Interaction with DLX

- `move` with target: bypasses DLX entirely.
- `move` without target: falls back to DLX (reason `delivery_limit` for poison-pill, `rejected` for nack).
- `drop`: bypasses DLX.
- `tee`: copies to target *and* dead-letters the original through DLX (or drops it if no DLX is configured).

`x-delivery-limit` and `x-quarantine-after-redeliveries` can coexist. If both are set, the lower of the two thresholds fires first; once `x-delivery-limit` is exceeded the message is dead-lettered through the normal DLX path regardless of any quarantine target.

## Metrics

Per-queue counters exposed via JSON, Prometheus, and the Rates chart:

| Counter | Description |
|---------|-------------|
| `poison_quarantine` | Messages diverted by `x-quarantine-after-redeliveries` with action `move` (or `drop`) |
| `poison_tee` | Tee copies sent for poison-pill triggers |
| `nack_quarantine` | Messages diverted by `x-nack-to-quarantine` with action `move` (or `drop`) |
| `nack_tee` | Tee copies sent for nack triggers |

Prometheus metric names: `detailed_queue_poison_quarantine`, `detailed_queue_poison_tee`, `detailed_queue_nack_quarantine`, `detailed_queue_nack_tee`.

## Example

Divert payloads that repeatedly crash a consumer to a `repairs` queue after three failed deliveries:

```
ch.queue("orders", durable: true, args: {
  "x-quarantine-after-redeliveries" => 3,
  "x-quarantine-target"             => "repairs",
})
```

Send everything a consumer explicitly rejects to the same repair queue, but keep a copy in DLX for audit:

```
ch.queue("orders", durable: true, args: {
  "x-nack-to-quarantine"      => true,
  "x-quarantine-target"       => "repairs",
  "x-quarantine-action"       => "tee",
  "x-dead-letter-exchange"    => "",
  "x-dead-letter-routing-key" => "orders.audit",
})
```

Apply the same behaviour broker-wide via a policy (paired with an `x-queue-type: replay` target for fix-and-resend):

```
PUT /api/policies/%2F/pp-global
{
  "pattern": "^(?!repairs$|amq\\.|federation\\.).*",
  "apply-to": "queues",
  "priority": 10,
  "definition": {
    "quarantine-after-redeliveries": 5,
    "quarantine-target": "repairs",
    "quarantine-action": "move"
  }
}
```

## Use cases

* **Crashy consumer.** A bug in a consumer keeps crashing on the same payload. Set `x-quarantine-after-redeliveries: 3` on the queue once; the consumer recovers forever; the message lands on the operator-facing target (typically a replay queue) for review.
* **Producer-side validation failures.** A producer occasionally sends payloads that fail business validation. Consumers nack with `requeue=false`; `x-nack-to-quarantine: true` diverts those to a fix-and-resend queue instead of `/dev/null`.
* **Replacing a bespoke "drop-list" pattern.** Many teams maintain an app-side queue of correlation-ids to drop plus a consumer that scans that queue before requeueing. With poison-pill the broker handles the divert (per redelivery threshold or per nack-with-requeue=false) and stamps origin metadata, so the consumer no longer needs to know about the drop list.
* **Audit trail (`tee`).** Need a parallel copy of every quarantined message for compliance / audit without disturbing the existing DLX flow.
* **Silent drop (`drop`).** "I just don't want to see this message again, and I don't care to inspect it" — bypass DLX entirely.
* **Migrating off RabbitMQ DLX patterns.** Existing DLX setup keeps working; add `x-quarantine-target` to opt into the new flow when ready.

## Comparison to existing LavinMQ features

| Need | Existing | Poison-pill |
|------|----------|--------------|
| Dead-letter on reject / TTL / overflow | DLX (`x-dead-letter-exchange` / `x-dead-letter-routing-key`) | Same triggers continue to work. Adds two new triggers (redelivery threshold, nack-to-quarantine) plus a configurable `move` / `drop` / `tee` action. |
| Cap on retries before dead-lettering | `x-delivery-limit` | `x-quarantine-after-redeliveries` shares the same `@deliveries` counter. Difference: delivery-limit always dead-letters via DLX; poison-pill routes to an explicit `x-quarantine-target` and stamps `x-source-*` so the message is replayable. Fall-back to DLX when no target is set keeps backwards-compatibility. |
| Stamp origin metadata | `x-death` array + `x-first-death-*` | Adds the `x-source-*` schema shared with the live filter and replay queue type. Replay queues normalise `x-first-death-*` → `x-source-*` automatically on intake, so an existing DLX flow gains replay/edit semantics by simply pointing the DLX at a replay queue. |
| Tee a copy somewhere for audit | None today (operators build with `duplicate_to` exchanges or alternate-exchange) | First-class `tee` action; counters distinguish full diversions from tee'd copies. |

Poison-pill is intentionally "DLX with two more triggers, a configurable disposition, and `x-source-*` stamping" — same mental model, incremented. It is not a generic publish-time predicate; the trigger event itself is the match.

## Punch list (deferred for follow-up work)

* **TTL-expiry diversion.** Route to `x-quarantine-target` when `x-message-ttl` fires, instead of DLX.
* **Overflow diversion.** Route to `x-quarantine-target` on `x-max-length` drop, instead of DLX.
* **Channel-disconnect diversion.** Catch in-flight messages from a crashed consumer's channel and divert before the broker requeues them.
* **Optional separate target per trigger.** `x-poison-target` vs `x-nack-target` for use cases where the two triggers should land on different queues.
* **Optional separate action per trigger.** Split `x-quarantine-action` into `x-poison-action` / `x-nack-action` when a use case appears.
* **Aggregated counter `total_diverted`** across triggers, for dashboards that don't want to sum four series.
* **Cross-restart counter persistence.** `@deliveries` is in-memory today; a restart resets the threshold to zero. Acceptable trade-off for now, matching `x-delivery-limit` semantics, but operators sometimes want stable behaviour across rolling restarts.
* **`x-delivery-count` header awareness.** When other features (e.g. retry-with-backoff) stamp `x-delivery-count` across DLX-roundtrips, honour it in the threshold check so the count survives republishes.
