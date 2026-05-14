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
