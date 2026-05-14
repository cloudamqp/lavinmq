# Queue Live Filter

A queue live filter is a runtime predicate evaluated on every publish to a queue. When a message matches, the broker either drops it, moves it to a target queue, or duplicates it to a target queue. The rule is changed at runtime via a single HTTP endpoint (or a policy) — no queue redeclare, no producer or consumer changes.

The feature targets debugging, audit sampling, and tenant-misrouting cleanup, not steady-state routing. Every configured filter adds work to the publish path, so the rule should be a temporary or operator-managed concern rather than a high-throughput primitive.

## Configuration

The filter rule is supplied either through the runtime endpoint (which wraps it in an auto-managed policy) or through a regular policy. Queue arguments are not supported in this release — the rule is a *runtime* concept by design.

```http
PUT /api/queues/:vhost/:name/filter
Content-Type: application/json

{
  "x-match": "all",
  "action": "move_to",
  "target": "audit-pool",
  "rule_id": "free-tier-eu",
  "clauses": [
    {"key": "x-tier",   "op": "eq",     "value": "free"},
    {"key": "x-region", "op": "eq",     "value": "eu"},
    {"key": "x-debug",  "op": "exists"}
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `clauses` | yes | Non-empty array of clauses (key + op + optional value). |
| `x-match` | no (default `all`) | `all` (AND) or `any` (OR). |
| `action` | no (default `drop`) | `drop`, `move_to` or `duplicate_to`. |
| `target` | required for `move_to` and `duplicate_to` | Destination queue. Must exist; otherwise the original is dropped with a warning. |
| `rule_id` | no | Free-text identifier stamped on diverted messages for traceability. |

Clause ops:

| Op | Behaviour | `value` required |
|----|-----------|------------------|
| `eq` | Header value compared as string equals `value`. | yes |
| `not_eq` | Header value differs from `value`, or header absent. | yes |
| `exists` | Header key is present, value ignored. | no |

The predicate is skipped (never matches) when the message carries an `x-source-queue` header — see [Replay-loop prevention](#replay-loop-prevention).

### Setting via policy

The same rule can be authored as a regular policy, where `message-filter` is the definition key and the value is the rule JSON object:

```http
PUT /api/policies/:vhost/:name
Content-Type: application/json

{
  "pattern": "^orders$",
  "apply-to": "queues",
  "priority": 10,
  "definition": {
    "message-filter": {
      "clauses": [{"key": "country", "op": "eq", "value": "RU"}],
      "action": "move_to",
      "target": "orders.audit"
    }
  }
}
```

The runtime endpoint and the user-authored policy can both apply to the same queue; the priority field determines which one wins. The runtime endpoint always creates a policy with priority 100 named `__queue-filter__<queue>`.

## Actions

| Action | Effect | Counter |
|--------|--------|---------|
| `drop` | Drop the message silently before it is stored on the queue. | `filter_drop` |
| `move_to:<queue>` | Stamp origin headers, publish to the target queue, do not store on the source queue. | `filter_move` (only when the target queue exists) |
| `duplicate_to:<queue>` | Push the original message to the source queue AND publish a stamped copy to the target queue. | `filter_duplicate` (only when the target queue exists) |

Only `duplicate_to` keeps the original message on the source queue. `drop` and `move_to` consume the message.

## `x-source-*` stamp

When a message is moved or duplicated, the copy gains the following headers (existing user headers are preserved):

| Header | Description |
|--------|-------------|
| `x-source-queue` | The queue the message was originally going to. |
| `x-source-exchange` | The exchange the message arrived on. |
| `x-source-routing-key` | The routing key the message was published with. |
| `x-source-timestamp` | Server time (ms since epoch) when the message was diverted. |
| `x-source-rule-id` | Copy of the rule's `rule_id` if set. |

Downstream tooling — including the planned replay queue type — consumes these headers to identify a message's origin and republish it later.

## Replay-loop prevention

When a downstream "replay" or "audit" tool republishes a moved message back to its original source, the released message still carries `x-source-queue`. The filter predicate is short-circuited for any message carrying that header so the same rule cannot immediately re-match its own output. Tools that want the filter to re-evaluate a released message strip `x-source-queue` (or use the replay endpoint's `?reset_replay=true` option, when paired with the planned replay queue type).

## Listing the active filter

```http
GET /api/queues/:vhost/:name/filter
```

Returns `{ "source": ..., "rule": ... }` where `source` is one of `queue-arg`, `user-policy`, `managed-policy`, or `null` (no filter). The management UI uses this to display where the active rule lives.

## Removing a filter

```http
DELETE /api/queues/:vhost/:name/filter
```

Removes the auto-managed policy if present. If a user-authored policy also matched the queue, that policy continues to apply.

## Metrics

Per-queue counters are emitted in three places:

* `GET /api/queues/:vhost/:name` includes `filter_drop`, `filter_move`, and `filter_duplicate` totals under `message_stats`, with per-second rates.
* `message_stats` is consumed by the queue page's Rates chart automatically, so the chart legend gains three lines when a queue has activity.
* `GET /metrics/detailed?family=queue_coarse_metrics` exposes per-queue Prometheus counters:
  * `detailed_queue_filter_dropped_total`
  * `detailed_queue_filter_moved_total`
  * `detailed_queue_filter_duplicated_total`

## Permissions

Writes to `/api/queues/:vhost/:name/filter` (PUT and DELETE) require the `policymaker` tag. GET requires `management`.

## Performance

The filter check runs on the publish hot path. When no rule is configured the check is a single `nil?` comparison and adds no observable cost.

When a rule is configured every publish inspects the message's headers. High-throughput queues pay a real per-message cost; use the feature for diagnostic and operational scenarios rather than as a steady-state routing primitive.

`duplicate_to` is the most expensive action because the message body is read into memory before the copy is built. Avoid it on multi-MiB payloads.

## Reserved policy prefix

Auto-managed filter policies are named `__queue-filter__<queue>`. The default `/api/policies` listings hide them; pass `?include_managed=true` to see them. Operators should not create user-authored policies under this prefix.

## Limitations

* **Single rule per queue.** Compound behaviour requires multiple queues today.
* **Headers only.** The predicate cannot read message payload or AMQP properties other than headers.
* **No regex.** Clauses use exact equality (`eq`, `not_eq`) or presence (`exists`).
* **Queue scope only.** The predicate is attached per queue; exchange-level and vhost-level scopes are tracked as future work.
* **No backfill.** Applying a filter only affects future messages crossing the publish boundary; messages already in the queue are untouched.
