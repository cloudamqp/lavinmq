# LavinMQ PoC Summary

Roadmap item: <https://github.com/orgs/84codes/projects/44/views/1?pane=issue&itemId=160360059&issue=84codes%7Croadmap%7C126>

## 1. `serverdev-poc/feature/live-filter-queues`

**Feature:** Runtime predicate on a queue's publish path. A single HTTP call (or policy) installs a rule that, per message, drops it, moves it to another queue, or duplicates it to another queue. Rule is authored as `clauses` over headers (`eq` / `not_eq` / `exists`) combined with `x-match: all|any`.

**Problem:** Today, sampling traffic, quarantining a misrouted tenant, or capturing a debug subset requires producer/consumer changes or a queue redeclare. This makes those operator-driven concerns a runtime knob instead.

## 2. `serverdev-poc/feature/replay-queue-type`

**Feature:** A new queue type (`x-queue-type: replay`) intended for operator-facing review. Messages can only arrive via broker-internal paths (DLX, future `move_to`, manual publish), get stamped with `x-replay-id` and `x-source-*` origin metadata, and are inspected, edited (`PATCH`), or released back to their origin (`POST .../release`) through a dedicated `/api/replay` surface and UI.

**Problem:** Fix-and-resend after a bug, auditing captured samples, and inspecting dead-lettered messages today need ad-hoc consumers or shovels. This gives operators a first-class place to land, look at, and replay messages.

## 3. `serverdev-poc/feature/poison-pill`

**Feature:** Per-queue arguments/policies (`x-quarantine-after-redeliveries`, `x-nack-to-quarantine`, `x-quarantine-target`, `x-quarantine-action` of `move`/`drop`/`tee`) that divert messages out of a queue when redelivery counts blow past a threshold or when a consumer rejects them. Diverted messages get `x-source-*` headers so they slot directly into a replay queue.

**Problem:** Today the only built-in answer to a poison message is `x-delivery-limit` + DLX, which is coarse. This adds explicit quarantine routing with a target queue and richer trigger options, and reuses the cross-feature `x-source-*` schema so the message store is consistent with replay/filter.

## 4. `peek-queue-messages`

**Feature:** `POST /api/queues/:vhost/:name/peek` that reads messages straight off disk under the message-store lock without consuming them via AMQP. Supports `offset`/`count` windowing, an `include_unacked` flag that folds in in-flight messages, and a "Peek messages" UI card on the queue page. Stream queues are rejected (they already have `/stream`).

**Problem:** "Get message" in the UI is destructive (it acks/requeues). Operators want to look at queue contents — including in-flight unacked messages — without disturbing them.

## 5. `scheduled-jobs`

**Feature:** Per-vhost named jobs that publish a configured message body to an exchange on a cron schedule. Stored as parameters (`component_name = "scheduled-job"`) so CRUD + HA replication come for free. One fiber per job, manual-trigger channel, pause/resume via API, paused-file marker for restart durability (same pattern as shovels). Includes an in-tree 5-field UTC cron parser (no shard) and a 1s debounced stats flusher.

**Problem:** There is no built-in way to fire a recurring message from the broker itself — users today run external cron + amqp clients or delayed-message hacks. This makes it a first-class broker feature.

## 6. `serverdev-poc/feature/processed-log` (Message processing metadata)

**Feature:** Broker-side recording of per-ack metadata to an append-only log per queue, with HTTP read endpoints for raw rows and per-queue summary stats (counts, p50/p95/p99 latency, redelivery histogram, avg payload). Always-on, every queue. Each record carries: ack timestamp, latency (ack ts − publish ts), payload size, redelivery count, exchange, routing key, consumer tag.

**Problem:** Visibility into "how many messages did queue X process yesterday, what was the p95 latency, how often did they need a retry?" lived outside the broker — every consumer had to ship its own metrics, or operators stitched numbers from Prometheus rate counters. This puts the answer next to the message store, no extra pipeline required.

**Shape of v1:**
- Per-queue `processed.{seg:010d}` files alongside `msgs.*` / `acks.*`.
- Time-based retention (default 24h, broker-config setting).
- Leader-only — not part of replication. After failover, new leader starts fresh.
- System metadata only — no header capture yet.
- Read: `GET /api/queues/:vhost/:name/processed` (paged rows) and `.../processed/summary` (aggregates).
- New "Processed messages" card on the queue page.

**Deferred:** header allowlist for filter-by-header, Prometheus exposition of summary aggregates, replication, nack/reject as separate outcome rows.
