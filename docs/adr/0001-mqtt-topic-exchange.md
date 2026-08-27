# 1. Routing MQTT messages to AMQP queues via an `x-mqtt-topic` exchange

Date: 2026-08-21

## Status

Accepted. Implemented 2026-08-27, with the amendment to collateral fix 1 noted
below.

## Context

Messages published by an MQTT producer can currently only be delivered to MQTT
sessions. There is no way for an AMQP consumer to receive them.

We want an AMQP-declarable exchange that an AMQP client can bind queues to using
**MQTT topic filter syntax**, so that an MQTT publish to `a/b/c` is routed to any
queue bound with `a/b/#`, `a/+/c`, and so on. A vhost must support any number of
such exchanges, each with any number of bound queues.

Constraints:

- **No allocations on the routing path.** MQTT publish is a hot path.
- Message publishing must not be transformed: the AMQP routing key is the MQTT
  topic verbatim, slashes included.
- The design must not depend on `MQTT::Exchange` (`mqtt.default`) surviving —
  it is expected to be removed — but may share machinery with it in the interim.

Relevant existing structure:

- `MQTT::Exchange` (type `mqtt`, name `mqtt.default`) owns a
  `SubscriptionTree(MQTT::Session)` and is created per vhost by `MQTT::Broker`.
- `MQTT::Broker` is owned by `MQTT::Server` and keyed by vhost name. **There is
  no `VHost` -> `Broker` path**, and exchanges are constructed by
  `DefinitionsStore#make_exchange(@vhost, ...)` with only the vhost in scope.
- `LavinMQ::Server` creates `VHostStore` (and thus replays every vhost's
  definitions) *before* `Launcher` creates `MQTT::Server`. At definitions-replay
  time no `Broker` and no `mqtt.default` exist.

## Decision

### Routing shape

One `SubscriptionTree` per vhost, **owned by `VHost`**, so it exists before
definitions replay. Tree entries are `MQTT::Subscriber`s: `MQTT::Session`
(unchanged behaviour) and `MqttTopicExchange` instances.

The **exchange itself is the tree entry**, registered once per distinct binding
key, not once per bound queue. `SubscriptionTree#each_entry` already yields the
matched filter alongside the subscriber, so `deliver(msg, filter)` looks the
filter up in the exchange's own binding book.

`Broker#publish` is **unchanged**. The single tree walk inside
`MQTT::Exchange#publish` now reaches sessions and `x-mqtt-topic` exchanges alike;
its loop variable `_filter` becomes used.

Rationale for exchange-as-entry rather than a per-queue subscription object: the
exchange needs its own binding book regardless (for `bindings_details`,
`binding_count`, the HTTP bindings API, and `DefinitionsStore#compact!`), and a
shared tree cannot attribute an entry to an exchange. Making the queue the entry
would mean maintaining two structures encoding the same thing. As a bonus the
tree holds *filters*, not filters x queues: 100 queues on `a/#` is one entry.

Because an exchange is a single object, the tree's `compare_by_identity` inner
hashes are still correct and are kept. `MQTT::Subscriber` must therefore be
reference-only.

### The exchange

`LavinMQ::AMQP::MqttTopicExchange`, in
`src/lavinmq/amqp/exchange/mqtt_topic.cr`, registered in
`DefinitionsStore#make_exchange`.

| Aspect | Decision |
|---|---|
| Type string | `x-mqtt-topic`. `x-` per the convention used by `x-consistent-hash` / `x-delayed-message`; `mqtt` names both the source protocol and the binding-key syntax; `topic` says pattern-matched. `mqtt` alone is already taken by `MQTT::Exchange#type`. |
| Default instance | None. Users declare their own; `mqtt.`/`amq.` stay reserved prefixes. Adding a default later is backwards-compatible, removing one is not. |
| `internal` | Forced `true` in the constructor, with `match?` overridden to ignore the flag so a redeclare with `internal: false` is still idempotent rather than PRECONDITION_FAILED. |
| Binding book | `Hash(String, Set({AMQP::Destination, BindingKey}))`; sole source of truth for `bindings_details`, `binding_count`, and auto-delete. |
| Tree writes | `subscribe(filter, self, 1u8)` on the first binding for a filter; `unsubscribe(filter, self)` on the last. |
| `deliver(msg, filter)` | Builds a **fresh** `Message` (a struct — no heap allocation): `exchange_name` = own name, routing key = MQTT topic verbatim, `delivery_mode = 2`, `headers = nil`. Publishes to each destination in the book, rewinding `body_io` between them: a queue destination directly, an exchange destination through `route_msg`. |
| Stats | `deliver` counts `publish_in` (once per *matching filter*, so an overlapping-filter publish counts more than once) and `publish_out` (per destination that accepted), so the exchange isn't blank in the management UI. |
| `each_destination` | No-op. |
| Arguments and policies | `handle_arguments`, `apply_policy_argument`, `clear_policy_arguments` are no-ops, as in `MQTT::Exchange`. |
| `delete` | **Must** unsubscribe every filter from the vhost tree before/while calling `super`. |
| Auto-delete | Tests the binding book, **never** `@tree.empty?`. |

A fresh `Message` rather than the shared one, because the shared message is built
with `exchange_name = "mqtt.default"` and its `properties.delivery_mode` is
rewritten in place by the tree walk for each yielded entry (`Message` and
`Properties` are both structs, and Crystal does mutate through a struct getter on
a local). `delivery_mode = 2` unconditionally: LavinMQ derives persistence from
queue durability and never reads this field, so it is metadata only, and widening
the `Subscriber` contract to carry the publisher's QoS is not worth it.

### Binding keys are not validated

Binding keys are passed to `SubscriptionTree#subscribe` as-is. Invalid MQTT
filters fail silently; see *Known footguns*.

### Overlapping filters deliver N copies

A queue bound to one exchange with both `sensors/#` and `sensors/+/temp` receives
**two** copies of a publish to `sensors/a/temp`, because the tree yields the
exchange once per matching filter. An AMQP `TopicExchange` would deliver one — it
accumulates into a `Set(AMQP::Queue)` before publishing. We accept N copies:
semantics are "one message per matching binding", consistent with the MQTT
sessions sharing the tree, and deduplication would need either a per-publish
`Set` (allocates) or generation stamps. Pinned by spec so it is a decision rather
than an accident.

### No retained-message replay on bind

A newly bound queue receives only messages published after the bind. An MQTT
client subscribing to the same filter gets the full retained state immediately;
this asymmetry is accepted.

`RetainStore` is owned by `Broker`, so it does not exist at definitions-replay
time, and `Exchange#bind` cannot distinguish a live bind from a replay — bindings
are re-applied on *every* boot, so a naive implementation would re-dump the
retain store into the queue on each restart.

### AMQP publishes into the exchange are refused

`internal = true` makes `basic.publish` (and the HTTP publish endpoint) return
ACCESS_REFUSED. Binding a *queue* to an internal source is unaffected — the
`internal?` checks in `http/controller/bindings.cr` are on the destination.

`Exchange.Bind` has no `internal?` guard, so an AMQP client can bind an internal
exchange as a *destination* of e.g. `amq.topic` and reach it through
`find_queues`. Adding one was attempted and reverted — it breaks exchange
federation; see *Collateral fixes*. For this exchange the back door is inert
regardless: `each_destination` is a no-op, so nothing routes through it.

## Consequences

### Changes to existing code

- `VHost` gains the `SubscriptionTree` and exposes it.
- `MQTT::Exchange` drops its own `@tree` for the vhost's, and filters
  `bindings_details` and `binding_count` to `MQTT::Session` entries only —
  otherwise `mqtt.default` reports `x-mqtt-topic` bindings as its own and
  `SubscriptionDetails` gets a non-session destination.
- `SubscriptionTree#size` and `#empty?` now mean "everything in the vhost", not
  "MQTT subscriptions". They get a comment saying so.
- `MQTT::Exchange` keeps deriving its binding view from the tree. Giving it its
  own binding book (making the tree a pure matching index) is the cleaner end
  state and the direction the `mqtt.default` removal points, but it touches
  `Session#subscribe`/`#unsubscribe` and is deferred.

### Collateral fixes required

1. ~~`src/lavinmq/amqp/client.cr:896` (`bind_exchange`) — add an `internal?`
   guard on the destination, matching `http/controller/bindings.cr:140`.~~
   **Attempted and reverted (2026-08-27).** `Federation::ExchangeLink#setup`
   binds the internal `x-federation-upstream` exchange as the *destination* of
   the upstream exchange, over a real AMQP client connection — so the guard sits
   squarely on federation's own path, and 14 `spec/upstream_spec.cr` examples
   hang with it in place. RabbitMQ also refuses only `basic.publish` on an
   internal exchange, not `exchange.bind`, which makes
   `http/controller/bindings.cr:140` the outlier rather than `client.cr`.
   `spec/exchange_spec.cr` now pins that both bind directions stay allowed, so
   the next reader finds the reason instead of the hole.
2. `src/lavinmq/http/controller/exchanges.cr:68-70` — remove the
   `if e.internal?` -> 400 branch on redeclare. It is copy-pasted from the
   publish endpoint and makes `PUT /api/exchanges/...` non-idempotent for any
   internal exchange, which breaks definitions import and Terraform-style tooling.
3. `src/lavinmq/definitions.cr:338` (`export_exchanges`) — narrow
   `reject(&.internal?)` to
   `reject { |e| e.internal? && NameValidator.reserved_prefix?(e.name) }`.
   Rejecting on `internal?` alone omits `x-mqtt-topic` exchanges from export
   while `export_bindings` still emits their bindings, so an export/import
   round-trip yields bindings referencing a nonexistent exchange. Rejecting on
   reserved prefix alone would stop exporting `amq.*`, a live behaviour change.
   Of the exchanges that exist today the conjunction excludes only
   `mqtt.default`; it also starts *including* federation's internal
   `x-federation-upstream` exchanges, which are internal but not
   reserved-prefixed. Those re-import cleanly (`make_exchange` knows the type)
   and the link recreates them regardless, so this is accepted as noise.
   `mqtt.default`'s own bindings are still exported while the exchange is not —
   pre-existing, and out of scope here.

### Known footguns (accepted, to be documented)

- `a/#/b` silently becomes `a/#`: `SubscriptionTree#subscribe` registers in
  `@wildcard_rest` and returns, discarding the rest of the filter. This
  **over**-matches, which is the dangerous direction.
- `sport#` and `a+b` become literal level names that never match.
- `a.b.#` and `a.b.*` (AMQP muscle memory) are accepted as literal
  single-level filters and never match.
- `SUBSCRIBE` rejects filters that `bind` accepts: the protocol library
  validates topic filters in
  `MQTT::Protocol::Subscribe::TopicFilter#initialize`, the AMQP bind path does
  not. The same filter string behaves differently depending on which protocol
  created it.

### Pre-existing bugs found, out of scope

- `src/lavinmq/mqtt/exchange.cr:41` **overwrites** `delivery_mode` with the
  subscription's QoS instead of taking `min(publish qos, subscription qos)`.
  A QoS 0 publish to a QoS 1 subscriber is *upgraded*, violating MQTT 3.1.1
  §4.3. Only the downgrade direction is covered by specs. If fixed, this
  exchange gets publisher-QoS fidelity for free by reading
  `msg.properties.delivery_mode` (since its entries register at QoS 1 and MQTT
  caps at 1 here).
- MQTT topics may be up to 65535 bytes; AMQP routing keys are `ShortString`
  (255). A longer topic breaks on `msg_store.push`. Already true for MQTT
  sessions.

### Deferred

- Retained replay on bind, via an `after_bind` hook fired only when
  `DefinitionsStore#apply` is not `loading`, plus moving `RetainStore` to `VHost`.
- Per-exchange binding books for `MQTT::Exchange`.
- Routing AMQP publishes by MQTT filter (a real `each_destination` that walks the
  shared tree and selects own entries) — deliberately not done to avoid owning a
  second semantic, and because it cannot reach MQTT sessions anyway:
  `find_queues` collects into `Set(AMQP::Queue)`.
- Deduplicating overlapping filters via a per-publish generation counter threaded
  through `deliver`.

## Alternatives considered

- **A tree per exchange, with `Broker` fanning out over a registry.** Attributes
  stats and `exchange_name` per exchange and keeps the new exchange independent
  of `mqtt.default`, at the cost of one tree walk per exchange per publish and a
  registry that cannot live on `Broker` (no `VHost` -> `Broker` path) without a
  catch-up scan at broker creation. Rejected in favour of a single shared tree.
- **Tree entries typed `Session | AMQP::Queue`,** with attribution in a
  per-exchange set. Requires a refcount to stop an unbind from one exchange
  killing another's subscription, and keeps two structures in sync.
- **Per-`(exchange, queue)` subscription objects as tree entries.** Needs value
  equality (so `compare_by_identity` would have to go) and still leaves the
  exchange needing its own binding book.
- **The exchange keeping its own binding registry with a catch-up scan in
  `Broker#initialize`,** instead of moving the tree to `VHost`. Avoids touching
  `VHost` but defers the refactor that the `mqtt.default` removal needs anyway.
- **A no-op `each_destination` with `internal` left false,** so AMQP publishes
  are silently unroutable. Rejected: a non-mandatory publisher gets no signal.
- **Validating binding keys** as MQTT topic filters, rejecting with
  `PreconditionFailed`. Rejected for now; footguns documented instead.
- **Auto-creating a `mqtt.topic` instance per vhost** alongside `amq.topic`.
  Rejected: puts an internal, undeletable exchange into vhosts with no MQTT
  traffic.

## Test plan

- Bind `a/b/#`, MQTT publish `a/b/c`, assert the message lands in the queue with
  the exchange's own `exchange_name`, routing key `a/b/c`, `delivery_mode` 2.
- Non-matching topic is not routed.
- Two queues on the same filter each get a copy.
- Two exchanges with the same filter both route; **unbinding from one does not
  kill the other's subscription** (the entry-collapse regression).
- Overlapping filters on one queue deliver N copies (pins the decision above).
- Queue delete and exchange delete both clear tree entries; a publish after
  either routes nowhere.
- `auto_delete` exchange is deleted when its last binding is removed.
- Durable exchange plus durable queue: bindings survive a restart via definitions
  replay.
- `basic.publish` to the exchange returns ACCESS_REFUSED.
- `exchange.bind` with an internal exchange as destination, and as source, are
  both allowed (pins the reverted guard above).
- Definitions export includes the exchange and its bindings, and an
  export/import round-trip restores both.
- `mqtt.default`'s `binding_count` and `bindings_details` exclude
  `x-mqtt-topic` bindings.
- MQTT sessions still deliver correctly with `x-mqtt-topic` entries in the same
  tree, including will messages.
- Retained messages are *not* replayed on bind.
