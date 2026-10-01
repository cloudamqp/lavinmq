# MQTT 5.0 follow-up work

Deferred past this PR, opened as their own issues once it lands. Nothing here
blocks the PR: the branch ships v5 with `maximum_qos = 1` and every deferral
advertised and rejected.

---

## QoS 2 (exactly-once delivery)

> **Revised 2026-09-08.** An earlier version of this section called QoS 2 a
> "session-state and durability project" and listed four blockers. Three of them
> do not hold; they are kept below under *Blockers that turned out not to be*,
> because the reasoning is worth not repeating. The precursor work is done and
> in review, so what is left is smaller than this section used to claim.

QoS 2 replaces the single PUBACK with a two-phase handshake:
PUBLISH -> PUBREC -> PUBREL -> PUBCOMP. The exactly-once guarantee comes from the
receiver remembering **packet ids**, not messages: between PUBREC and PUBREL it
holds id N, so a re-sent PUBLISH with id N is answered with another PUBREC and
not delivered a second time.

### Precursor: done, in review

`fix/mqtt-resend-original-packet-ids` (PR #2233, draft, off `main` - *not* this
branch) makes a resuming session resend its in-flight window with the
**original packet ids** and DUP set, instead of requeueing it and handing out
fresh ids. QoS 2's PUBREL step is meaningless without that: a subscriber dedupes
on the id it was given.

It also fixed `next_id`'s bound (`==` -> `>=` against `max_inflight_messages`)
and made `get_packet` close the capacity gate when `next_id` comes back empty,
without which the deliver_loop spins on a non-empty store.

Rebase this branch on `main` once it lands. Expect small conflicts in
`consts.cr`, `session.cr` and `client.cr` - both branches touch all three.

One regression it introduced, deliberately unfixed: an abandoned non-clean
session now pins up to `max_inflight_messages` messages in `@unacked`, and
`drop_overflow` does not reclaim them, so `max-length` no longer bounds them.
`AMQP::Queue` exempts unacked messages from `max-length` too, so it is
consistent rather than novel. Wants its own issue.

### Blockers that turned out not to be

1. ~~Inbound state with the message held *undelivered* until PUBREL.~~ Not
   required. The spec's own flow (Figure 4.3, both versions) has the receiver
   *store the packet id, then initiate onward delivery immediately*, before
   PUBREC. `[MQTT-4.3.3-10]` only requires that a re-sent PUBLISH with the same
   id gets another PUBREC and is not delivered twice. So inbound dedupe is a
   `Set(UInt16)`, not a message buffer.
2. ~~`@unacked` models one ack step, and the second must outlive the body.~~ It
   does outlive it, but cheaply: on PUBREC, `delete_message(sp)` exactly as an
   ack does today, and move the id to a pubcomp-pending set that `next_id` also
   skips. `@unacked` keeps its current shape.
3. ~~A different QoS carrier is needed.~~ `properties.delivery_mode` is a
   `UInt8` and nothing outside MQTT reads it (`grep` finds one write, in
   `lavinmqperf`). `delivery_mode = 2` works, on disk too.
4. **Still real: a decision on cross-protocol semantics.** AMQP 0-9-1 has no
   equivalent handshake, so exactly-once could only ever be a promise between
   two MQTT endpoints. A documentation decision, not code.

No shard work either: `PubRec` / `PubRel` / `PubComp` are in the **released**
shard `v0.3.1` with specs, so unlike this branch a QoS 2 PR needs no shard tag.

### Sketch

| File | Change |
| --- | --- |
| `consts.cr` | `MAX_QOS` 1 -> 2; add `QOS2_ARGUMENTS`; `granted_qos` / `subscription_options` clamps follow |
| `connection_factory.cr` | drop the `maximum_qos` CONNACK property and the Will-QoS-2 `0x9B` rejection |
| `session.cr` | `@incoming_qos2 : Set(UInt16)` (inbound dedupe, `[MQTT-4.3.3-10]`); `@waiting_pubcomp : Set(UInt16)`; `next_id` skips both; PUBREL replay joins `resend_unacked` |
| `client.cr` | dispatch `PubRec`/`PubRel`/`PubComp`; QoS 2 branch in `recieve_publish`; drop the `qos > MAX_QOS` protocol violation |
| `exchange.cr` | nothing - `delivery_mode = packet.qos` already passes 2 through |

Specs asserting the downgrade, which will need flipping: `consts_spec.cr`,
`integrations/subscribe_spec.cr`, `integrations/message_qos_spec.cr`,
`v5/publish_spec.cr`, `integrations/will_spec.cr`.

Retransmission stays as it is - no timers. `[MQTT-4.4.0-1]` is the *only*
circumstance where a resend is required, and it explicitly forbids resending at
any other time.

### Open design question

`@unacked` is a bare `Hash(UInt16, SegmentPosition)` whose MQTT invariants live
only in comments, and the `max_inflight_messages` bound is now checked in three
places. QoS 2 would otherwise add two more loose `Set(UInt16)`s to `Session`. An
`InflightWindow` type owning the map, `next_id`, the capacity signal and the
resend iteration would put the bound in one place and make "ids are stable
across a reconnect" a property of the type. Agreed to revisit when starting this
work rather than deciding up front.

### Not urgent, but

The current position is fully spec-legal, and the external run confirmed real
clients handle it gracefully: mosquitto refused a QoS 2 publish client-side off
our advertised `maximum_qos = 1` without putting it on the wire. The usual
answer for users who ask is an idempotent consumer on QoS 1, which is cheaper
than four round-trips per message.

The pull the other way: 22 of the Paho v5 suite's 27 tests use QoS 2 somewhere
(see `MQTT5-INTEROP.md`), so that suite cannot grade this broker until QoS 2
exists. That, not user demand, is the argument for doing it next.

### Landmines worth not rediscovering

- **Requirement IDs are numbered differently in 3.1.1 and 5.0**, and this repo
  cites **3.1.1** throughout. Proof: `keepalive_spec.cr` cites
  `[MQTT-3.1.2-24]` for the 1.5x keepalive grace period, which is that ID in
  3.1.1 but "MUST NOT send packets exceeding Maximum Packet Size" in 5.0. So
  the local `MQTT-v5.0-spec.txt` *cannot* validate them - check the OASIS 3.1.1
  HTML instead. It is Word-generated windows-1252: `iconv` it and strip tags
  first, or the IDs will not match at all.
- **`[MQTT-4.6.0-1]` is a client-side rule** in both versions ("A Client MUST
  follow these rules"). The server inherits resend ordering via `[MQTT-4.6.0-5]`
  + `[MQTT-4.6.0-6]` in 3.1.1. **5.0 relaxed it**: that incorporation is gone,
  and a non-normative comment explicitly permits `1,2,3,2,3,4` after a
  reconnect. `message_qos_spec.cr` still miscites `-1` for a server-side test.
- **`Client#close` joins the read loop** (`@waitgroup.wait`, with
  `@waitgroup.done` in `read_loop`'s `ensure`), so no `Session#ack` can run
  during a reconnect's resend. **`Session#deliver_loop` is not in that
  waitgroup**, so it *can* sit parked mid-send for a displaced connection and
  resume straight into `@unacked[id] = sp` - which is why `resend_unacked`
  iterates a snapshot.
- **`MessageStore#[]` returns a view into the mmap**; `#copy` is the one that
  survives dropping the lock. `resend_unacked` gets away with `[]` only because
  an unacked message's segment cannot be unmapped: `delete` drops a segment only
  once every message in it is acked, and `Session#purge` calls `purge`, not
  `purge_all`.
- **Spec helpers must live in `MqttHelpers`, or a module extended into
  `MqttSpecs`** - not a `def self.` beside a `describe` (`can't declare def
  dynamically`). `with_client_io`'s `with MqttHelpers yield` *prefers*
  `MqttHelpers` but falls back to the lexical `self`, so both resolve; verified
  empirically.
- **`Publish#payload` is owned by the packet** (`read_bytes` does
  `Bytes.new(len)`), so specs can return packets out of a closed socket's block.

## Pre-existing bugs, found in passing

Neither is caused by the QoS 1 packet-id work (PR #2233) - both predate it and
both live in code that PR touches, so they were noticed rather than introduced.
Left alone there to keep that diff single-purpose. Each wants its own issue.

### `next_id` can hand out packet id 0

`Session#next_id` (`src/lavinmq/mqtt/session.cr`):

```crystal
start_id = @count
next_id : UInt16 = start_id &+ 1_u16
while @unacked.has_key?(next_id)
  next_id &+= 1u16
  next_id = 1u16 if next_id == 0   # only corrects inside the collision loop
  return if next_id == start_id
end
```

At `@count == 65535` the initial `&+ 1` wraps to 0. If id 0 is not in `@unacked`
- which it never is - the loop body never runs, so the `next_id == 0`
correction is skipped and 0 is returned. Packet id 0 is illegal
(`[MQTT-2.3.1-1]`: "MUST assign it a non-zero Packet Identifier").

One-line fix (hoist the zero check above the loop). Reaching it needs 65535
QoS>0 deliveries on one session, so a spec for it is either slow or has to poke
`@count` directly.

### `Client#close` deadlocks if `read_loop` never started

`Client#close` ends in `@waitgroup.wait`, and `@waitgroup.done` only fires in
`read_loop`'s `ensure`. Anything that closes a client between construction and
`read_loop` blocks the closing fiber forever.

Two reachable-ish routes: a connection displaced by a same-client_id CONNECT
before its read loop starts, and `Session#deliver_loop`'s rescue calling
`@client.try &.close` in the window between `Session#client=` and `Client#run`.
PR #2233 narrows the second (it moved `client=` into `run_client`, next to
`client.run`) and relies on `resend_unacked` swallowing to avoid the first, but
neither is a fix. The fix is for `close` not to wait on a waitgroup that may
never be decremented - e.g. decrement it on the paths that bypass `read_loop`,
or make the wait bounded.

## Others

- Topic aliases, shared subscriptions, subscription identifiers, enhanced
  authentication. Each advertised as unavailable and rejected today, each a
  standalone feature afterwards.
- `$`-prefixed topics matching wildcard filters (spec 4.7.2, a SHOULD NOT).
  Pre-existing on v3 as well; the Paho suite flags it. Cheap to fix, but it is a
  behaviour change for existing v3 users, so it wants its own decision.

---

## Parked: PUBLISH topic as Bytes

Kept because the conclusion is non-obvious and someone will suggest it again.

A colleague proposed reading the PUBLISH topic as `Bytes` instead of `String`.
Prototyped off shard `main` (commit `e16e186`, never pushed). An isolated
microbenchmark showed 1.8x-3.0x faster topic decode.

**It does not help LavinMQ.** The topic must become an AMQP `String`
routing_key anyway: `routing_key : String` is load-bearing across the shared
AMQP core (message store on-disk format, bindings, dead-lettering, management
API). LavinMQ would call `String.new(packet.topic)` regardless, making the change
net-negative: a Bytes alloc in the shard *plus* a String alloc in LavinMQ, saving
only the UTF-8 validation we arguably want.

**The real win was elsewhere and already landed.** The subscription-tree match
allocated one throwaway `String` per topic level on every publish. Byte slicing
the tokenizer makes that zero-allocation, needs no shard change, and is on `main`
via `bytes_token_iterator.cr` (#1920). `topic_tree.cr` (retain store) still uses
`StringTokenIterator`, but it is off the publish hot path.

`feat/mqtt5` has the Bytes-topic change baked into its `publish.cr` already, so
the two branches conflict. Park the breaking version until MQTT is decoupled
from the AMQP core and `routing_key` no longer has to be a String.

**Dead end, do not repeat:** a `Char::Reader` single-pass UTF-8 validation in
`read_string` benchmarked **5-17x slower** than the stdlib two-scan
(`includes?('\0') || !valid_encoding?`). It was reverted.
