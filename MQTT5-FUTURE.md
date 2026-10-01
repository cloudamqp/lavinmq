# MQTT 5.0 follow-up work

Deferred past this PR, opened as their own issues once it lands. Nothing here
blocks the PR: the branch ships v5 on top of QoS 2 (#2236), with every deferral
advertised and rejected.

---

## QoS 2 follow-ups

QoS 2 itself comes from #2236, which this branch is built on. What it leaves:

- **The QoS 2 state is in memory.** Inbound packet ids awaiting PUBREL and
  outbound deliveries awaiting PUBCOMP survive a reconnect but neither a broker
  restart nor a failover. #2236 plans persisting them as its own follow-up.
- **Cross-protocol semantics.** AMQP 0-9-1 has no equivalent handshake, so
  exactly-once can only ever be a promise between two MQTT endpoints. A
  documentation decision, not code.
- **A session can expire between CONNECT and attach.** `Broker#add_client`
  keeps a resumed session, but the client attaches only in `Client#run`, after
  the CONNACK. An expiry timer firing in that window deletes the session, and
  `run_client`'s `deleted?` check then closes the client it just told
  `session_present=1`. The same path as an operator deleting the queue, and
  narrow, but a resuming client can be disconnected for it.

### Landmines worth not rediscovering

- **`[MQTT-4.6.0-1]` is a client-side rule** in both versions ("A Client MUST
  follow these rules"). The server inherits resend ordering via `[MQTT-4.6.0-5]`
  + `[MQTT-4.6.0-6]` in 3.1.1. **5.0 relaxed it**: that incorporation is gone,
  and a non-normative comment explicitly permits `1,2,3,2,3,4` after a
  reconnect.
- **Spec helpers must live in `MqttHelpers`, or a module extended into
  `MqttSpecs`** - not a `def self.` beside a `describe` (`can't declare def
  dynamically`). `with_client_io`'s `with MqttHelpers yield` *prefers*
  `MqttHelpers` but falls back to the lexical `self`, so both resolve; verified
  empirically.
- **`Publish#payload` is owned by the packet** (`read_bytes` does
  `Bytes.new(len)`), so specs can return packets out of a closed socket's block.
- **A client that publishes QoS 1 to a topic it subscribes to may see its own
  delivery before the PUBACK**, because #2296 sends the PUBACK only once the
  publish is persisted. Use `read_delivery_and_puback`, not two typed reads.

## Pre-existing bugs, found in passing

Both that were listed here, `next_id` handing out packet id 0 and `Client#close`
deadlocking when `read_loop` never started, are fixed in #2236.

## Others

- Topic aliases, shared subscriptions, subscription identifiers, enhanced
  authentication. Each advertised as unavailable and rejected today, each a
  standalone feature afterwards.
- `$`-prefixed topics matching wildcard filters: a MUST NOT [MQTT-4.7.2-1], so
  it moved to item O in `MQTT5-TODO.md`.

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
