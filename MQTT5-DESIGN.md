# MQTT 5.0 design

Architecture and the decisions behind it, across `mqtt-protocol.cr` and LavinMQ.
This is the *why* file: invariants a future change can break without failing a
spec. Status lives in `MQTT5.md`.

---

## 1. Division of responsibility

```
MQTT 5.0 = [ wire codec ] + [ broker semantics ]
             mqtt-protocol.cr    lavinmq
```

The shard is a **pure codec with zero semantics**. Its job ends at the wire:
encode and decode every v5 packet correctly, and hand the consumer a reason code
when bytes are invalid. Everything stateful is LavinMQ's: sessions, expiry, flow
control, topic-alias maps, shared subscriptions, capability advertisement,
QoS 2 state machines.

This split is load-bearing. Anyone can build an MQTT 5 server on the shard, and
**every v5 behaviour decision lives in LavinMQ**, which is why the LavinMQ half
is where the remaining work is.

---

## 2. Shard: one IO, a framing strategy per version

The single most important design decision. All v3-vs-v5 wire differences are
isolated into ~9 framing hooks on a `Framing` strategy that the `IO` holds:
`Framing::V3`, `Framing::V5`, and `Framing::Bootstrap` for an IO that has not
seen CONNECT yet. Packet structs are version-agnostic and never branch on the
version. The strategies are stateless, one shared instance per version, so
switching framing allocates nothing.

```mermaid
classDiagram
    class Packet {
        <<abstract struct>>
        +to_io(io)*
        +remaining_length(version) UInt32*
        +bytesize(version) UInt32
        +from_io(io) Packet$
    }
    Packet <|-- Connect
    Packet <|-- Connack
    Packet <|-- Publish
    Packet <|-- PubAck_PubRec_PubRel_PubComp
    Packet <|-- Subscribe
    Packet <|-- SubAck
    Packet <|-- Unsubscribe
    Packet <|-- UnsubAck
    Packet <|-- Disconnect
    Packet <|-- Auth
    Packet <|-- PingReq_PingResp

    class IO {
        +new(socket, max)$ IO  // unpinned, Bootstrap
        +v3(socket, max)$ / v5(socket, max)$ IO  // pinned
        +version() Version
        +negotiated?() Bool
        +read_connect() Connect
        +read_byte() / read_int() / read_string()
        -framing : Framing::Base
    }
    class Base {
        <<Framing, abstract>>
        ~read_properties() / write_properties()*
        ~read_ack_tail() / write_ack()*
        ~read_reason_tail() / write_reason_tail()*
        ~validate_subscription_options()*
        ~allow_empty_topic?()*
    }
    IO o-- Base
    Base <|-- V3
    Base <|-- V5
    V3 <|-- Bootstrap

    class V3 {
        version = V3_1 / V3_1_1
        read_properties -> empty
        write_properties -> no-op
        read_ack_tail -> {nil, empty}
        allow_empty_topic? -> false
    }
    class V5 {
        version = V5
        read_properties -> parse property section
        write_properties -> length-prefixed section
        read_ack_tail -> {reason_byte?, props}
        allow_empty_topic? -> true
    }
    class Bootstrap {
        version = Unknown
        reads only CONNECT
        writes only CONNECT or a v3 CONNACK
    }

    Packet ..> IO : from_io / to_io call the framing hooks
    note for Packet "Packets are VERSION-AGNOSTIC.\nNo `if version.v5?` on the wire path\n(except UnsubAck: genuine structural diff)."
```

**Why a strategy rather than a mutable `io.version` field:** the original field
defaulted to `V3_1_1`, so decoding a CONNACK on a fresh IO silently took the v3
path, and every packet had to remember an `if io.version.v5?` branch. That is
exactly how the PUBREL/PUBCOMP gate bug crept in. Dispatching through the framing
makes the branch impossible to forget, and a future protocol version becomes a
third strategy rather than a third branch in twelve places.

**Why not a subclass per version**, which the shard used until `84codes/mqtt-protocol.cr#16`: CONNECT is
the packet that reveals the version, so a server had to read it on a v3 IO and
then swap in a new IO object. A CONNECT that failed after the level byte was
then answered on the old v3 IO, with v3 framing, whatever version the client
asked for. Holding the framing as a field keeps one IO per connection.

The version is **write-once**: pinned at construction (`IO.v3` / `IO.v5`, what a
client does) or negotiated by the first CONNECT (`IO.new`, what a server does).
A later CONNECT for another version raises `ProtocolError` when read and
`PacketEncode` when written. A repeat CONNECT for the same version is let
through, and LavinMQ answers it with DISCONNECT `0x82` [MQTT-3.1.0-2].

Two acknowledged exceptions:

- `UnsubAck` is the one packet that checks `io.version` directly, because v3 is a
  bare packet id with no payload at all: a structural difference, not a framing
  one.
- `remaining_length(version)` takes the version as a parameter, because there is
  no IO object at size-computation time.

### Version negotiation

```
socket
  -> IO.new(socket, max)   # unpinned: Framing::Bootstrap, reads only CONNECT
  -> io.read_connect       # switches framing in place from the level byte
  -> Connect               # io now frames every later packet for that version
```

The IO keeps its identity, so `connection_factory.cr`'s rescue answers a failed
CONNECT on the same object, with the framing the CONNECT got as far as
revealing. A client whose version was never learned gets a v3 CONNACK, which is
what `Bootstrap` writes ([MQTT-3.1.2-2]).

---

## 3. Other shard decisions

- **Typed properties struct per packet context.** `ConnectProperties`,
  `PublishProperties`, `ConnackProperties` and so on, generated by a
  `define_properties` macro. A property that is illegal in a packet is
  *structurally absent* (no field), not a runtime table lookup. User Property is
  always present as an ordered `Array(StringPair)` since it is legal everywhere.
  Value ranges are declared in the macro spec table, so "it is a Protocol Error
  if value is 0" is enforced by the generated decoder **and** the setter: the
  shard can never construct a packet its own decoder would reject.
- **Repeatable properties are nil-backed** so a packet without them allocates
  nothing on the hot path. The getters deliberately do **not** memoize: these are
  value structs, and a memo set on one copy is lost through the next copy, which
  would break `==`. Build the array and assign it whole.
- **Per-packet reason-code enums.** `Connack::ReasonCode`,
  `Disconnect::ReasonCode`, `SubAck::ReasonCode` and so on, each listing only the
  codes legal for that packet with contextually correct names, because the same
  byte `0x00` means "Success" / "Normal disconnection" / "Granted QoS 0"
  depending on the packet. The v3 `ReturnCode` enums survive only as deprecated
  shims: every packet takes a `ReasonCode`, and the IO hooks pick the v3 byte for
  it on a v3 connection.
- **Decode errors carry the reason code.** `Error::ProtocolError` carries the v5
  reason byte the consumer must respond with, so "which violation maps to which
  reason code" stays protocol knowledge owned by the shard instead of being
  re-derived in every consumer. `PacketDecode` remains the just-close case.
- **Validation split.** The codec rejects only bytes that can never be valid in
  *any* connection state. An empty PUBLISH topic is version-gated (illegal in v3,
  legal in v5 where a Topic Alias substitutes). Topic-alias limits, receive
  maximum and alias resolution are consumer-side, because they depend on state
  the codec does not hold.
- **`remaining_length` by arithmetic precompute, per version, on demand.** No
  serialize-then-measure. It is not frozen at construction, because a frozen
  value matched only one version: that was a real review finding.
- **An IO-owned byte budget bounds every read.** Every length-prefixed field read
  is charged against the packet's remaining length, and `consume(remaining, n)`
  raises `PacketDecode` rather than letting a `UInt32` subtraction underflow.
  This was the single highest-leverage hardening change; it fixed a class of
  malformed-packet DoS bugs (uncaught `OverflowError` / `EOFError`) rather than
  individual instances. `forward_missing_to` was removed from IO so no read can
  bypass the budget.

### Per-packet v3 -> v5 differences

| Packet | Change in v5 |
|---|---|
| `Connect` | protocol level `0x05`; `ConnectProperties`; `Will` gains `WillProperties` |
| `Connack` | `ConnackProperties`; `ReasonCode` (v3 `ReturnCode` on the wire for v3) |
| `Publish` | `PublishProperties`; empty topic legal (Topic Alias substitutes) |
| `PubAck`/`PubRec`/`PubRel`/`PubComp` | reason-code byte + properties, omittable when reason is `0x00` and there are no properties (3.4.2.1) |
| `Subscribe` | `SubscribeProperties`; per-filter options: No Local, Retain As Published, Retain Handling |
| `SubAck`/`UnsubAck` | per-entry `ReasonCode` + properties |
| `Unsubscribe` | properties |
| `Disconnect` | promoted out of `SimplePacket`: optional reason code + properties, **bidirectional** |
| `Auth` | **new** packet type `0x0F` |
| `PingReq`/`PingResp` | unchanged |

---

## 4. LavinMQ decisions

- **`ProtocolViolation` exception -> server DISCONNECT.** Packet handlers raise
  `MQTT::ProtocolViolation` carrying a `Disconnect::ReasonCode`; `read_loop`
  catches it centrally, sends a v5 DISCONNECT with that reason, and publishes the
  will. v3 has no server DISCONNECT packet, so it just closes. The shard's
  `Protocol::Error::ProtocolError` is caught in the same place and its reason byte
  mapped through `disconnect_reason`. One place decides how a violation reaches
  the wire.
- **Capability set built once in `initialize`.** The advertised properties depend
  only on config, which is fixed after startup. The one per-connection variant
  (`assigned_client_identifier`) builds a fresh copy rather than mutating the
  shared struct.
- **v5 PUBLISH properties round-trip through AMQP headers**
  (`publish_headers.cr`). Header-only mapping under an `mqtt.*` prefix, with no
  mapping onto AMQP-native slots like `content_type` / `reply_to`; that
  cross-protocol mapping is a separate concern and a separate decision. User
  Properties are stored as an **array of `{key, value}` tables**, not a flat
  table, because [MQTT-3.3.2-18] requires order and duplicate keys to survive and
  a Hash would lose both.
- **`MAX_QOS = 2u8` in `consts.cr`** clamps granted and delivered QoS through
  `MQTT.granted_qos`, which replaced three different spellings of that clamp. At
  2 the clamp only guards against a bad value read off disk or out of a binding
  table, and the CONNACK omits `maximum_qos`, which may only be sent as 0 or 1
  (§3.2.2.3.4).
- **Granted QoS is clamped at subscribe time, not delivery time.** SUBACK reports
  the clamped value per [MQTT-3.8.4-7], and the session stores and delivers at
  that same value, so the granted QoS and the actual QoS cannot drift apart.
- **Delivery QoS is `min(publish, subscription)`** [MQTT-3.8.4-8] in
  `Exchange#publish`. It used to be the subscription's QoS alone, so a QoS 0
  publish reached a QoS 1 subscriber as QoS 1: LavinMQ then waited for a PUBACK on
  a fire-and-forget message, counted it against `max_inflight_messages`,
  redelivered it on reconnect, and stored it for an offline subscriber. Retained
  replay follows the same rule: the retain store keeps the publisher's QoS, and
  `Broker#subscribe` replays at the lower of that and the granted QoS.
- **The retain store keeps a header per message, under a new file suffix.** A
  `.rmsg` file holds the publish time and `AMQP::Properties` (the publisher's QoS
  and the `mqtt.*` headers) ahead of the payload, so a replay keeps its v5
  properties [MQTT-3.3.2-17], its QoS and its Message Expiry Interval. A legacy
  payload-only `.msg` cannot be told apart by content, so the suffix carries the
  format: legacy files are read as QoS 1 with no properties (QoS 1 was the most
  any older version accepted), and replaced on the topic's next retain. No
  boot-time migration, and a downgrade drops the topics it cannot find rather
  than replaying header bytes as payload.
- **Oversized outbound PUBLISH is dropped, not requeued.** A message exceeding
  the subscriber's Maximum Packet Size is deleted and the delivery completed
  without entering `@unacked`, so it is never redelivered [MQTT-3.1.2-25].
  Requeuing would loop forever, since the packet will be exactly as oversized
  next time.
- **Message expiry is checked at delivery, against the store timestamp.** The
  interval is already in the message headers and the timestamp is written with
  the message, so expiry survives a restart with no new state and costs nothing
  for a message without an interval. "Delivery has started" [MQTT-3.3.2-5] is
  read as "has a remembered packet id", not `env.redelivered`: a message
  requeued for want of a packet id is marked redelivered without ever being
  sent (item P).
- **Any other oversized outbound packet closes the connection.** We put no
  Reason String or User Property on acks, so outside PUBLISH there is nothing
  optional to strip, and the exchange cannot complete without the packet.
  `Client#send` checks every packet against the client's limit [MQTT-3.1.2-24]
  and closes, and the CONNACK is sized before `run_client`, so a client that
  cannot take it never gets a session. Our CONNACK is about 21 bytes with the
  capability set, so a client advertising less cannot connect at all.
- **The v3 wire path is byte-for-byte unchanged.** On v3, properties are ignored
  on the wire, so the v3 CONNACK is identical to before. This is a hard
  constraint: the v3.1.1 suite must stay green throughout.
- **`main`'s `MQTT::ProtocolVersion` enum was removed during the rebase.** #2139
  landed on `main` while this branch was paused and solved the same problem
  (reporting the real protocol in connection details) with a LavinMQ-local enum
  holding only levels 3 and 4. It cannot represent v5, and `Broker#add_client`
  called `ProtocolVersion.from_value(packet.version)`, which **raises on a v5
  connection**. `Client#protocol_name` derives the string from the shard's
  `Version` instead, which covers all three versions, so the local enum is
  redundant. #2139's three specs were kept and pass unchanged. **Worth mentioning
  to whoever wrote #2139.**

### Subscription options

- All three per-filter options are honoured. They are **not** advertisable
  features: v5 CONNACK has no flag for any of them, so unlike shared subscriptions
  this was a real compliance gap rather than a legal deferral.
- **No Local** - `Exchange#publish` takes the publishing client's session name
  and skips a matching subscription that set the bit [MQTT-3.8.3-3]. Identity is
  the ClientID, and a session's name is `mqtt.<client_id>`, so a name compare is
  the spec's test. One Bool test per matched entry; the string compare is paid for
  only by a subscription that asked for it. It applies to the will too, whose
  publisher is the connection that died, so a takeover suppresses the
  predecessor's will if it had such a subscription: the will is published while
  that session is still attached.
- **Retain As Published** - the retain flag is resolved per matched entry in
  `Exchange#publish`, next to the `delivery_mode` it already varies there, and
  read back unchanged by `build_packet`. It is written for *every* matched entry,
  not just the ones that set it, or a `true` leaks into every later subscriber in
  the same tree walk. Skipped entirely unless the publish is retained, so the
  ordinary path is untouched. Safe only because `MessageStore#push` serializes
  properties synchronously, the same invariant `delivery_mode` already relies on;
  worth knowing before anyone makes that store lazy.
- **Retain Handling** - gates the existing retain-store replay in
  `Broker#subscribe`. `Session#subscribe` returns whether the filter was new.
  Existence is by **topic filter alone** [MQTT-3.8.4-3], so a re-subscribe that
  changes the QoS or the options is a replacement, not a new subscription.
  Value 1 sends retained messages only for a subscription that did **not**
  already exist.

  Worth knowing before you verify that by grepping the local spec text: our
  `MQTT-v5.0-spec.txt` has an **Appendix B row for [MQTT-3.3.1-10] that states
  the value-1 case inverted**, contradicting the four body locations that agree
  with each other (3.3.1.3 where the statement is defined, 3.8.3.1's value list,
  and 3.8.4's separate new-vs-replaced rules, which match this implementation
  clause for clause). The body governs. Whether the inversion is a defect in the
  OASIS document or an artifact of our text extraction was not determined: the
  OASIS HTML truncates before chapter 3 and the PDF resists extraction.
  Third-party restatements agree with the body text.
- Persisted in binding arguments as `mqtt.no-local` and
  `mqtt.retain-as-published`, **omitted when false**, so a default subscription's
  arguments table stays byte-identical to what LavinMQ has always written, older
  definitions files load unchanged, and the shared `QOS0_ARGUMENTS` /
  `QOS1_ARGUMENTS` constants remain the zero-allocation path.
  `SubscriptionKey#arguments` renders them, which is required rather than
  cosmetic: `compact!` re-derives every binding from that method instead of
  replaying frames, so anything it cannot reconstruct survives a restart and then
  vanishes at the first compaction.
- `SubscriptionOptions` carries QoS plus the two delivery-time options through
  the subscription tree, replacing the bare `UInt8`. Stored inline as a `Hash`
  value, so no extra allocation. Retain Handling is deliberately not in it: it is
  consulted only during the SUBSCRIBE.
- **v3.1.1 is unaffected by construction, not by convention.**
  `IO::Framing::V3#validate_subscription_options` rejects a v3 SUBSCRIBE with any of bits
  7-2 set, so these paths are unreachable from v3 and need no version gating.
- A prerequisite fixed on the way: `Session#find_binding` matched a **synthetic
  default-exchange binding** whose routing key is the queue's own name, so a
  client subscribing to the literal filter `mqtt.<its own client id>` looked like
  an existing subscription.

### Will

- The six Will Properties that are also PUBLISH properties are carried onto the
  message the will becomes. `client.cr#publish_will` previously built a
  `Protocol::Publish` with no properties at all, so every one was dropped.
  `will_delay_interval` is deliberately not mapped: it is server behaviour, not
  wire content (item E in `MQTT5-TODO.md`).
- No version gate: v3 CONNECT has no will properties, so they are all nil there
  and `IO::Framing::V3#write_properties` discards them regardless.
- A will at QoS 2 is accepted on both versions. Before QoS 2 a v5 will above
  `maximum_qos` was refused with CONNACK `0x9B` (§3.1.2.6); with nothing to
  exceed, that check went.
- A **retained** will keeps its properties for later subscribers too, through
  the retain store.

### Session expiry

- `Session#session_expiry_interval : UInt32` is the single input to a session's
  lifetime; `auto_delete?` and the expiry clock derive from it. `clean_session?`
  is gone from both `Session` and `Client`; it conflated two independent things.
- **`durable?` deliberately does not follow the interval.** It is derived from it
  once, at construction, because it selects the message store's data dir,
  replicator and durability, which cannot be re-derived later. A resuming client
  that narrows a 3600s session to 0 would otherwise flip `durable?` to false while
  its files sit in the durable dir, and the `Queue::Delete` frame is only written
  for a durable queue, so nothing would record the deletion and the original
  declare would replay into a ghost session on the next boot.
- Clean Start is a separate, connect-only input: it decides whether to discard the
  stored session, the interval decides how long the one this connection ends up
  with outlives it. That makes Clean Start 1 plus a non-zero interval
  expressible, which the old single bit could not represent.
- DISCONNECT can name a new interval (§3.14.2.2.2), applied before
  `remove_client` runs. Absent there means keep the CONNECT value, the opposite
  default from CONNECT. Non-zero after a CONNECT of 0 is `0x82`,
  which needs no new code: 3.14.2.2.2 says to answer it as a section 4.13
  protocol error, which is exactly what the `ProtocolViolation` handler does,
  will included.
- The clock runs in the session's existing `deliver_loop`, not a new fiber: the
  offline park is a `select` on `@has_client` versus a timeout, measured from
  `@offline_since`/`@offline_ttl`, which are fixed when the offline window
  starts. A connection claims the session at CONNECT (`Session#resume`), so a
  timer cannot fire between CONNACK and attach. A 0-interval session has no
  timer: `Broker#remove_client` deletes it with its connection, under the
  client-id lock.
- `add_client_locked` deletes an existing 0-interval session before declaring,
  so a takeover of a still-connected 0-interval client ends that session rather
  than resuming it (3.1.4), and Session Present is 0.
- **Session state rides AMQP queue arguments, deliberately but not happily.** A
  session is persisted as a `Queue::Declare` frame, whose only field that can hold
  a `UInt32` is `arguments`, so the interval lives in `x-mqtt-session-expiry`.
  `Session` had to start keeping per-instance arguments for this; it previously
  returned a shared constant and silently dropped whatever it was declared with.
  When the argument is absent the declare's `auto_delete` flag is the fallback,
  because that is what carried this meaning before, which is what keeps an older
  definitions file and a direct `declare_queue` working. **Breaking MQTT away
  from AMQP-shaped persistence is the refactor that removes this.**
- **No deadline is persisted, only the interval.** A restored session starts its
  full interval from boot. MQTT leaves restart behaviour implementation-defined,
  and this is the forgiving reading: it never deletes something a client might
  still want, at the cost of a short interval outliving a long outage.
- An interval changed on a reconnect is in-memory until the next definitions
  compaction; the log has no update frame for a name it already holds. Narrowing
  to 0 is exempt, since it ends in a persisted deletion. See the release notes.
- `Session.expiry_from` warns instead of silently falling back when the argument
  is present but unusable (negative, out of range, not an integer), because an
  AMQP client can declare `mqtt.<id>` by hand.

### Robustness and hot path

- `PublishHeaders.restore` tolerates any AMQP header. Four-byte ints go through
  one `fetch_u32?` and `response_topic` through `fetch_topic?` ([MQTT-3.3.2-14],
  wildcards dropped). Previously `to_u32` on a negative
  `mqtt.message_expiry_interval` raised inside `build_packet`, which requeued and
  re-raised, force-closed the subscriber and re-poisoned on reconnect. An AMQP
  client can reach this by binding `mqtt.<client-id>` to `amq.topic`, since
  `definitions_store.cr` resolves a bind target as
  `@queues[name]? || @sessions[name]?`.
- `Session#get_packet`'s QoS>0 rescue is split in two: the inner one rolls back
  the `@unacked_*` counters it incremented, the outer one requeues. It used to
  subtract unconditionally, so a raise from `build_packet` drove both counters
  negative.
- `Exchange#publish` holds the decoded topic in a local instead of calling
  `packet.topic` two or three times: the getter allocates per call.
- `Session#build_packet` skips the property restore for a v3 subscriber, whose
  `IO::Framing::V3#write_properties` discards them anyway.
- `Client#protocol_name` is an exhaustive `case/in`, so a new `Version` member is
  a compile error rather than a silent "MQTT 3.1.1". `Version::Unknown`, the
  state of an IO before CONNECT, gets an arm that is unreachable in practice: a
  `Client` exists only once CONNECT has set the version.

---

## 5. Breaking shard API changes vs 0.3.1

These are why the shard needs a major-ish release, and what any other consumer
would have to fix. **All of this belongs in the release notes.**

- `Protocol::IO.new(socket)` now starts **unpinned**, reading only CONNECT, and
  `read_connect` sets its version. A client pins one with `IO.v3` / `IO.v5`. The
  `Packet.from_io(io : ::IO)` raw-IO overload is gone, and `Version` gained an
  `Unknown` member, which breaks an exhaustive `case/in` over it.
- `Packet#bytesize` / `#remaining_length` lost their no-arg form and now take a
  `Version`.
- `SubAck` and `Connack` use `ReasonCode` / `reason_codes` (was `ReturnCode` /
  `return_codes`). The old constructors and getters remain, deprecated.
  `Error::Connect` carries a `Connack::ReasonCode`; its `return_code` is
  deprecated.
- **Packets are populated with their v5 meaning on v3 too** (`84codes/mqtt-protocol.cr#19`). A
  property with a spec default reads as that default when absent
  (`receive_maximum` is 65535, the raw value is `receive_maximum?`), and a Bool
  property has only the predicate reader. A v3 CONNECT gets the Session Expiry
  Interval its Clean Session bit means: 0, or `UInt32::MAX` for Clean Session 0.
- Renames, the old names kept as deprecated aliases where possible:
  `Connect#keepalive` -> `keep_alive`, `clean_session?` -> `clean_start?`,
  `Unsubscribe#topics` -> `topic_filters`. Enum members `GrantedQoS0..2` ->
  `GrantedQos0..2` and `QoSNotSupported` -> `QosNotSupported` have no alias.
  `UnsubAck.new` takes `(reason_codes, packet_id)`. `TopicFilter#retain_handling`
  is a `RetainHandling` enum, and Retain Handling 3 decodes as a Protocol Error
  (0x82).
- `Connect`, `Will` and `Publish` constructors take keyword arguments, and
  `Connect` defaults to `Version::V5`. Writing that default on an IO pinned to
  v3 raises `PacketEncode` before any byte goes out, so a consumer that relied
  on the old v3 default fails loudly rather than sending the wrong version.
- A v5 CONNECT with an empty client id and Clean Start 0 is no longer refused at
  decode (§3.1.3.1); v3.1.1 still requires Clean Session [MQTT-3.1.3-8 v3.1.1].
- `Connect#version` is now the `Version` enum, was `UInt8`. **Silent hazard:** in
  Crystal, comparing a `UInt8`-backed enum to an Int compiles and is always
  `false`, so a consumer doing `if connect.version == 4` keeps compiling and
  routes everything into the else branch. Decided (2026-07-02) to keep the enum
  and call it out rather than rename, since LavinMQ is the only consumer.
- `Publish#topic` is stored as `Bytes` but `topic` still returns a decoded
  `String`, so routing code is unchanged. It allocates on **every** call
  (memoization is unreliable through struct copies), so hold the result or use
  `topic_bytes` on hot paths. Documenting that did not stop a consumer from
  calling it three times on the hot path; renaming it to `topic_string` before
  the 1.0 tag would make the cost visible at the call site.
- `PubComp` fixed-header flags corrected `0b0010` -> `0b0000`; the old value was a
  pre-existing v3 wire bug ([MQTT-2.1.3-1]). Decode accepts `0b0010` too, ported
  from shard `main` (`84codes/mqtt-protocol.cr#15`): the library wrote it up to and including v0.3.1, and
  with QoS 2 in LavinMQ, PUBCOMP is now on the wire.
