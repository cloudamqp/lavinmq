# MQTT 5.0 support - status, design, and remaining work

Status and design doc for the MQTT 5.0 work in LavinMQ, spanning both this repo
and the `mqtt-protocol.cr` shard.

Last reconciled against the code: **2026-08-19**.

---

## 1. TL;DR

MQTT 5.0 spans two repos. The **wire codec is done**; the **broker semantics are
about 80% done**.

| | branch | ahead of main | PR | state |
|---|---|---|---|---|
| `mqtt-protocol.cr` | `feat/mqtt5` | 29 commits | none | complete v5 codec, reviewed twice, needs a release tag |
| `lavinmq` | `feat/implement-mqtt5-support` | 18 commits, on current `main` | none | foundation + PUBLISH + SUBSCRIBE/UNSUBSCRIBE + PUBACK/DISCONNECT + full compliance contract |

A v5 client can today connect, subscribe, publish and receive with properties
intact, gets an accurate reason code on every ack, and gets a spec-correct
rejection for every feature we don't implement. What is missing is the rest of
the *session-lifecycle* half of v5: session expiry, will properties and will
delay, subscription options.

**On `main` (`77c9ceb2`) and verified green on 2026-08-19: 2093 examples,
0 failures, lint and format clean.** See section 7.

---

## 2. Scope and the compliance contract

**Read this section first. It is the correctness anchor for the whole project.**

MQTT 5.0 lets a server omit optional features **only if it advertises their
absence in CONNACK** and then rejects clients that use them anyway. Advertising
alone is not compliance, and enforcing alone is not compliance. Both columns
must be ticked, because a client can send anything regardless of what we said.

This is what makes our deferrals legal rather than broken.

| Property advertised in CONNACK | Value | Enforcement on use | Adv. | Enf. |
|---|---|---|---|---|
| `maximum_qos` | `1` | QoS 2 PUBLISH -> DISCONNECT `0x9B` QoSNotSupported | [x] | [x] |
| `topic_alias_maximum` | `0` | PUBLISH with a Topic Alias -> DISCONNECT `0x94` TopicAliasInvalid | [x] | [x] |
| `subscription_identifier_available` | `0` | SUBSCRIBE with a Subscription Identifier -> DISCONNECT `0xA1` | [x] | [x] |
| `shared_subscription_available` | `0` | `$share/...` filter -> DISCONNECT `0x9E` | [x] | [x] |
| `retain_available` | `1` | supported (LavinMQ has a retain store) | [x] | n/a |
| `wildcard_subscription_available` | `1` | supported | [x] | n/a |
| `maximum_packet_size` | `Config#mqtt_max_packet_size` | oversized inbound rejected by the codec; oversized outbound dropped | [x] | [x] |
| `receive_maximum` | omitted (default 65535) | **not enforced** - see limitations | [x] | [ ] |

Plus: enhanced authentication (the AUTH-packet flow, [MQTT-4.12]) is rejected at
CONNECT with CONNACK reason `0x8C` BadAuthenticationMethod, before
username/password auth runs so the reason is accurate.

Everything in this table is implemented and spec'd. **The advertise/enforce
matrix is complete** - this was the largest single risk in the project and it is
closed.

### Deliberately out of scope for the first release

QoS 2 (LavinMQ is QoS 0/1 throughout; PUBREC/PUBREL/PUBCOMP exist in the codec
only), topic aliases, shared subscriptions, subscription identifiers, enhanced
auth, will delay. All are advertised as unavailable per the table above.

---

## 3. Architecture

### 3.1 Division of responsibility

```
MQTT 5.0 = [ wire codec ] + [ broker semantics ]
             mqtt-protocol.cr    lavinmq
```

The shard is a **pure codec with zero semantics**. Its job ends at the wire:
encode and decode every v5 packet correctly, and hand the consumer a reason code
when bytes are invalid. Everything stateful is LavinMQ's: sessions, expiry, flow
control, topic-alias maps, shared subscriptions, capability advertisement,
QoS 2 state machines.

This split is load-bearing. It means anyone can build an MQTT 5 server on the
shard, and it means **every v5 behaviour decision lives in LavinMQ**, which is
why the LavinMQ half is where the remaining work is.

### 3.2 Shard: the version is the IO's *type*

The single most important design decision. All v3-vs-v5 wire differences are
isolated into ~9 framing hooks on an abstract `IO`, with concrete `IO::V3` and
`IO::V5` subclasses. Packet structs are version-agnostic and never branch on the
version.

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
        <<abstract class>>
        +version() Version*
        +read_byte() / read_int() / read_string()
        +variable_byte_int(max)
        +consume(remaining, n)  // checked
        ~read_properties(klass, remaining)*
        ~write_properties(props)*
        ~read_ack_tail() / write_ack()*
        ~read_reason_tail() / write_reason_tail()*
        ~validate_subscription_options()*
        ~read_connack_reason() / write_connack_body()*
        ~allow_empty_topic?()*
        +for(version, socket)$ IO
        +read_connect(socket)$ {Connect, IO}
    }
    IO <|-- V3
    IO <|-- V5

    class V3 {
        version = V3_1 / V3_1_1
        read_properties -> {empty, 0}
        write_properties -> no-op
        read_ack_tail -> {nil, empty}
        write_connack_body -> session+return_code byte
        allow_empty_topic? -> false
    }
    class V5 {
        version = V5
        read_properties -> parse property section
        write_properties -> length-prefixed section
        read_ack_tail -> {reason_byte?, props}
        write_connack_body -> session+reason+props
        allow_empty_topic? -> true
    }

    Packet ..> IO : from_io / to_io call hooks
    note for Packet "Packets are VERSION-AGNOSTIC.\nNo `if version.v5?` on the wire path\n(except UnsubAck: genuine structural diff)."
```

**Why it replaced the original mutable `io.version` field:** the field defaulted
to `V3_1_1`, so decoding a CONNACK on a fresh IO silently took the v3 path, and
every packet had to remember an `if io.version.v5?` branch. That is exactly how
the PUBREL/PUBCOMP gate bug crept in. Type dispatch makes the branch impossible
to forget, and a future protocol version becomes a third subclass rather than a
third branch in twelve places.

Two acknowledged exceptions:
- `UnsubAck` is the one packet that checks `io.version` directly, because v3 is a
  bare packet id with no payload at all - a structural difference, not a framing
  one.
- `remaining_length(version)` takes the version as a parameter, because there is
  no IO object at size-computation time.

### 3.3 Version negotiation

CONNECT is the only packet that *reveals* the version, via its protocol-level
byte. The flow:

```
socket
  -> IO::V3.new(socket, max)          # bootstrap reader
  -> io.read_connect                  # reads CONNECT, reframes to the negotiated version
  -> {Connect, IO::V3 | IO::V5}       # every subsequent packet uses this IO
```

The instance method `io.read_connect` (not the class method) is what a
*rejecting* server needs: it only rebinds `io` on success, so if the CONNECT is
malformed the v3 bootstrap IO survives and can still frame a rejection CONNACK.
LavinMQ uses this at `connection_factory.cr:31`. The class-method
`IO.read_connect(socket, max)` builds its IO internally and raises before
returning, leaving a rejecting server with nothing to answer on. That instance
method was added to the shard specifically because a LavinMQ test caught the
trap.

### 3.4 Other shard design decisions

- **Typed properties struct per packet context.** `ConnectProperties`,
  `PublishProperties`, `ConnackProperties` etc., generated by a
  `define_properties` macro. A property that is illegal in a packet is
  *structurally absent* (no field), not a runtime table lookup. User Property is
  always present as an ordered `Array(StringPair)` since it is legal everywhere.
  Value ranges are declared in the macro spec table, so "it is a Protocol Error
  if value is 0" is enforced by the generated decoder **and** the setter - the
  shard can never construct a packet its own decoder would reject.
- **Repeatable properties are nil-backed** so a packet without them allocates
  nothing on the hot path. The getters deliberately do **not** memoize: these are
  value structs, and a memo set on one copy is lost through the next copy, which
  would break `==`. Build the array and assign it whole.
- **Per-packet reason-code enums.** `Connack::ReasonCode`,
  `Disconnect::ReasonCode`, `SubAck::ReasonCode`, ... Each lists only the codes
  legal for that packet, with contextually correct names, because the same byte
  `0x00` means "Success" / "Normal disconnection" / "Granted QoS 0" depending on
  the packet. The v3 `Connack::ReturnCode` stays as the v3 wire format; the
  IO hooks pick which byte goes on the wire.
- **Decode errors carry the reason code.** `Error::ProtocolError` carries the v5
  reason byte the consumer must respond with, so "which violation maps to which
  reason code" stays protocol knowledge owned by the shard instead of being
  re-derived in every consumer. `PacketDecode` remains the just-close case.
- **Validation split.** The codec rejects only bytes that can never be valid in
  *any* connection state. Empty PUBLISH topic is version-gated (illegal in v3,
  legal in v5 where a Topic Alias substitutes). Topic-alias limits, receive
  maximum and alias resolution are all consumer-side because they depend on state
  the codec does not hold.
- **`remaining_length` by arithmetic precompute, per version, on demand.** No
  serialize-then-measure. It is not frozen at construction, because a frozen
  value matched only one version - that was a real review finding.
- **An IO-owned byte budget bounds every read.** Every length-prefixed field read
  is charged against the packet's remaining length, and `consume(remaining, n)`
  raises `PacketDecode` rather than letting a `UInt32` subtraction underflow.
  This was the single highest-leverage hardening change; it fixed a class of
  malformed-packet DoS bugs (uncaught `OverflowError` / `EOFError`) rather than
  individual instances. `forward_missing_to` was removed from IO so no read can
  bypass the budget.

### 3.5 Per-packet v3 -> v5 differences

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

### 3.6 LavinMQ-side design decisions

- **`ProtocolViolation` exception -> server DISCONNECT.** Packet handlers raise
  `MQTT::ProtocolViolation` carrying a `Disconnect::ReasonCode`; `read_loop`
  catches it centrally, sends a v5 DISCONNECT with that reason, and publishes the
  will. v3 has no server DISCONNECT packet, so it just closes. Shard
  `Protocol::Error::ProtocolError` is caught in the same place and its reason
  byte mapped through `disconnect_reason`. One place decides how a violation
  reaches the wire.
- **Capability set built once in `initialize`.** The advertised properties depend
  only on config, which is fixed after startup. The one per-connection variant
  (`assigned_client_identifier`) builds a fresh copy rather than mutating the
  shared struct.
- **v5 PUBLISH properties round-trip through AMQP headers**
  (`publish_headers.cr`). Header-only mapping under an `mqtt.*` prefix, no
  mapping onto AMQP-native slots like `content_type` / `reply_to`; that
  cross-protocol mapping is a separate concern and a separate decision. User
  Properties are stored as an **array of `{key, value}` tables**, not a flat
  table, because [MQTT-3.3.2-18] requires order and duplicate keys to survive and
  a Hash would lose both.
- **`MAX_QOS = 1u8` in `consts.cr`** is the single source for "QoS 2 is not
  implemented": advertised in CONNACK, enforced on inbound v5 PUBLISH, and used
  to clamp granted and delivered QoS.
- **Granted QoS is clamped at subscribe time, not delivery time.** SUBACK reports
  the clamped value per [MQTT-3.8.4-7], and the session stores and delivers at
  that same value, so the granted QoS and the actual QoS cannot drift apart.
- **Oversized outbound PUBLISH is dropped, not requeued.** A message exceeding
  the subscriber's Maximum Packet Size is deleted and the delivery completed
  without entering `@unacked`, so it is never redelivered [MQTT-3.1.2-25].
  Requeuing would loop forever, since the packet will be exactly as oversized
  next time.
- **`main`'s `MQTT::ProtocolVersion` enum was removed during the rebase.** #2139
  landed on `main` while this branch was paused and solved the same problem
  (reporting the real protocol in connection details) with a LavinMQ-local enum
  holding only levels 3 and 4. It cannot represent v5, and `Broker#add_client`
  called `ProtocolVersion.from_value(packet.version)`, which **raises on a v5
  connection**. Our `Client#protocol_name` derives the string from the shard's
  `Version` instead, which already covers all three versions, so the local enum
  is redundant. #2139's three specs were kept and pass unchanged (only their
  `version:` arguments moved from `UInt8` to the `Version` enum). **Worth
  mentioning to whoever wrote #2139.**
- **The v3 wire path is byte-for-byte unchanged.** On v3, properties are ignored
  on the wire, so the v3 CONNACK is identical to before. This is a hard
  constraint: the v3.1.1 suite must stay green throughout.

### 3.7 Breaking API changes in the shard vs 0.3.1

These are why the shard needs a major-ish release, and what any other consumer
would have to fix:

- `Protocol::IO` is now **abstract**. `IO.new(socket)` and the
  `Packet.from_io(io : ::IO)` raw-IO overload are gone. Construct `IO::V3` /
  `IO::V5`, or use `read_connect`.
- `Packet#bytesize` / `#remaining_length` lost their no-arg form and now take a
  `Version`.
- `SubAck` uses `ReasonCode` / `reason_codes` (was `ReturnCode` /
  `return_codes`). No compat shim. `Connack` **is** back-compat: the
  `(Bool, ReturnCode)` overload and `return_code` getter are retained.
- `Connect#version` is now the `Version` enum, was `UInt8`. **Silent hazard:**
  in Crystal, comparing a `UInt8`-backed enum to an Int compiles and is always
  `false`, so a consumer doing `if connect.version == 4` keeps compiling and
  routes everything into the else branch. Decided (2026-07-02) to keep the enum
  and call it out in the PR rather than rename, since LavinMQ is the only
  consumer and both sides are ours. **This must be in the shard release notes.**
- `Publish#topic` is stored as `Bytes` but `topic` still returns a decoded
  `String`, so routing code is unchanged. It allocates on **every** call
  (memoization is unreliable through struct copies) - hold the result or use
  `topic_bytes` on hot paths. Decided to keep the name and document the cost.
- `PubComp` fixed-header flags corrected `0b0010` -> `0b0000`; the old value was
  a pre-existing v3 wire bug ([MQTT-2.2.2-1]). Strict rejection kept by decision
  (2026-07-02): nothing in use involves PUBCOMP, since LavinMQ is QoS 0/1, and a
  leniency shim would tolerate a wire form nobody emits. **Note it in the
  changelog.**

---

## 4. What is done in LavinMQ

All of this is committed on `feat/implement-mqtt5-support` with specs.

**Foundation**
- Version negotiation on a single listener (3.1 / 3.1.1 / 5) via
  `io.read_connect` - `connection_factory.cr:25-40`
- Version carried onto `Client`; `details_tuple` reports the real protocol name
  instead of the hardcoded `"MQTT 3.1.1"` - `client.cr#protocol_name`
- `packet.bytesize` -> `@io.bytesize(packet)` everywhere (read accounting, send
  accounting)
- Client's Maximum Packet Size plumbed CONNECT -> `Broker#add_client` -> `Client`

**CONNECT / CONNACK**
- Full capability advertisement - `connection_factory.cr#build_server_capabilities`
- `assigned_client_identifier` echoed when we generate a client id
  [MQTT-3.2.2-16]; refactored from rebuilding the packet to
  `Connect#copy_with` - `connection_factory.cr#generated_client_id`
- Enhanced auth rejected with `0x8C` before authentication runs
- `Connack::ReasonCode.from_v3_return_code` bridges the v3 accept path

**Error handling**
- `ProtocolViolation` + central `read_loop` handling -> v5 server DISCONNECT
- Shard `ProtocolError` reason byte mapped to a DISCONNECT reason code

**PUBLISH**
- v5 property passthrough end to end, publisher -> store -> subscriber, via
  `publish_headers.cr` (new file): payload format indicator, message expiry
  interval, response topic, correlation data, content type, user properties
- Maximum Packet Size enforced on delivery - `session.cr#exceeds_max_packet_size?`
- QoS 2 rejected `0x9B`; Topic Alias rejected `0x94`; empty topic rejected
  `0x82` by the codec

**SUBSCRIBE / SUBACK**
- Granted QoS clamped to `MAX_QOS`, reported in SUBACK as a `ReasonCode`
- Subscription Identifier rejected `0xA1`; `$share/` rejected `0x9E`, including
  when mixed with valid filters (the whole packet fails)

**UNSUBSCRIBE / UNSUBACK**
- Per-topic reason codes, `Success` vs `NoSubscriptionExisted`;
  `Session#unsubscribe` now returns a Bool to drive that [MQTT-3.11.3]

**PUBACK / ack reason codes**
- `NoMatchingSubscribers` (`0x10`) when the publish matched no session;
  `Exchange#publish` already returned the match count [MQTT-3.4.2.1]
- `NotAuthorized` (`0x87`) instead of a silent `close_socket`, via
  `client.cr#refuse_publish`. QoS > 0 gets a PUBACK and keeps the connection;
  QoS 0 has no ack, so it gets a server DISCONNECT `0x87` (spec 3.3.4)
- SUBSCRIBE denial likewise answers a SUBACK of per-filter `NotAuthorized`
- Inbound non-`Success` PUBACK is logged and still acks the message: a refused
  QoS 1 delivery is finished, not retried [MQTT-3.4.2.1]

**Client DISCONNECT**
- The reason code is honoured: only `0x00` discards the will, so `0x04`
  (Disconnect with Will Message) and every error code publish it
  [MQTT-3.14.4-3]. We send nothing back - the receiver just closes
  [MQTT-3.14.4-2]

**Specs:** 34 examples in `spec/mqtt/v5/` (connect, publish, subscribe,
unsubscribe, puback/disconnect), plus the existing v3 suite adapted to the new
shard API.

---

## 5. What is left

Ordered roughly easiest-first. **B and D are independent of each other and of
everything else** - these are the two clean hand-offs. (C is done; see
section 4.)

### B. Honor subscription options

The shard already parses the per-filter options byte and exposes `no_local`,
`retain_as_published` and `retain_handling` on `TopicFilter`.
`Broker#subscribe` currently reads only `tf.qos` and always replays the retain
store. Pure LavinMQ behaviour, no shard work, well-bounded.

- **Retain Handling** (0 = send retained on subscribe, 1 = only if the
  subscription is new, 2 = never). We currently always behave as 0.
- **No Local** - do not deliver a message back to the client that published it.
- **Retain As Published** - preserve the publisher's retain flag on delivery
  instead of clearing it.
- Files: `broker.cr#subscribe`, `session.cr#build_packet`, `client.cr#recieve_subscribe`.

### D. Session expiry

v5 replaces the clean-session boolean with Clean Start + Session Expiry
Interval. We read `ConnectProperties#maximum_packet_size` and nothing else;
`session_expiry_interval` is ignored, and DISCONNECT can also carry a new
expiry value. Sessions are still clean-session-or-forever.

C landed the DISCONNECT reason-code path, so the packet is now inspected in
`read_loop`; reading `DisconnectProperties#session_expiry_interval` there is the
natural next step. That also brings [MQTT-3.14.2]: a non-zero expiry on
DISCONNECT when CONNECT sent zero is a Protocol Error (`0x82`), which we cannot
enforce until the CONNECT value is stored.

- Files: `connection_factory.cr`, `broker.cr#add_client`, `sessions.cr`, `session.cr`.

### E. Will properties and Will Delay

`client.cr#publish_will` constructs a `Protocol::Publish` with **no properties
at all**, so every v5 Will Property (payload format, message expiry, content
type, response topic, correlation data, user properties) is silently dropped.
`WillProperties#will_delay_interval` is also unread; will delay is currently in
the deferred list but is not advertised as unavailable, because MQTT has no
capability flag for it.

- Files: `client.cr#publish_will`, plus the `Will` plumbing through `Broker`.

### F. Retained messages lose v5 properties

The retain store keeps only the body (`retain_store.cr#retain` takes topic +
body + size). A retained v5 message therefore reaches a later subscriber with
its properties stripped. Needs a store-format change, so it is the most invasive
of the remaining items. `topic_tree.cr` (which backs the retain store) also
still uses `StringTokenIterator`, unlike the publish-path subscription tree.

### G. Shard release and open items

- **Cut a tagged release.** `shard.yml` currently pins `branch: feat/mqtt5`,
  which cannot ship. Given the breaking changes in 3.7, `1.0` was the intended
  target. `main` is at `0.3.1`.
- Decide **U1**: v5 CONNACK with a non-zero reason but `session_present = 1` is
  accepted at decode. Arguably a server-side semantic rather than a codec rule.
- Optional low-severity conformance gaps, none blocking: **N3** packet
  identifier `0` accepted where a non-zero id is required; **O1** zero-entry
  SUBSCRIBE / UNSUBSCRIBE / SUBACK accepted at decode; **O2** AUTH accepted on a
  v3 connection; **O3** some receiver-side property value validations missing.
- Optional test gaps: **N5** no malformed property-*value* test (the UTF-8 / NUL
  validation branch has zero coverage); **N6** the `consumed != total`
  intra-section property guard is untested; **U2** v3 CONNACK return-code byte
  `>= 6` rejection untested.

### H. Cross-cutting cleanup

- **Merge the v5 specs back.** `spec/mqtt/v5/*_spec.cr` were kept separate so
  each chunk's diff stayed self-contained. Before the PR, fold each into the
  matching `spec/mqtt/integrations/*_spec.cr` and delete the v5 file. **Except**
  the advertise-and-reject compliance matrix, which stays as its own standing
  file. Nothing has been merged back yet: connect, subscribe, unsubscribe,
  publish and puback/disconnect are all outstanding. The DISCONNECT/will
  examples in `puback_disconnect_spec.cr` belong next to
  `integrations/will_spec.cr`, not `publish_spec.cr`.
- Consider moving `build_server_capabilities` into `consts.cr` or making it a
  constant, to make it obvious it is static.
- Consider adding a v5 mode to the `lavinmqperf mqtt` throughput tool. It is
  now pinned to `IO::V3` (section 7), so there is no load-testing path for v5
  at all. Optional, not a blocker.

---

## 6. Known limitations to state at release

Things we will ship with, deliberately. These belong in the release notes and in
the docs, not in a bug tracker.

- **QoS 2 unsupported.** Advertised as Maximum QoS 1.
- **Receive Maximum ignored.** We do not pace QoS 1 inflight against the
  client's advertised Receive Maximum, and we do not advertise our own (so
  clients assume the 65535 default). LavinMQ has its own
  `Config#max_inflight_messages` cap instead. This is the one line in the
  compliance table with an unticked enforcement column.
- **Topic aliases unsupported** in both directions.
- **Shared subscriptions unsupported.**
- **Subscription identifiers unsupported.**
- **Enhanced authentication unsupported.**
- **Will delay interval ignored** (wills fire immediately).
- **Payload Format Indicator is not validated.** Spec 3.3.2.3.2 only says a
  server MAY check that a payload declared as UTF-8 really is, so we never
  answer `0x99` PayloadFormatInvalid. Validating means a String allocation plus
  a UTF-8 scan on the publish hot path for an optional check.
- **No Reason Strings or User Properties on ack packets.** [MQTT-3.1.2-29]
  makes them illegal on anything but PUBLISH / CONNACK / DISCONNECT when the
  client set Request Problem Information to 0, and we never send them, so
  `request_problem_information` needs no plumbing.
- **UNSUBSCRIBE is not permission checked** (it never was, on v3 either), so
  UNSUBACK never carries `0x87` NotAuthorized.
- v5 PUBLISH properties are carried in `mqtt.*` AMQP headers and are **not**
  mapped onto AMQP-native properties, so an AMQP consumer of an MQTT-published
  message sees them as headers.

---

## 7. Testing

**Strategy:** the shard has exhaustive codec specs, so LavinMQ tests
**behaviour**, not framing. Per packet: a v5 client connects, exercises the
feature, and we assert the broker's response and reason codes. The v3.1.1 suite
must stay green throughout, since the v3 wire path is unchanged.

**Shard:** 259 examples, 0 failures as of 2026-06-30, `crystal tool format
--check` clean. Exact-byte vectors are the backbone, with round-trip tests
focused on properties and a version matrix for the genuinely ambiguous cases
(PUBREL/PUBCOMP remaining-length 2 vs a reason tail; CONNACK return code vs
reason code). A blanket version matrix was deliberately dropped as redundant:
the `IO::V3`/`IO::V5` split makes "a v5 packet parsed with v3 framing"
structurally hard to even express.

**LavinMQ, measured 2026-08-19** after the PUBACK/DISCONNECT work, on `main`
`77c9ceb2`:

| what | result |
|---|---|
| `make test SPEC=spec/mqtt` | **248 examples, 0 failures, 0 errors, 0 pending** (18.0s) |
| `make test TAGS=~etcd` | **2093 examples, 0 failures, 0 errors, 9 pending** (2:35) |
| `make lint` | 400 inspected, 0 failures |
| `crystal tool format --check` | clean |

(The previous reconciliation recorded 233 for `spec/mqtt`; the full-suite delta
is exactly the 10 new examples, so that figure was mistyped, not a lost spec.)

The 9 pending are pre-existing and unrelated to MQTT (queue dead-lettering
headers, kTLS, UNIX sockets, VHost GC segments). The etcd-tagged specs were
skipped because no etcd is running locally, not because they fail.

**The v5 work is green and has not regressed anything on the AMQP side.**

One shard-bump miss was found and fixed during this run:
`src/lavinmqperf/mqtt/throughput.cr:101` still called
`LavinMQ::MQTT::Protocol::IO.new(socket)`, which stopped compiling when the
shard made `IO` abstract (breaking change A in section 3.7). Changed to
`Protocol::IO::V3.new(socket)`, since the perf tool speaks 3.1.1. It hid for a
while because `spec/mqtt` never requires `lavinmqperf` - only the full suite
does, so a targeted MQTT spec run looked green while `make test` died at compile
time. The other three `Packet.from_io(io)` call sites in that file are fine: the
`from_io(io : Protocol::IO)` overload survived, only the raw-`::IO` one was
removed. Nothing else in `src/` or `spec/` constructs the abstract `IO`
(verified by grep). Committed as `9e2f53f9`.

**Gap worth closing before release:** all byte vectors are self-confirming
round-trips. Nobody has cross-checked them against a real v5 client
(`mosquitto_pub -V 5`, paho, MQTTX). That is the test that catches
"consistently implemented my own wrong format." `mosquitto-clients` is
apt-installable.

---

## 8. Sequencing to ship

1. Finish B / D / E (F and G in parallel, different people).
2. External smoke test against a real v5 client.
3. Tag the shard release (`1.0`), with 3.7's breaking changes in the changelog.
4. Repoint `shard.yml` from `branch: feat/mqtt5` to the tag, update `shard.lock`.
5. Merge back the v5 specs (section H).
6. Open the LavinMQ PR as draft.

The per-feature commit granularity is deliberate: each rejection in the
section 2 compliance table is its own commit with its spec citation and its
test, which is the unit a reviewer checks and the template for the remaining
work.

Rebasing this work onto `main` is cheap today, with two standing hazards. The
`shard.yml` / `shard.lock` v5 pin conflicts as soon as `main` bumps the shard
again - step 3 above ends that permanently. And `connection_factory.cr#start`
and `client.cr#details_tuple` are conflict magnets, for the reason in 3.7.

---

## Appendix: parked PUBLISH topic perf investigation

Kept because the conclusion is non-obvious and someone will suggest it again.

A colleague proposed reading the PUBLISH topic as `Bytes` instead of `String`.
Prototyped off shard `main` (commit `e16e186`, never pushed). Isolated
microbenchmark showed 1.8x-3.0x faster topic decode.

**It does not help LavinMQ.** The topic must become an AMQP `String`
routing_key anyway - `routing_key : String` is load-bearing across the shared
AMQP core (message store on-disk format, bindings, dead-lettering, management
API). So LavinMQ would call `String.new(packet.topic)` regardless, making the
change net-negative: a Bytes alloc in the shard *plus* a String alloc in
LavinMQ, saving only the UTF-8 validation we arguably want.

**The real win was elsewhere and it already landed.** The subscription-tree
match allocated one throwaway `String` per topic level on every publish. Byte
slicing the tokenizer makes that zero-allocation, needs no shard change, and is
**already on `main`** via `bytes_token_iterator.cr` (#1920) -
`subscription_tree.cr` uses `BytesTokenIterator` today. `topic_tree.cr` (retain
store) still uses `StringTokenIterator`, but it is off the publish hot path.

`feat/mqtt5` has the Bytes-topic change baked into its `publish.cr` already, so
the two branches conflict. Park the breaking version until MQTT is decoupled
from the AMQP core and `routing_key` no longer has to be a String.

**Dead end, do not repeat:** a `Char::Reader` single-pass UTF-8 validation in
`read_string` benchmarked **5-17x slower** than the stdlib two-scan
(`includes?('\0') || !valid_encoding?`). It was reverted.

---

## Doc hygiene

- Status claims here are only as fresh as the "last reconciled" date at the
  top. Re-derive from the code before trusting a checkbox.
- To verify reason codes and behaviour, keep a local untracked copy of the full
  OASIS spec text (`MQTT-v5.0-spec.txt`, ~311KB) in the repo root and grep it.
  The OASIS HTML truncates before chapter 3 in most fetchers.
