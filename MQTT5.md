# MQTT 5.0 support - status

Status doc for the MQTT 5.0 work in LavinMQ, spanning this repo and the
`mqtt-protocol.cr` shard.

Last reconciled against the code: **2026-09-08**.

## Doc map

| file | what is in it |
|---|---|
| `MQTT5.md` (this) | status, the compliance contract, what works today, how to ship |
| `MQTT5-DESIGN.md` | architecture and the decisions behind it, both repos |
| `MQTT5-TODO.md` | remaining work, ordered |
| `MQTT5-TESTING.md` | test strategy, current numbers, external verification |
| `MQTT5-RELEASE-NOTES.md` | known limitations and behaviour changes to announce |
| `MQTT5-FUTURE.md` | deferred past this PR, chiefly QoS 2 |
| `MQTT5-INTEROP.md` | the interop harness and how to re-run it |

---

## 1. TL;DR

MQTT 5.0 spans two repos. The **wire codec is done**; the **broker semantics are
about 92% done**.

| | branch | ahead of main | PR | state |
|---|---|---|---|---|
| `mqtt-protocol.cr` | `feat/mqtt5` | 29 commits | none | complete v5 codec, reviewed twice, needs a release tag |
| `lavinmq` | `feat/implement-mqtt5-support` | 37 commits, on current `main` | none | foundation, PUBLISH, SUBSCRIBE/UNSUBSCRIBE, PUBACK/DISCONNECT, delivery QoS, session expiry, subscription options, will properties, full compliance contract |

A v5 client can today connect, subscribe, publish and receive with properties
intact, gets an accurate reason code on every ack, gets a session whose lifetime
it controls, gets its subscription options honoured, gets its will published
with its properties intact, and gets a spec-correct rejection for every feature
we do not implement. Missing: the Will Delay Interval, and properties on
retained messages.

Green on 2026-09-08: 2225 examples, 0 failures, lint and format clean.

---

## 2. The compliance contract

**This is the correctness anchor for the whole project.**

MQTT 5.0 lets a server omit optional features **only if it advertises their
absence in CONNACK** and then rejects clients that use them anyway. Advertising
alone is not compliance, and enforcing alone is not compliance. Both columns
must be ticked, because a client can send anything regardless of what we said.
This is what makes our deferrals legal rather than broken.

| Property advertised in CONNACK | Value | Enforcement on use | Adv. | Enf. |
|---|---|---|---|---|
| `maximum_qos` | `1` | QoS 2 PUBLISH -> DISCONNECT `0x9B`; QoS 2 Will -> CONNACK `0x9B` | [x] | [x] |
| `topic_alias_maximum` | `0` | PUBLISH with a Topic Alias -> DISCONNECT `0x94` TopicAliasInvalid | [x] | [x] |
| `subscription_identifier_available` | `0` | SUBSCRIBE with a Subscription Identifier -> DISCONNECT `0xA1` | [x] | [x] |
| `shared_subscription_available` | `0` | `$share/...` filter -> DISCONNECT `0x9E` | [x] | [x] |
| `retain_available` | `1` | supported (LavinMQ has a retain store) | [x] | n/a |
| `wildcard_subscription_available` | `1` | supported | [x] | n/a |
| `maximum_packet_size` | `Config#mqtt_max_packet_size` | oversized inbound rejected by the codec; oversized outbound dropped | [x] | [~] |
| `receive_maximum` | omitted (default 65535) | **not enforced**, see the release notes | [x] | [ ] |

Plus: enhanced authentication (the AUTH-packet flow, [MQTT-4.12.0-1]) is rejected at
CONNECT with CONNACK `0x8C` BadAuthenticationMethod, before username/password
auth runs so the reason is accurate.

**The one `[~]` row:** `maximum_packet_size` is checked only on the outbound
PUBLISH path, while [MQTT-3.1.2-24] covers *every* packet the server sends. A
client may legally advertise any limit >= 1, so a very small limit already gets
an oversized CONNACK, and a SUBSCRIBE with many filters an oversized SUBACK.
Tracked as item I in `MQTT5-TODO.md`.

`maximum_qos` was the other `[~]` until the Will QoS check landed; both of its
paths, inbound PUBLISH and the Will at CONNECT, are enforced now. Everything
else in the table is implemented and spec'd, which was the largest single risk
in the project.

### Out of scope for the first release

QoS 2, topic aliases, shared subscriptions, subscription identifiers, enhanced
auth. All advertised as unavailable per the table above.

Two things could **not** be deferred that way, because MQTT has no capability
flag for them: the per-filter subscription options (done) and the Will Delay
Interval (item E). Shipping without those is a real gap, not a legal deferral.

---

## 3. What works today

Facts only; the reasoning is in `MQTT5-DESIGN.md`. All of it is committed on
`feat/implement-mqtt5-support` with specs.

**Foundation**
- Version negotiation on a single listener (3.1 / 3.1.1 / 5) via `io.read_connect`
- Version carried onto `Client`; `details_tuple` reports the real protocol name
  instead of a hardcoded `"MQTT 3.1.1"`
- All packet sizing goes through `@io.bytesize(packet)`
- The client's Maximum Packet Size is plumbed CONNECT -> `Broker#add_client` -> `Client`

**CONNECT / CONNACK**
- Full capability advertisement (`build_server_capabilities`)
- `assigned_client_identifier` echoed when we generate a client id [MQTT-3.2.2-16]
- Enhanced auth rejected `0x8C` before authentication runs
- `Connack::ReasonCode.from_v3_return_code` bridges the v3 accept path

**Error handling**
- Handlers raise `MQTT::ProtocolViolation`; `read_loop` catches it centrally,
  sends a v5 DISCONNECT with that reason and publishes the will. v3 just closes.
- The shard's `ProtocolError` reason byte maps through the same path
- An unexpected packet is a `ProtocolViolation(ProtocolError)`: v5 gets
  DISCONNECT `0x82` and the log gets one WARN line

**PUBLISH**
- All six v5 properties round-trip publisher -> store -> subscriber via
  `publish_headers.cr`: payload format indicator, message expiry interval,
  response topic, correlation data, content type, user properties
- Maximum Packet Size enforced on delivery; an oversized message is dropped, not
  requeued
- QoS 2 rejected `0x9B`, Topic Alias `0x94`, empty topic `0x82` (by the codec)
- Delivery QoS is `min(publish QoS, subscription QoS)` [MQTT-3.8.4-8], on v3.1.1 too

**SUBSCRIBE / SUBACK**
- Granted QoS clamped to `MAX_QOS` and reported in SUBACK as a `ReasonCode`
- Subscription Identifier rejected `0xA1`; `$share/` rejected `0x9E`, including
  when mixed with valid filters (the whole packet fails)
- All three per-filter options honoured: No Local, Retain As Published, Retain
  Handling
- Options persist in binding arguments as `mqtt.no-local` and
  `mqtt.retain-as-published`, omitted when false

**UNSUBSCRIBE / UNSUBACK**
- Per-topic reason codes, `Success` vs `NoSubscriptionExisted` [MQTT-3.11.3-2]

**PUBACK and ack reason codes**
- `NoMatchingSubscribers` `0x10` when the publish matched no session
- `NotAuthorized` `0x87` instead of a silent close; QoS 0 has no ack, so it gets
  a server DISCONNECT `0x87` instead
- SUBSCRIBE denial answers a SUBACK of per-filter `NotAuthorized`
- An inbound non-`Success` PUBACK is logged and still acks the message

**DISCONNECT**
- The client's reason code is honoured: only `0x00` discards the will
  [MQTT-3.14.4-3]. We send nothing back.
- It may carry a new Session Expiry Interval (§3.14.2.2.2)

**Will**
- The six will properties that are also PUBLISH properties are carried onto the
  message the will becomes
- A v5 will at QoS 2 is refused with CONNACK `0x9B` (3.1.2.6); v3 stays accepted
  and clamped at delivery, as before

**Session expiry**
- `Session#session_expiry_interval : UInt32` is the single input to a session's
  lifetime; `clean_session?` is gone from both `Session` and `Client`
- Derived at CONNECT: v5 reads the property, absent meaning 0 (§3.1.2.11.2);
  v3 maps `clean_session=1` to 0 and `clean_session=0` to `UInt32::MAX`
- DISCONNECT can narrow it, applied before the client is removed, so narrowing
  to 0 ends the session on that disconnect
- The clock runs in the session's existing `deliver_loop`; reattaching cancels it

---

## 4. Sequencing to ship

1. Finish the Will Delay half of item E. F and G can run in parallel, different
   people.
2. Re-run the interop harness once E and F land. It has not been re-run since
   the delivery-QoS and item I fixes; `MQTT5-INTEROP.md` names the rows to
   confirm.
3. Tag the shard release (`1.0`), with the breaking changes from
   `MQTT5-DESIGN.md` in the changelog.
4. Repoint `shard.yml` from `branch: feat/mqtt5` to the tag, update `shard.lock`.
5. Merge the v5 specs back into the integration files (item H).
6. Open the LavinMQ PR as draft.

The per-feature commit granularity is deliberate: each rejection in the
compliance table is its own commit with its spec citation and its test, which is
the unit a reviewer checks and the template for the remaining work.

Rebasing onto `main` is cheap today, with two standing hazards. The `shard.yml`
/ `shard.lock` v5 pin conflicts as soon as `main` bumps the shard again, which
step 3 ends permanently. And `connection_factory.cr#start` and
`client.cr#details_tuple` are conflict magnets.

---

## Doc hygiene

- Status claims are only as fresh as the "last reconciled" date at the top.
  Re-derive from the code before trusting a checkbox.
- These docs name methods and files, not line numbers. Grep for the name.
- To verify reason codes and behaviour, keep a local untracked copy of the OASIS
  spec text (`MQTT-v5.0-spec.txt`, ~311KB) in the repo root and grep it. The
  OASIS HTML truncates before chapter 3 in most fetchers.
