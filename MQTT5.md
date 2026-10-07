# MQTT 5.0 support - status

Status doc for the MQTT 5.0 work in LavinMQ, spanning this repo and the
`mqtt-protocol.cr` shard.

Last reconciled against the code: **2026-10-07**.

## Doc map

| file | what is in it |
|---|---|
| `MQTT5.md` (this) | status, the compliance contract, what works today, how to ship |
| `MQTT5-DESIGN.md` | architecture and the decisions behind it, both repos |
| `MQTT5-TODO.md` | remaining work, ordered |
| `MQTT5-TESTING.md` | test strategy, current numbers, external verification |
| `MQTT5-RELEASE-NOTES.md` | known limitations and behaviour changes to announce |
| `MQTT5-FUTURE.md` | deferred past this PR |
| `MQTT5-INTEROP.md` | the interop harness and how to re-run it |

---

## 1. TL;DR

MQTT 5.0 spans two repos. The **wire codec is done**; the **broker semantics are
about 92% done**.

| | branch | ahead of main | PR | state |
|---|---|---|---|---|
| `mqtt-protocol.cr` | `feat/mqtt5` | 42 commits | none | complete v5 codec, reviewed twice, needs a release tag; LavinMQ pins `2b6713f` |
| `lavinmq` | `feat/implement-mqtt5-support` | 19 commits, on `feat/mqtt-qos-2` `562ce8ca` | #2185 (draft) | foundation, PUBLISH, SUBSCRIBE/UNSUBSCRIBE, PUBACK/DISCONNECT, session expiry, subscription options, will properties, Will Delay Interval, the client's Receive Maximum, Message Expiry, retained-message properties, full compliance contract |

QoS 2 and delivery at the lower of the publish and subscription QoS come from
`feat/mqtt-qos-2` (#2236), which merges first.

A v5 client can today connect, subscribe, publish and receive with properties
intact, gets an accurate reason code on every ack, gets a session whose lifetime
it controls, gets its subscription options honoured, gets its will published
with its properties intact and after its Will Delay Interval, and gets a
spec-correct rejection for every feature we do not implement.

Green on 2026-10-07 on `b43e1537`: 2702 examples, 0 failures, lint and format
clean (`MQTT5-TESTING.md`).
External run the same day: Paho v5 20 of 27 with the suite patched to match
`paho.mqtt.python` (19 as published), v3.1.1 7 of 9 (`MQTT5-INTEROP.md`).

---

## Merge blockers

Nothing on this list may still be open when #2185 merges. The spec items are
MUSTs in MQTT 5.0 that cannot be advertised away, so shipping without them is
non-compliance, not a deferral. Detail and fix sketches are in `MQTT5-TODO.md`;
the Paho tests named are the external check for each (`MQTT5-INTEROP.md`).

**Spec violations**

- [x] **E** Will Delay Interval [MQTT-3.1.2-8]. Paho `test_will_delay`.
- [x] **F** retained messages keep their v5 properties [MQTT-3.3.2-17] and are
  replayed at the lower of the publisher's and the subscription's QoS
  [MQTT-3.8.4-8]. Paho `test_retained_message`, `test_subscribe_options`.
- [x] **M** Message Expiry Interval enforced [MQTT-3.3.2-5] and counted down
  [MQTT-3.3.2-6]. Paho `test_publication_expiry`.
- [x] **N** the client's Receive Maximum honoured [MQTT-3.3.4-9]. Paho
  `test_flow_control1`.
- [x] **I** Maximum Packet Size on every outbound packet, not just PUBLISH
  [MQTT-3.1.2-24]. Raw cases `tiny_max_packet_size`, `oversized_suback`.
- [x] **K** no PUBREL after a PUBREC with a failure reason code [MQTT-4.3.3-4].
- [x] **L** a PUBLISH with packet id 0 answered as a Protocol Error
  [MQTT-2.2.1-3]. Raw case `packet_id_zero`.

**Shipping steps**

- [ ] **G** shard release tagged and `shard.yml` pinned to it.
- [x] **H** v5 specs merged back into the integration files, by feature.
- [ ] #2236 merged, and #2185 rebased onto `main` and retargeted.
- [ ] Interop harness re-run, with every Paho failure on the list below.

**Handled outside this PR**

- [ ] **Q** our own Receive Maximum (DISCONNECT `0x93`): goes with the QoS 2
  durability branch, not #2185. Paho `test_flow_control2` times out until then.
  `MQTT5-TODO.md` item Q.

**Must fix, but not in this PR**

- [ ] **O** wildcards match `$`-prefixed topics [MQTT-4.7.2-1]. Pre-existing on
  v3.1.1 too, so fixing it changes behaviour for existing users: its own PR and
  CHANGELOG entry. Paho `test_dollar_topics`, on both versions.

**Fine to fail.** Spec-legal, and expected to keep failing in the Paho suite:
`test_server_keep_alive` (sending Server Keep Alive is optional),
`test_server_topic_alias` (a server never has to send aliases),
`test_subscribe_identifiers` and `test_shared_subscriptions` (advertised
unavailable and rejected), `test_subscribe_failure` (needs a broker configured
to deny the topic).

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
| `maximum_qos` | omitted (QoS 2 is supported) | n/a | [x] | n/a |
| `topic_alias_maximum` | `0` | PUBLISH with a Topic Alias -> DISCONNECT `0x94` TopicAliasInvalid | [x] | [x] |
| `subscription_identifier_available` | `0` | SUBSCRIBE with a Subscription Identifier -> DISCONNECT `0xA1` | [x] | [x] |
| `shared_subscription_available` | `0` | `$share/...` filter -> DISCONNECT `0x9E` | [x] | [x] |
| `retain_available` | `1` | supported (LavinMQ has a retain store) | [x] | n/a |
| `wildcard_subscription_available` | `1` | supported | [x] | n/a |
| `maximum_packet_size` | `Config#mqtt_max_packet_size` | oversized inbound rejected by the codec; oversized outbound PUBLISH dropped, any other oversized packet closes the connection | [x] | [x] |
| `receive_maximum` | omitted (default 65535) | 16-bit packet ids cannot exceed it; the *client's* Receive Maximum narrows our outbound window | [x] | n/a |

Plus: enhanced authentication (the AUTH-packet flow, [MQTT-4.12.0-1]) is rejected at
CONNECT with CONNACK `0x8C` BadAuthenticationMethod, before username/password
auth runs so the reason is accurate.

`maximum_qos` is omitted rather than sent as 2: it may only be sent as 0 or 1,
and absent means 2 (§3.2.2.3.4). Everything else in the table is implemented and
spec'd, which was the largest single risk in the project.

### Out of scope for the first release

Topic aliases, shared subscriptions, subscription identifiers, enhanced auth.
All advertised as unavailable per the table above.

Two things could **not** be deferred that way, because MQTT has no capability
flag for them: the per-filter subscription options and the Will Delay Interval
(item E), both done. Shipping without those would have been a real gap, not a
legal deferral.

---

## 3. What works today

Facts only; the reasoning is in `MQTT5-DESIGN.md`. All of it is committed on
`feat/implement-mqtt5-support` with specs.

**Foundation**
- Version negotiation on a single listener (3.1 / 3.1.1 / 5) via `io.read_connect`,
  which switches the IO's framing in place
- Version carried onto `Client`; `details_tuple` reports the real protocol name
  instead of a hardcoded `"MQTT 3.1.1"`
- All packet sizing goes through `@io.bytesize(packet)`
- The client's Maximum Packet Size is plumbed CONNECT -> `Broker#add_client` -> `Client`

**CONNECT / CONNACK**
- Full capability advertisement (`build_server_capabilities`)
- `assigned_client_identifier` echoed when we generate a client id [MQTT-3.2.2-16]
- Enhanced auth rejected `0x8C` before authentication runs
- Every CONNACK is built from a `ReasonCode`, rejections included; a v3 IO writes
  the matching return code

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
- Topic Alias rejected `0x94`, empty topic `0x82` (by the codec)
- QoS 2 (#2236) answers with v5 reason codes: PUBREC `0x10` / `0x87`, and PUBCOMP
  `0x92` for a PUBREL of an unknown packet id

**SUBSCRIBE / SUBACK**
- Granted QoS (up to 2) reported in SUBACK as a `ReasonCode`
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
- Every PUBACK, whatever its reason code, goes through the persist-ordered queue
  from #2296, so PUBACKs keep publish order

**DISCONNECT**
- The client's reason code is honoured: only `0x00` discards the will
  [MQTT-3.14.4-3]. We send nothing back.
- It may carry a new Session Expiry Interval (§3.14.2.2.2)

**Will**
- The six will properties that are also PUBLISH properties are carried onto the
  message the will becomes
- A will at QoS 2 is accepted on both versions, now that QoS 2 is supported
- Will Delay Interval: the will waits on the session (`Session#arm_will`) and is
  published at the delay or when the session ends, whichever comes first. A
  connection resuming the session cancels it (`Session#resume`). Lost on a
  broker restart (release notes)

**Session expiry**
- `Session#session_expiry_interval : UInt32` is the single input to a session's
  lifetime; `clean_session?` is gone from both `Session` and `Client`
- Derived at CONNECT: v5 reads the property, absent meaning 0 (§3.1.2.11.2);
  for v3 the shard derives it from Clean Session, 1 to 0 and 0 to `UInt32::MAX`
- DISCONNECT can narrow it, applied before the client is removed, so narrowing
  to 0 ends the session on that disconnect
- The clock runs in the session's `deliver_loop` and is fixed when the offline
  window starts (`@offline_since`/`@offline_ttl`). A connection claims the
  session at CONNECT (`Session#resume`, under the client-id lock), which holds
  expiry off until it attaches or goes away. A 0-interval session has no timer:
  `Broker#remove_client` ends it with its connection

---

## 4. Sequencing to ship

1. E, the last spec-violation merge blocker, is done. Q is handled in the QoS 2
   durability branch.
2. Interop re-run after E: 2026-10-06, Paho v5 20 of 27 (patched suite), v3.1.1
   7 of 9, every failure accounted for in `MQTT5-INTEROP.md`. Run it once more
   on the final rebased branch.
3. Tag the shard release (`1.0`), with the breaking changes from
   `MQTT5-DESIGN.md` in the changelog.
4. Repoint `shard.yml` from `branch: feat/mqtt5` to the tag, update `shard.lock`.
5. Merge the v5 specs back into the integration files (item H). Done.
6. Once #2236 merges, rebase #2185 onto `main` and retarget it.

The per-feature commit granularity is deliberate: each rejection in the
compliance table is its own commit with its spec citation and its test, which is
the unit a reviewer checks and the template for the remaining work.

Until #2236 merges, this branch is rebased onto `feat/mqtt-qos-2`, not `main`.
Two standing hazards. The `shard.yml` / `shard.lock` v5 pin conflicts as soon as
either base bumps the shard again, which step 3 ends permanently. And
`client.cr#recieve_publish`, `session.cr#get_packet` and `broker.cr#add_client`
are conflict magnets, since both branches rewrite them.

---

## Doc hygiene

- Status claims are only as fresh as the "last reconciled" date at the top.
  Re-derive from the code before trusting a checkbox.
- These docs name methods and files, not line numbers. Grep for the name.
- To verify reason codes and behaviour, keep a local untracked copy of the OASIS
  spec text (`MQTT-v5.0-spec.txt`, ~311KB) in the repo root and grep it. The
  OASIS HTML truncates before chapter 3 in most fetchers.
