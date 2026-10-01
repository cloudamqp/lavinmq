# MQTT 5.0 testing

Strategy, current numbers, and what external verification has and has not
covered. The interop harness itself is in `MQTT5-INTEROP.md`.

---

## Strategy

The shard has exhaustive codec specs, so LavinMQ tests **behaviour**, not
framing. Per packet: a v5 client connects, exercises the feature, and we assert
the broker's response and reason codes. The v3.1.1 suite must stay green
throughout, since the v3 wire path is unchanged.

Every new spec is **first run against the unfixed code** and must fail for the
intended reason. That earned its keep twice during the subscription-options
work: the restart spec passed even with the options stripped from
`SubscriptionKey#arguments`, because the restart path reads the stored frame
rather than the key, which is exactly the compaction gap and needed its own unit
spec. And the retain-flag leak is caught in only one of the two subscriber
orders, which is why both are asserted.

## Numbers

**Shard**, 2026-06-30: 259 examples, 0 failures, `crystal tool format --check`
clean. Exact-byte vectors are the backbone, with round-trip tests focused on
properties and a version matrix for the genuinely ambiguous cases (PUBREL /
PUBCOMP remaining-length 2 versus a reason tail; CONNACK return code versus
reason code). A blanket version matrix was dropped as redundant: the
per-version `Framing` split makes "a v5 packet parsed with v3 framing"
structurally hard to even express.

**LavinMQ**, measured 2026-10-02 on `9e3d3559` (on `feat/mqtt-qos-2`):

| what | result |
|---|---|
| `crystal spec spec/mqtt` (slow included) | **477 examples, 0 failures, 0 errors, 0 pending** |
| `make test` | **2637 examples, 0 failures, 0 errors, 10 pending** |
| `make lint` | 443 inspected, 0 failures |
| `crystal tool format --check` | clean |

The pending examples are pre-existing and unrelated to MQTT (queue dead-lettering
headers, kTLS, UNIX sockets, VHost GC segments). The etcd-tagged specs were
skipped because no etcd runs locally, not because they fail. Two expiry specs
are tagged `slow`: the interval's unit is seconds, so the shortest honest test
of elapse and of reconnect-cancels-it is one second each.

v5 coverage lives in `spec/mqtt/v5/` (connect, publish, subscribe, unsubscribe,
puback/disconnect, session expiry, subscription options) plus
`spec/mqtt/publish_headers_spec.cr`, `spec/mqtt/session_spec.cr`,
`consts_spec.cr`, `subscription_key_spec.cr` and cases added to
`integrations/will_spec.cr` and `exchange_spec.cr`. Item H folds most of it back
into the integration files before the PR.

**The v5 work is green and has not regressed anything on the AMQP side.**

### A shard-bump trap worth remembering

`src/lavinmqperf/mqtt/throughput.cr` still called
`LavinMQ::MQTT::Protocol::IO.new(socket)` long after the shard made `IO`
abstract (it is concrete again since `84codes/mqtt-protocol.cr#16`). It hid because `spec/mqtt` never requires `lavinmqperf`: a targeted
MQTT spec run looked green while `make test` died at compile time. Run the full
suite after a shard bump.

## External verification, 2026-10-02

The same harness, against a debug build of `9e3d3559` with QoS 2 from #2236:
the Paho suite as published (QoS 2 no longer needs clamping), paho-mqtt 2.1.0,
mqtt.js 5.16.0 and the current mosquitto clients. The score per test is in
`MQTT5-INTEROP.md`.

- Paho v5 went from 6 to **15 of 27** passing, v3.1.1 from 3 to **7 of 9**. Every
  remaining failure maps to a documented item or a harness assumption.
- No crashes and no `Read Loop error` across 42 broker starts.
- mosquitto completes the QoS 2 handshake: PUBLISH, PUBREC, PUBREL, PUBCOMP.
- Everything the 2026-08-19 run left as "expectations to confirm" held: delivery
  QoS is the lower of publish and subscription QoS on v5 and v3.1.1, the J2 paths
  answer `0x82`, and both session-expiry rows behave.
- Three new findings: retained replays ignore the publisher's QoS (folded into
  item F), packet id 0 is accepted (item L), and the Message Expiry Interval is
  never enforced (item M).
- One harness fault: a dev LavinMQ on 1883 answered for ours, which had failed to
  bind, so a first run graded the wrong broker. The harness now runs on its own
  ports and checks its own process.

## External verification, 2026-08-19

The Eclipse Paho interoperability suite, paho-mqtt 2.1.0, mqtt.js 5.15.2 and the
mosquitto 2.1.2 clients were run against a debug build of `709a9f31`. This
closed the project's largest open risk: until then every byte vector was a
self-confirming round-trip, never checked against a real v5 client.

- No crashes, hangs or memory errors in ~75 broker starts.
- The CONNACK capability bytes decode identically in three independent codecs,
  and every rejection in the compliance table produced its promised reason code
  on the wire (`0x9B`, `0x94`, `0x9E`, `0xA1`, `0x8C`, `0x87`, `0x82`). `0x9B`
  was the QoS 2 rejection, gone since QoS 2 (#2236).
- All six v5 PUBLISH properties survive paho -> paho, paho -> mqtt.js,
  mqtt.js -> paho and mosquitto -> paho. A v5 publisher to a v3.1.1 subscriber
  drops them cleanly, and the reverse works.
- Both then-`[~]` rows were confirmed from outside: a Will at QoS 2 was accepted
  with CONNACK Success, and `maximum-packet-size 5` still got a 23-byte CONNACK
  (`maximum-packet-size 12` a 15-byte SUBACK). With QoS 2 supported, a Will at
  QoS 2 is correct as accepted.
- Items B, D, E, F, the Receive Maximum limitation and shard items N3/O2 each
  reproduced under a third-party client, so all were real and correctly
  described. B, D and E's will properties are fixed; E's Will Delay half and F
  are not.
- Three defects were new information, tracked as item J. All three are fixed.

**Re-run on 2026-10-02**, above. The delivery-QoS and DISCONNECT `0x82` rows it
left to confirm all held.

### Why the stock Paho v5 suite cannot grade this broker

At the 2026-08-19 run, 22 of its 27 tests used QoS 2 somewhere, and its client
ignored our then-advertised `maximum_qos = 1`, itself a [MQTT-3.2.2-11] violation
on the client's side. We correctly killed those connections, after which several
tests spun forever on `while len(messages) < 3`. QoS 2 (#2236) removes that
obstacle, so the unmodified suite should now grade the broker; it has not been
re-run. In the QoS-clamped copy used then, the v3.1.1 suite went 7/9,
failing only on `$`-prefixed topics matching wildcards (item O) and on a test
that needs an ACL denying a topic.
