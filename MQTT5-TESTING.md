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
`IO::V3`/`IO::V5` split makes "a v5 packet parsed with v3 framing" structurally
hard to even express.

**LavinMQ**, measured 2026-09-08 on `main` `02e97d70`:

| what | result |
|---|---|
| `make test SPEC=spec/mqtt` | **353 examples, 0 failures, 0 errors, 0 pending** |
| `make test TAGS=~etcd` | **2225 examples, 0 failures, 0 errors, 9 pending** |
| `make lint` | 412 inspected, 0 failures |
| `crystal tool format --check` | clean |

The 9 pending are pre-existing and unrelated to MQTT (queue dead-lettering
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
abstract. It hid because `spec/mqtt` never requires `lavinmqperf`: a targeted
MQTT spec run looked green while `make test` died at compile time. Run the full
suite after a shard bump.

## External verification, 2026-08-19

The Eclipse Paho interoperability suite, paho-mqtt 2.1.0, mqtt.js 5.15.2 and the
mosquitto 2.1.2 clients were run against a debug build of `709a9f31`. This
closed the project's largest open risk: until then every byte vector was a
self-confirming round-trip, never checked against a real v5 client.

- No crashes, hangs or memory errors in ~75 broker starts.
- The CONNACK capability bytes decode identically in three independent codecs,
  and every rejection in the compliance table produced its promised reason code
  on the wire (`0x9B`, `0x94`, `0x9E`, `0xA1`, `0x8C`, `0x87`, `0x82`).
- All six v5 PUBLISH properties survive paho -> paho, paho -> mqtt.js,
  mqtt.js -> paho and mosquitto -> paho. A v5 publisher to a v3.1.1 subscriber
  drops them cleanly, and the reverse works.
- Both then-`[~]` rows were confirmed from outside: a Will at QoS 2 was accepted
  with CONNACK Success, and `maximum-packet-size 5` still got a 23-byte CONNACK
  (`maximum-packet-size 12` a 15-byte SUBACK). The Will QoS one has since been
  fixed, so a re-run should now see CONNACK `0x9B`.
- Items B, D, E, F, the Receive Maximum limitation and shard items N3/O2 each
  reproduced under a third-party client, so all were real and correctly
  described. B, D and E's will properties are fixed; E's Will Delay half and F
  are not.
- Three defects were new information, tracked as item J. All three are fixed.

**Not re-run since.** The delivery-QoS and item I fixes landed afterwards, so
the interop doc's delivery-QoS and DISCONNECT `0x82` rows are expectations to
confirm on the next run, which should happen once E and F land.

### Why the stock Paho v5 suite cannot grade this broker

22 of its 27 tests use QoS 2 somewhere, and its client ignores our advertised
`maximum_qos = 1`, itself a [MQTT-3.2.2-11] violation on the client's side. We
correctly kill those connections, after which several tests spin forever on
`while len(messages) < 3`. A mechanically QoS-clamped copy of the suite is the
run worth reading; with QoS 2 out of the picture the v3.1.1 suite goes 7/9,
failing only on `$`-prefixed topics matching wildcards (4.7.2 is a SHOULD NOT)
and on a test that needs an ACL denying a topic.
