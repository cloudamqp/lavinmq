# MQTT 5.0 remaining work

Ordered roughly easiest-first. Everything here is on
`feat/implement-mqtt5-support`. Design context is in `MQTT5-DESIGN.md`.

---

## E. Will Delay Interval

**Merge blocker.** [MQTT-3.1.2-8]; listed in `MQTT5.md`.

Will *properties* are done. What remains is
`WillProperties#will_delay_interval`, still unread. Like the subscription
options and unlike the features in the compliance table, it has **no capability
flag**, so it cannot be
advertised as unavailable: shipping without it is a real gap.

Not a tweak:

- **The will has to outlive its owner.** `@will` lives on `Client`, and all seven
  `publish_will` call sites are inside `read_loop`'s rescues, so today the will is
  always published by the dying connection's own fiber. A delayed will must fire
  after that fiber is gone.
- **Nothing downstream can publish it.** `Session` holds no reference to the
  `Broker`, and `Broker#publish` is what applies the retain store, so a retained
  delayed will routed straight through `@vhost.mqtt_exchange` would silently skip
  retention.
- **It belongs in the session-expiry timer, not a second one.** [MQTT-3.1.2-8]
  and [MQTT-3.1.3-9] make it "the delay elapses **or** the session ends, whichever
  first", cancelled by a reconnect: the exact shape of
  `Session#wait_for_client`'s existing select. Spec 3.1.3.2.2 explicitly supports
  a delay longer than the expiry as a way to be told the session expired, so
  session-end has to win.
- **Takeover has its own rule** (3.1.4): a takeover publishes the predecessor's
  will *unless* the new connection has Clean Start 0 **and** will delay > 0. We
  currently always publish on takeover.
- A delayed will need not survive a broker restart: 3.1.3.2.2 lets a server defer
  publication until after a restart, and session expiry already sets the
  precedent of persisting no deadlines.

## F. Retained messages lose v5 properties and their QoS

**Merge blocker.** [MQTT-3.3.2-17] and [MQTT-3.8.4-8]; listed in `MQTT5.md`.

`retain_store.cr#retain` takes the whole `Publish` but writes only
`packet.payload` to the file, so a retained v5 message reaches a later
subscriber with its properties stripped. The call already has
`packet.properties` in hand; the work is entirely in the store format, which
makes this the most invasive item left.

The same gap drops the publisher's QoS: `Broker#subscribe` replays every
retained message at the subscription's granted QoS, where [MQTT-3.8.4-8] wants
the lower of the two. Found by Paho's `test_subscribe_options` on 2026-10-02:
retained messages published at QoS 0, 1 and 2 all came back at 2. Pre-existing,
and v3 is affected too. Storing the QoS belongs in the same format change. `topic_tree.cr`, which backs the retain
store, also still uses `StringTokenIterator` unlike the publish-path
subscription tree.

## G. Shard release and open items

- **Merge blocker.** **Cut a tagged release.** `shard.yml` pins
  `branch: feat/mqtt5`, which cannot ship. Given the breaking changes (`MQTT5-DESIGN.md` section 5), `1.0` was the
  intended target. `main` is at `0.3.1`.
- Decide **U1**: a v5 CONNACK with a non-zero reason but `session_present = 1` is
  accepted at decode. Arguably a server-side semantic rather than a codec rule.
- Low-severity conformance gaps: **N3** packet identifier `0` accepted where a
  non-zero id is required, which is item L and blocking; **O1** zero-entry SUBSCRIBE /
  UNSUBSCRIBE / SUBACK accepted at decode; **O2** AUTH accepted on a v3
  connection; **O3** some receiver-side property value validations missing.
- **Retain Handling 3** is a Protocol Error (3.8.3.1), so v5 wants a DISCONNECT.
  The shard raises `ArgumentError` in the `TopicFilter` constructor and
  `Subscribe.from_io` maps it to `Error::PacketDecode`, the just-close case, so
  the client gets no reason code. Belongs with N3/O1/O2/O3.
- Test gaps: **N5** no malformed property-*value* test (the UTF-8 / NUL
  validation branch has zero coverage); **N6** the `consumed != total`
  intra-section property guard is untested; **U2** v3 CONNACK return-code byte
  `>= 6` rejection untested.

## H. Cross-cutting cleanup

- **Merge blocker.** **Merge the v5 specs back.** `spec/mqtt/v5/*_spec.cr` were kept separate so each
  chunk's diff stayed self-contained. Before the PR, fold each into the matching
  `spec/mqtt/integrations/*_spec.cr` and delete the v5 file, **except** the
  advertise-and-reject compliance matrix, which stays as its own standing file.
  Nothing has been merged back yet: connect, subscribe, unsubscribe, publish and
  puback/disconnect are all outstanding. The DISCONNECT/will examples in
  `puback_disconnect_spec.cr` belong next to `integrations/will_spec.cr`.
- Consider moving `build_server_capabilities` into `consts.cr` or making it a
  constant, to make it obvious it is static.
- Consider a v5 mode for the `lavinmqperf mqtt` throughput tool. It is pinned to
  `IO.v3`, so there is no load-testing path for v5 at all. Optional.

## K. PUBREC with a failure reason code

**Merge blocker.** [MQTT-4.3.3-4]; listed in `MQTT5.md`.

`Session#pubrec` sends PUBREL whatever the reason code. [MQTT-4.3.3-4] sends one
only for a reason code below `0x80`: a v5 subscriber answering PUBREC `0x80` or
greater has refused the message, which ends that delivery like a PUBACK does,
so the id should be freed without a PUBREL. v3 PUBREC has no reason code, so only
v5 is affected.

## L. PUBLISH with packet id 0

**Merge blocker.** [MQTT-2.2.1-3]; listed in `MQTT5.md`. The shard's open item **N3**.

A QoS 1 or 2 PUBLISH must carry a non-zero packet id [MQTT-2.2.1-3]. One with id
0 parses but breaks that rule, which makes it a Protocol Error: DISCONNECT `0x82`
[MQTT-4.13.1-1]. We PUBACK it instead, putting id 0 on the wire ourselves, and a
QoS 2 one would book 0 in the dedupe set. The raw `packet_id_zero` case in
`MQTT5-INTEROP.md` shows it. The check fits the shard's decoder, next to its
empty-topic rejection, so every consumer gets it.

## M. Message Expiry Interval is not enforced

**Merge blocker.** [MQTT-3.3.2-5] and [MQTT-3.3.2-6]; listed in `MQTT5.md`.

The property round-trips intact, but nothing acts on it: a message whose interval
has passed is still delivered, where [MQTT-3.3.2-5] requires deleting it for any
subscriber delivery has not started for, and a forwarded one keeps its original
value, where [MQTT-3.3.2-6] wants it reduced by the time spent waiting. Found by
Paho's `test_publication_expiry` on 2026-10-02. The delivery path already reads
the property in `Session#build_packet`, so both halves can live there, against
the message's store timestamp.

## N. The client's Receive Maximum is not honoured

**Merge blocker.** [MQTT-3.3.4-9]; listed in `MQTT5.md`.

A v5 client's CONNECT can set Receive Maximum: the most QoS 1 and QoS 2 PUBLISHes
it will take before acking them. We never read it, so the in-flight window is
`Config#max_inflight_messages` for every client. A client advertising less gets
more than it allows, and may DISCONNECT us with `0x93`. Found by Paho's
`test_flow_control1` / `test_flow_control2`. `Session#next_id` and
`Session#refresh_capacity` both bound the window by `max_inflight_messages`; the
bound becomes the lower of that and the client's value, carried on `Client` the
way `max_packet_size` is, and both have to use it.

## O. Wildcards match `$`-prefixed topics

**Must fix, not in this PR.** [MQTT-4.7.2-1]: a filter starting with `#` or `+`
must not match a topic starting with `$`. LavinMQ matches them, on v3.1.1 as
well, and Paho's `test_dollar_topics` fails on both versions. Fixing it changes
what existing `#` subscribers receive, so it gets its own PR and CHANGELOG entry.

## I. Open review finding

**Merge blocker.** [MQTT-3.1.2-24]; listed in `MQTT5.md`.

One finding from the two review rounds is still open:

- **Maximum Packet Size is only enforced for outbound PUBLISH**, the `[~]` row in
  the compliance table. `Client#send` is the single outbound choke point and is
  the place to put it, so no future packet type can forget it.

---

## Resolved

Kept as one line each so nobody re-opens them; the reasoning is in git and in
`MQTT5-DESIGN.md`.

- **B** subscription options, **D** session expiry, **E**'s will properties, and
  all of **J** are done.
- **Review round 1** (full branch, 2026-08-19), seven findings: six fixed
  (poison message and its `@unacked_*` corruption, hot-path allocation,
  `protocol_name` exhaustiveness, v3 property restore, Will QoS 2). The seventh
  is item I above.
- **Review round 2** (item D only, 2026-08-21), five findings, all fixed in one
  commit with specs, each spec first run against the unfixed code. The one that
  mattered: `durable?` followed the interval, so a durable session narrowed to 0
  by a resuming client was deleted in memory with its files removed while neither
  `apply Queue::Delete` nor `compact!` recorded it, and the original declare
  replayed into a ghost session on the next boot.
- **J** external interop findings (2026-08-19): J1 delivery QoS, J2 unexpected
  packets, J3 session expiry. All three lived on lines predating the branch, so
  none was a regression.

One round-2 finding was closed as **not fixable in this log format**: a runtime
interval change never reaches disk without a compaction, because
`apply Queue::Declare` returns early on a name it already holds and forcing a
`compact!` per reconnect is far too expensive. It is a documented limitation.
