# MQTT 5.0 remaining work

Ordered roughly easiest-first. Everything here is on
`feat/implement-mqtt5-support`. Design context is in `MQTT5-DESIGN.md`.

---

## E. Will Delay Interval

Will *properties* are done. What remains is
`WillProperties#will_delay_interval`, still unread. Like the subscription
options and unlike QoS 2, it has **no capability flag**, so it cannot be
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

## F. Retained messages lose v5 properties

`retain_store.cr#retain` takes the whole `Publish` but writes only
`packet.payload` to the file, so a retained v5 message reaches a later
subscriber with its properties stripped. The call already has
`packet.properties` in hand; the work is entirely in the store format, which
makes this the most invasive item left. `topic_tree.cr`, which backs the retain
store, also still uses `StringTokenIterator` unlike the publish-path
subscription tree.

## G. Shard release and open items

- **Cut a tagged release.** `shard.yml` pins `branch: feat/mqtt5`, which cannot
  ship. Given the breaking changes (`MQTT5-DESIGN.md` section 5), `1.0` was the
  intended target. `main` is at `0.3.1`.
- Decide **U1**: a v5 CONNACK with a non-zero reason but `session_present = 1` is
  accepted at decode. Arguably a server-side semantic rather than a codec rule.
- Low-severity conformance gaps, none blocking: **N3** packet identifier `0`
  accepted where a non-zero id is required; **O1** zero-entry SUBSCRIBE /
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

- **Merge the v5 specs back.** `spec/mqtt/v5/*_spec.cr` were kept separate so each
  chunk's diff stayed self-contained. Before the PR, fold each into the matching
  `spec/mqtt/integrations/*_spec.cr` and delete the v5 file, **except** the
  advertise-and-reject compliance matrix, which stays as its own standing file.
  Nothing has been merged back yet: connect, subscribe, unsubscribe, publish and
  puback/disconnect are all outstanding. The DISCONNECT/will examples in
  `puback_disconnect_spec.cr` belong next to `integrations/will_spec.cr`.
- Consider moving `build_server_capabilities` into `consts.cr` or making it a
  constant, to make it obvious it is static.
- Consider a v5 mode for the `lavinmqperf mqtt` throughput tool. It is pinned to
  `IO::V3`, so there is no load-testing path for v5 at all. Optional.

## I. Open review finding

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
