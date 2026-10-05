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

## G. Shard release and open items

- **Merge blocker.** **Cut a tagged release.** `shard.yml` pins
  `branch: feat/mqtt5`, which cannot ship. Given the breaking changes (`MQTT5-DESIGN.md` section 5), `1.0` was the
  intended target. `main` is at `0.3.1`.
- Decide **U1**: a v5 CONNACK with a non-zero reason but `session_present = 1` is
  accepted at decode. Arguably a server-side semantic rather than a codec rule.
- Low-severity conformance gaps: **O1** zero-entry SUBSCRIBE /
  UNSUBSCRIBE / SUBACK accepted at decode; **O2** AUTH accepted on a v3
  connection; **O3** some receiver-side property value validations missing.
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

## P. DUP set on a first delivery

Not a merge blocker. When `deliver_acked` finds no free packet id it requeues
the message it just shifted, unsent, and `MessageStore` marks every requeued
message redelivered. Its first real send then goes out with DUP 1 and is counted
as a redelivery, where DUP 0 means a first attempt (§3.3.1.1). The capacity gate
keeps this rare: it needs the window to shrink between the gate and `next_id`.
A remembered packet id is the precise "sent before" signal, which is what
`Session#expired_undelivered?` uses.

## O. Wildcards match `$`-prefixed topics

**Must fix, not in this PR.** [MQTT-4.7.2-1]: a filter starting with `#` or `+`
must not match a topic starting with `$`. LavinMQ matches them, on v3.1.1 as
well, and Paho's `test_dollar_topics` fails on both versions. Fixing it changes
what existing `#` subscribers receive, so it gets its own PR and CHANGELOG entry.

## Resolved

Kept as one line each so nobody re-opens them; the reasoning is in git and in
`MQTT5-DESIGN.md`.

- **B** subscription options, **D** session expiry, **E**'s will properties, and
  all of **J** are done.
- **F** the retain store keeps each message's QoS, v5 properties and publish time,
  so a replay keeps its properties, goes out at the lower QoS and honours expiry.
- **M** an expired message is deleted unless its delivery started, and a delivered
  one carries the remaining interval.
- **Retain Handling 3** is a Protocol Error in the shard since
  `84codes/mqtt-protocol.cr#19`, so a v5 client gets DISCONNECT `0x82`.
- **N** the outbound window is the lower of the client's Receive Maximum and
  `Config#max_inflight_messages`.
- **I** Maximum Packet Size is enforced on every outbound packet, in `Client#send`
  and before the CONNACK.
- **L** (the shard's **N3**) packet id 0 is a Protocol Error, raised by the shard's
  decoder for every packet that carries an id.
- **K** a PUBREC with a failure reason code ends the delivery without a PUBREL.
- **Review round 1** (full branch, 2026-08-19), seven findings: six fixed
  (poison message and its `@unacked_*` corruption, hot-path allocation,
  `protocol_name` exhaustiveness, v3 property restore, Will QoS 2). The seventh
  was item I.
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
