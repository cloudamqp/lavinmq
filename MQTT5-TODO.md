# MQTT 5.0 remaining work

Open items first; E is kept as a design record. Everything here is on
`feat/implement-mqtt5-support`. Design context is in `MQTT5-DESIGN.md`.

---

## E. Will Delay Interval

**Done** (2026-10-06). [MQTT-3.1.2-8], [MQTT-3.1.3-9]. Paho `test_will_delay`
passes since the 2026-10-06 run. Kept here as the design record.

`WillProperties#will_delay_interval` has **no capability flag**, so it could
not be advertised as unavailable: shipping without it would have been a real
gap.

### Model

A connection that closes without DISCONNECT `0x00` leaves its will pending on
the session, due at close + delay. It is published at whichever comes first:
the deadline, or the session ending. A new connection for the client id that
resumes the session before then cancels it. §3.1.4's takeover rule needs no
code of its own:

| Takeover case | What the model does | Result |
|---|---|---|
| delay 0 | published at close, as today | published |
| Clean Start 1 | `add_client_locked` deletes the session: it ended | published |
| Clean Start 0, delay > 0 | the new connection resumes the session | cancelled |

A pending will need not survive a broker restart: §3.1.2.5 lets a server defer
publication until after a restart, and session expiry already persists no
deadlines. A graceful shutdown (`Session#close` without `delete`) therefore
drops it, the same as a crash or a cluster failover.

### Design (approved 2026-10-06)

The constraint that shapes it: `Session` holds no reference to the `Broker`,
and `Broker#publish` is what applies the retain store, so a delayed will sent
straight to `@vhost.mqtt_exchange` would silently lose its retain flag.

- **`PendingWill`**, a new `record`: `packet : Protocol::Publish`,
  `broker : Broker`, `deadline : Time::Instant`. The session publishes it with
  `will.broker.publish(will.packet, @name)` and never holds a `Client` or a
  `Broker` of its own. Rejected: the session holding the dead `Client` (keeps
  a closed connection reachable) and a `Broker` field on every `Session` (a
  permanent cycle for one use).
- **`Client#publish_will` splits in two.** `will_packet : Protocol::Publish?`
  runs today's permission checks and builds the packet with
  `will_properties`. `publish_will` publishes it at once when the delay is 0,
  and otherwise hands it to `Session#arm_will`. The seven call sites in
  `read_loop`'s rescues stay as they are. Permissions are therefore checked at
  close, not when the will fires: a write permission revoked during the delay
  does not stop it.
- **Delay 0 keeps today's path** (every v3 client, most v5 ones): published
  synchronously by the dying read fiber, so existing ordering is unaffected.
- **Cancel in `Broker#add_client_locked`** (`Session#resume`), not in
  `Session#client=`. The spec's trigger is a connection *opened*, and
  attach happens only in `Client#run`, after CONNACK: a deadline passing in
  that window would publish a will the spec forbids. The client-id lock and
  `prev_client.close` joining the old read fiber guarantee a takeover's will
  is set before it is cancelled. That needs *every* `Client#close` to join it,
  not only the first: a client already closed by OAuth expiry or the
  `deliver_loop` rescue used to return at once, and its will could then arm
  after the cancel.
- **One wait, two deadlines.** `wait_for_client` selects on reconnect, the
  expiry deadline (unless 0 or `UInt32::MAX`; none while a connection has
  claimed the session) and the will deadline (when pending).
  The will firing publishes it and returns to the loop, so the expiry
  deadline must not move: `Session#client=` fixes `@offline_since` and
  `@offline_ttl` at detach, or when a claim ends without attaching
  (construction, for a restored session). Re-reading
  the interval per wait would let a resuming connection's narrower interval,
  set before it attaches, expire the session it is about to resume.
- **Session end publishes it, on the session's own fiber.** Every delete
  (expiry, `auto_delete` at disconnect, a Clean Start 1 takeover, an HTTP API
  delete, a vhost delete) ends `deliver_loop`, which publishes after the loop
  if `@deleted`. `expire` runs on that fiber and needs nothing extra. This
  keeps the publish out of `Session#delete`, which can run under the
  definitions lock. `arm_will` on a session already deleted (its read fiber
  ran after `deliver_loop` exited) publishes at once instead.

Covered without special cases: Session Expiry 0 with a delay publishes at
close; a delay longer than the expiry publishes at expiry; DISCONNECT `0x04`
with a delay is delayed; DISCONNECT `0x00` still discards [MQTT-3.14.4-3].

### Specs

In `spec/mqtt/integrations/will_spec.cr` since item H. Delays are
whole seconds, so these use 1-2s. 2, 5 and 7 were run against the unfixed
code first.

1. Delay 2: nothing at ~1s, the will at ~2s.
2. Reconnect with Clean Start 0 inside the delay: never published.
3. Session Expiry 1, delay 10: published at ~1s.
4. Session Expiry 0, delay 10: published at close.
5. Takeover with Clean Start 1: published. Takeover with Clean Start 0 and a
   delay: not published.
6. A retained delayed will reaches a later subscriber as retained.
7. Delay 1, Session Expiry 2: the session is gone at ~2s, not ~3s.
8. Deleting the session through the HTTP API publishes a pending will.
9. A second `Client#close` returns only once the will is armed.
10. Narrowing the interval while offline (what a resume does before attach)
    does not move the expiry deadline.
11. Deleting a connected session publishes its delayed will.

2 and 5 disconnect the new connection normally before asserting: while it is
attached no will can fire, cancelled or not.

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

- **Done** (2026-10-07). **The v5 specs are folded by feature.**
  `spec/mqtt/v5/` is gone: each v5 `describe` block lives in the spec file of
  the feature it tests, the advertise-and-reject matrix is
  `integrations/server_capabilities_spec.cr`, and session expiry and
  subscription options moved as files of their own. The same 518 MQTT
  examples run before and after; only the six capability examples changed
  their `describe`.
- Consider moving `build_server_capabilities` into `consts.cr` or making it a
  constant, to make it obvious it is static.
- Consider a v5 mode for the `lavinmqperf mqtt` throughput tool. It is pinned to
  `IO.v3`, so there is no load-testing path for v5 at all. Optional.
- `Client#read_loop` logs a client that closes without DISCONNECT as `ERROR
  Client unexpectedly closed connection`. That is routine client behaviour (36 of
  them in the 2026-10-05 interop run, wills and test teardown); WARN or INFO
  would keep ERROR for real faults.

## P. DUP set on a first delivery

Not a merge blocker. When `deliver_acked` finds no free packet id it requeues
the message it just shifted, unsent, and `MessageStore` marks every requeued
message redelivered. Its first real send then goes out with DUP 1 and is counted
as a redelivery, where DUP 0 means a first attempt (§3.3.1.1). The capacity gate
keeps this rare: it needs the window to shrink between the gate and `next_id`.
A remembered packet id is the precise "sent before" signal, which is what
`Session#expired_undelivered?` uses.

## Resolved

Kept as one line each so nobody re-opens them; the reasoning is in git and in
`MQTT5-DESIGN.md`.

- **Q** our own Receive Maximum: CONNACK advertises `max_awaiting_pubrel`
  (#2367's QoS 2 cap) and going over it is DISCONNECT `0x93`. QoS 1 counts towards
  the client's quota but is not enforced. The cap is per session while Receive
  Maximum is per connection, so ids a client forgot across a reconnect stay held
  and count; exact per-connection accounting waits until that is seen.
- **B** subscription options, **D** session expiry, **E** will properties and
  Will Delay Interval (design record above), and all of **J** are done.
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
- **O** wildcards no longer match `$`-prefixed topics [MQTT-4.7.2-1], live or
  retained, on both versions. Fixed against `main` in #2366 (closes #2313), so
  this branch gets it on the rebase.
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
- **Review round 3** (full branch, 2026-10-07), two findings, both session
  expiry races predating E, fixed with specs that failed first. A 0-interval
  session expired from its own fiber, so a connection that yielded before
  attaching (a slow CONNACK write) lost its fresh session; it now waits for
  `Broker#remove_client`. And `expire` ran outside the client-id lock, so a
  timer that fired as a reconnect arrived deleted the session after CONNACK
  said it was present; `Session#resume` now claims it until the connection
  attaches or goes away.
- **J** external interop findings (2026-08-19): J1 delivery QoS, J2 unexpected
  packets, J3 session expiry. All three lived on lines predating the branch, so
  none was a regression.

One round-2 finding was closed as **not fixable in this log format**: a runtime
interval change never reaches disk without a compaction, because
`apply Queue::Declare` returns early on a name it already holds and forcing a
`compact!` per reconnect is far too expensive. It is a documented limitation.
