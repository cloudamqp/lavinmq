# MQTT 5.0 release notes draft

Things we ship with, deliberately. These belong in the release notes and the
docs, not in a bug tracker. The entries marked **Release note** are visible
behaviour changes for existing users.

---

## Behaviour changes on existing setups

- **Clean Start 0 with no Session Expiry Interval now gives a session that ends
  with its connection**, where it used to persist forever. This is the normative
  reading of 3.1.2.11.2 ("if the Session Expiry Interval is absent the value 0 is
  used ... the Session ends when the Network Connection is closed") and what
  mosquitto does. The same section contains a *non-normative* aside calling it
  "equivalent to setting CleanSession to 0 in 3.1.1", i.e. persistent. That
  sentence is the one a user will cite when their session stops persisting, so
  following the normative text is recorded here as a decision. A client that
  wants the old behaviour sends an explicit interval. v3.1.1 is unaffected.
  **Release note.**
- **Delivery QoS is now the minimum of the publish and subscription QoS on
  v3.1.1 too**, not just v5. Spec-correct ([MQTT-3.8.4-6] in 3.1.1) and decided
  deliberately rather than version-gating the publish hot path, but it is
  visible: a QoS 0 publish to a QoS 1 subscription is no longer upgraded, so it
  is no longer stored while that subscriber is offline. A spec titled "[LavinMQ
  non-normative]" used to assert the old behaviour. **Release note.**
- **Retained messages replay at the lower of their own and the subscription's
  QoS**, on v3.1.1 too, where they used to replay at the subscription's
  [MQTT-3.8.4-8]. One retained before the upgrade replays as QoS 1 at most, the
  highest any older version accepted, until it is retained again.
  **Release note.**
- **The retain store has a new file format.** Each retained message is written
  as `<md5>.rmsg` with its QoS, properties and publish time; the old
  payload-only `<md5>.msg` files are still read, and are replaced when their
  topic is next retained. A downgrade to an older version does not read
  `.rmsg`, so it loses every retained message published since the upgrade.
  **Release note.**

## Session expiry caveats

- **The clock restarts on a broker restart.** The interval persists, the deadline
  does not, so a session restored from definitions gets its full interval again
  from boot. MQTT leaves this implementation-defined.
- **An interval changed on a reconnect reaches disk only at the next definitions
  compaction.** The definitions log has no update frame for an existing queue, so
  a restart can restore the interval the session was first declared with. Bounded
  by a value the same client chose earlier, and the clock resets on restart
  anyway. Narrowing to 0 is unaffected: it ends the session, and the deletion
  frame *is* persisted.

## Known gaps

- **Replacing a subscription has a message-loss window.** [MQTT-3.8.4-4] says
  Application Messages MUST NOT be lost when a subscription is replaced, but
  `Session#subscribe` unbinds before it binds and each call can reach disk and
  therefore yield, so a publish landing in between is lost. Pre-existing (it
  already triggered on a QoS change) and now reachable by changing a subscription
  option too. Bind-then-unbind does not fix it:
  `SubscriptionTree#unsubscribe` removes by (filter, session) and would delete
  the entry the bind just wrote, so a correct fix needs the tree's removal to
  become options-aware.
- **An UNSUBSCRIBE of a non-canonically stored binding can be undone by a
  restart.** Definitions replay cancels a stored `Queue::Bind` only on exact
  argument-table equality, while `Session#unsubscribe` sends the canonical table
  from `SubscriptionKey#arguments`. A binding an operator created with, say,
  `{mqtt.qos: 2i32}` is removed in memory but restored on the next boot.
  Pre-existing and QoS-only until now; the two option keys multiply the
  permutations.
- **A will waiting out its Will Delay Interval is lost on a broker restart.**
  It is held in memory only, so a shutdown or failover during the delay drops
  it, which §3.1.2.5 permits. Same precedent as session expiry deadlines.
- **A delayed will is authorised when the connection closes**, not when it is
  published: revoking the user's write permission during the Will Delay
  Interval does not stop it.
- **Deleting a session publishes its pending will.** A session deleted over the
  HTTP API, or with its vhost, has ended, so a will still waiting out its delay
  is published at once [MQTT-3.1.2-8].
- **`#` and `+` match `$`-prefixed topics** [MQTT-4.7.2-1], on v3.1.1 too. Fixed
  separately in #2366, because it changes what existing subscribers receive
  (item O). Drop this line once #2366 is merged and this branch is rebased.
- **A client whose Maximum Packet Size is smaller than our CONNACK cannot
  connect.** The CONNACK with the capability set is about 21 bytes, and
  [MQTT-3.1.2-24] forbids sending it, so the connection is closed without one.
- **Expired messages are deleted lazily.** A message past its Message Expiry
  Interval is deleted when it reaches the head of the session, not when it
  expires, so until then it still counts towards the session's message count,
  its disk use and `max-length`. A subscriber never receives it either way. An
  AMQP queue bound to an MQTT topic does not expire it at all: the interval is
  kept as an `mqtt.*` header, not mapped onto AMQP `expiration`.
- **No Receive Maximum of our own.** We do not advertise one, so clients assume
  the 65535 default, which 16-bit packet ids cannot exceed anyway. The client's
  Receive Maximum is honoured: the outbound window is the lower of it and
  `Config#max_inflight_messages`. A client with more in flight towards us is
  never disconnected with `0x93` (item Q, handled with QoS 2 durability).
- **Payload Format Indicator is not validated.** Spec 3.3.2.3.2 only says a
  server MAY check that a payload declared as UTF-8 really is, so we never answer
  `0x99` PayloadFormatInvalid. Validating means a String allocation plus a UTF-8
  scan on the publish hot path for an optional check.
- **No Reason Strings or User Properties on ack packets.** [MQTT-3.1.2-29] makes
  them illegal on anything but PUBLISH / CONNACK / DISCONNECT when the client set
  Request Problem Information to 0, and we never send them, so
  `request_problem_information` needs no plumbing.
- **UNSUBSCRIBE is not permission checked** (it never was, on v3 either), so
  UNSUBACK never carries `0x87` NotAuthorized.

## Advertised as unavailable

Each is rejected with the reason code in the compliance table, and each is
standalone follow-up work (`MQTT5-FUTURE.md`).

- **Topic aliases**, in both directions
- **Shared subscriptions**
- **Subscription identifiers**
- **Enhanced authentication**

## Interop notes

- v5 PUBLISH properties are carried in `mqtt.*` AMQP headers and are **not**
  mapped onto AMQP-native properties, so an AMQP consumer of an MQTT-published
  message sees them as headers.
