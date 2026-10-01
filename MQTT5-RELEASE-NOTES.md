# MQTT 5.0 release notes draft

Things we ship with, deliberately. These belong in the release notes and the
docs, not in a bug tracker. The two marked **Release note** are visible
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
- **Will Delay Interval ignored** (wills fire immediately). Not advertisable,
  MQTT has no capability flag for it, so this is a real gap rather than a legal
  deferral. See item E in `MQTT5-TODO.md`.
- **Retained messages lose v5 properties.** The retain store keeps only the
  payload. Item F.
- **Receive Maximum ignored.** We do not pace QoS 1 inflight against the client's
  advertised Receive Maximum, and we do not advertise our own (so clients assume
  the 65535 default). LavinMQ has its own `Config#max_inflight_messages` cap
  instead. This is the one row in the compliance table with a fully unticked
  enforcement column.
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

- **QoS 2** (advertised as Maximum QoS 1)
- **Topic aliases**, in both directions
- **Shared subscriptions**
- **Subscription identifiers**
- **Enhanced authentication**

## Interop notes

- v5 PUBLISH properties are carried in `mqtt.*` AMQP headers and are **not**
  mapped onto AMQP-native properties, so an AMQP consumer of an MQTT-published
  message sees them as headers.
