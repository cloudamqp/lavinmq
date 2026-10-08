# MQTT

LavinMQ implements MQTT 3.1.0, 3.1.1 and 5.0 natively. MQTT clients connect directly to the dedicated MQTT port and use the protocol as-is; no plugin or external proxy is required. Internally, LavinMQ maps MQTT concepts onto its AMQP infrastructure (sessions become queues, subscriptions become bindings), but this is invisible to MQTT clients.

## Ports

| Protocol | Default Port | Config Key |
|----------|-------------|------------|
| MQTT | 1883 | `mqtt_port` |
| MQTTS | 8883 | `mqtts_port` |
| MQTT over WebSocket | via HTTP port (15672) | `http_port` |

Unix domain sockets are also supported via `unix_path` in the `[mqtt]` section. See [Configuration](#configuration) below.

## QoS Levels

| QoS | Supported | Behavior |
|-----|-----------|----------|
| 0 (at most once) | Yes | Fire and forget. Messages are not persisted for the session. |
| 1 (at least once) | Yes | Incoming publishes are acknowledged with PUBACK after the affected durable data is synced, unless synchronization is disabled. |
| 2 (exactly once) | Yes | Four-step handshake: PUBLISH, PUBREC, PUBREL, PUBCOMP. |

For incoming QoS 1 publishes, PUBACKs are sent in publish order after the broker finishes handling the publish and syncing its affected durable message and retained-message files. In a cluster, the broker also waits for the in-sync followers to acknowledge the replicated writes after synchronization. This uses the same persistence mechanism as [publisher confirms](publisher-confirms.md#durability-and-synchronization).

Waiting for disk synchronization makes QoS 1 throughput depend on disk latency and the number of publishes the client keeps in flight. QoS 0 does not wait for this synchronization. Setting `sync=false` in `[main]` (or using `--no-sync`) skips local disk synchronization; followers also skip synchronization if it is disabled in their own configuration. A PUBACK then provides no disk durability guarantee on those nodes.

A PUBACK acknowledges the broker's handling of a publish, not delivery to a subscriber. Session lifetime and subscriptions still determine whether messages are retained for later delivery; a publish denied by topic permissions is acknowledged and dropped as described below.

A message is delivered at the lower of the QoS it was published with and the QoS of the subscription that matched it. Publishing at QoS 2 to a QoS 0 subscriber delivers at QoS 0, and publishing at QoS 0 to a QoS 2 subscriber delivers at QoS 0 as well. Retained messages follow the same rule, see [Retained Messages](#retained-messages).

### QoS 2 exactly-once

Exactly-once rests on remembering packet IDs, not messages.

When a client publishes at QoS 2, LavinMQ records the packet ID, routes the message, and answers PUBREC once the message is persisted to disk, like a QoS 1 PUBACK. PUBACKs and PUBRECs are sent in the order the publishes arrived. A re-sent PUBLISH carrying an ID that is still recorded is answered with another PUBREC and is not routed a second time, which is what makes the delivery exactly-once. The client's PUBREL releases the ID and is answered with PUBCOMP. A PUBREL for an ID LavinMQ is not holding is answered with PUBCOMP as well, so a client whose PUBCOMP was lost can always complete the exchange.

When LavinMQ delivers at QoS 2, the packet ID stays outstanding across both round trips. The message itself is released at PUBREC, since the subscriber owns it from that point and it must never be sent again; the ID alone is held until PUBCOMP. `max_inflight_messages` therefore bounds outstanding *packet IDs* rather than outstanding messages, and a QoS 2 subscriber reaches that bound at a lower message rate than a QoS 1 one.

For durable sessions (`clean_session=false`) the QoS 2 packet IDs held in both directions are persisted in a per-session `packet_ids.log`, created the first time the session uses QoS 2, and replicated to followers, so an unfinished exchange resumes after a broker restart or a failover. Clean sessions keep this state in memory only. The persistence has a cost: an inbound PUBREC waits for two persister syncs (the routed message, then the packet ID record) and PUBCOMP waits for the release record. For a durable subscriber, an outbound PUBLISH waits for its packet ID record and PUBREL waits for the delete recorded at PUBREC. QoS 0 and QoS 1 are unaffected. With `sync = false` the waits still go through the persister and the in-sync followers but skip the fsync. Outbound QoS 2 deliveries to one durable session are sent one at a time while each waits for its record, so QoS 2 throughput per subscriber is bounded by persister latency.

A publisher may hold at most `max_awaiting_pubrel` QoS 2 packet IDs between PUBLISH and PUBREL. Going over it closes the connection. MQTT 5.0 clients are told the limit as Receive Maximum and get a DISCONNECT with reason `0x93` (Receive Maximum exceeded).

### Packet ID 0

A QoS 1 or QoS 2 PUBLISH must carry a non-zero packet ID [MQTT-2.3.1-1]. One with packet ID 0 is a protocol violation: it is not routed, the connection is closed and the client's Will is published.

### Acknowledging with the wrong packet type

A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-3] and a QoS 1 delivery by PUBACK. Acknowledging one with the other, or sending PUBCOMP before PUBREC, is a protocol violation, so the connection is closed [MQTT-4.13.1-1] and the client's Will is published, which [MQTT-3.1.2-8] requires for any close that does not follow a DISCONNECT. A client that cannot complete the QoS 2 handshake should subscribe at QoS 1 rather than QoS 2.

A PUBREC, PUBCOMP or PUBREL for a packet ID the session has no record of is treated differently: the connection stays open. A PUBREL is answered with PUBCOMP, as described above, and a PUBREC with PUBREL, the answers that let the client release the ID; a PUBCOMP is ignored. A PUBREC for an ID that a requeued message is waiting to be re-sent under is not answered, since releasing that ID would make the client take the re-sent PUBLISH for a new message. For a durable session the QoS 2 IDs survive a broker restart, but the broker can still meet IDs it has no record of: for a clean session, for QoS 1 (whose IDs are not persisted), and for a session that no longer exists, for example one that was deleted. [MQTT-4.4.0-1] has a resuming client re-send its PUBLISH and PUBREL packets, so such a client legitimately arrives with IDs the broker has no record of. That is a limitation of the broker rather than an error by the client. PUBACK is not covered by this: nothing in the protocol re-sends one, so an unknown ID there closes the connection like any other protocol violation.

## Sessions

Each MQTT session is implemented as an internal AMQP queue named `mqtt.<client_id>`. The queue holds the session's pending QoS 1 and QoS 2 messages and tracks subscriptions as bindings. This is an implementation detail of how LavinMQ stores session state — MQTT clients never see the queue directly, but it explains why session names share the `mqtt.` prefix and why durability and lifetime follow the AMQP queue model.

Every connection has a session, created at CONNECT whether or not the client ever subscribes [MQTT-3.1.2-4] [MQTT-3.1.2-6]. It holds the client's inbound QoS 2 state as well as its subscriptions and pending messages. Deleting the session queue, for example over the HTTP API, closes the client's connection; a reconnect gets a new, empty session.

### Clean Sessions

When a client connects with `clean_session=true`:

- Any existing session for the client ID is deleted
- A new transient (auto-delete) session is created
- Subscriptions and unacknowledged messages are discarded on disconnect

### Persistent Sessions

When a client connects with `clean_session=false`:

- The session persists across disconnections
- Subscriptions are preserved
- Unacknowledged QoS 1 and QoS 2 messages are requeued and redelivered on reconnect, under the packet IDs the client already holds and with the `dup` flag set
- QoS 2 deliveries that reached PUBREC but not PUBCOMP have no message left to resend, so their PUBREL is re-sent instead, under the original packet ID [MQTT-4.4.0-1]
- The session queue is durable

QoS 1 packet IDs are not persisted. A QoS 1 message is re-sent under its original ID across a reconnect within the same process, but after a broker restart it is redelivered under a new ID. For a durable session the QoS 2 IDs are restored after a restart: an unacknowledged QoS 2 message is re-sent under its original ID with `dup` set, and one that is past PUBREC has its PUBREL re-sent. See [QoS 2 exactly-once](#qos-2-exactly-once). If a `max-length` policy or a purge discards a message the session still owes, a QoS 1 packet ID is forgotten along with it, while a QoS 2 one stays held and its PUBREL is sent, since the client may hold that ID until then; the messages that remain keep theirs.

### Session Expiry (MQTT 5.0)

MQTT 5.0 splits `clean_session` in two. Clean Start decides whether an existing session is discarded at CONNECT, and the Session Expiry Interval decides how long the session outlives its connection:

- `0`, or no interval at all, ends the session when the connection closes (§3.1.2.11.2). A v5 client that wants a persistent session must send an interval, unlike a 3.1.1 client with `clean_session=false`
- A non-zero interval keeps the session for that many seconds after the connection closes. A reconnect within the interval resumes it and stops the clock; the next disconnect starts a new one
- `4294967295` (`0xFFFFFFFF`) never expires, like a 3.1.1 persistent session
- A reconnecting client's interval replaces the stored one, and a DISCONNECT may name a new interval. Changing `0` to non-zero on DISCONNECT is a protocol error (§3.14.2.2.2)

Clean Start 1 with a non-zero interval discards the old session and persists the new one. The interval is stored with the session, but the time left is not: after a broker restart a session gets its full interval again from boot.

### Session Takeover

If a client connects with a client ID that already has an active connection, the existing connection is closed and the new client takes over the session. An MQTT 5.0 client on the old connection gets a DISCONNECT with `0x8E` (Session taken over) first [MQTT-3.1.4-3]. The old connection's will is published as described in [Will Messages](#will-messages), so it is held back only when the new connection resumes the session within the will's delay.

### Session Limits

Sessions count towards the vhost's `max-queues` [limit](vhosts.md#vhost-limits), together with AMQP queues. Since every connection has a session, a clean session counts while its client is connected and a persistent one counts until it is deleted, whether or not the client subscribes. A CONNECT that would create a session past the limit is answered with a CONNACK carrying return code 3 (server unavailable) and the socket is closed. A persistent client reconnecting to its existing session, or a client taking over its own connection, is still accepted, since that consumes no new resource.

Creating the session is part of accepting the connection, so it is not subject to `permission_check_enabled`: any user allowed to connect to the vhost gets one, and `max-queues` is what bounds them.

AMQP clients and the HTTP API cannot create queues with the `mqtt.` prefix, but a definitions import can. If a queue that is not an MQTT session already has the name `mqtt.<client_id>`, a CONNECT with that client ID is answered with a CONNACK carrying return code 2 (identifier rejected) and the socket is closed, until that queue is removed.

### Message Delivery

- QoS 0 messages are not enqueued if no consumer (client) is currently connected to the session
- QoS 1 and QoS 2 messages are stored in the session queue and tracked with packet IDs
- Unacknowledged messages are requeued when a persistent session client disconnects or a new client takes over, and keep their packet IDs for the redelivery. For clean sessions, unacknowledged messages are discarded.

## Will Messages

A client can register a will at CONNECT. LavinMQ publishes it when the connection closes without a DISCONNECT, for example on a network failure, a keepalive timeout or a protocol error [MQTT-3.1.2-8]. A DISCONNECT with reason `0x00` discards it; on MQTT 5.0, DISCONNECT with `0x04` (Disconnect with Will Message) or any error reason code publishes it.

On MQTT 5.0 the will can carry the same properties as a PUBLISH, and they reach the subscribers. It can also carry a Will Delay Interval. LavinMQ then waits that many seconds before publishing it, or until the session ends, whichever comes first. A connection that resumes the session within the delay cancels it [MQTT-3.1.3-9]. So:

- A session that ends with its connection (Session Expiry Interval `0`) publishes the will at once
- A will delay longer than the session expiry publishes the will when the session expires, which a client can use to be told about the expiry
- A takeover with Clean Start `1` ends the old session, so the old connection's will is published; a takeover with Clean Start `0` resumes it and cancels a delayed will

A will waiting out its delay is held in memory only, so a broker restart or a failover drops it.

## Connection Limits

The `max-connections` vhost limit applies to MQTT connections as well as AMQP ones. When the vhost is at its cap, a CONNECT is answered with a CONNACK carrying return code 3 (server unavailable) and the socket is closed. A client reconnecting with a client ID that already has an active connection is still accepted, because [session takeover](#session-takeover) replaces that connection instead of adding one. See [Connections](connections.md#connection-limits).

## Retained Messages

Retained messages are stored per topic and delivered to new subscribers upon subscription.

- When a message is published with the retain flag set, it is stored in the retain store, with its QoS, its MQTT 5.0 properties and its publish time
- When a client subscribes to a topic, any matching retained message is delivered immediately, at the lower of the QoS it was retained with and the QoS of the subscription [MQTT-3.8.4-8]
- A retained message's Message Expiry Interval counts down from when it was published, and an expired one is not delivered
- Publishing a retained message with an empty payload clears the retained message for that topic
- Retained messages are replicated across cluster nodes

The retain store files changed format in this version: each message is written as `<md5>.rmsg`. Files in the old payload-only `.msg` format are still read, as QoS 1, and are replaced the next time their topic is retained. A downgrade to an older version does not read `.rmsg` files, so it loses every message retained since the upgrade.

On MQTT 5.0 a subscription can also control retained messages, see [Subscription Options](#subscription-options).

## Topic Matching

MQTT topics use `/` as a level separator. LavinMQ supports the standard MQTT wildcards:

- `+` — matches exactly one topic level
- `#` — matches zero or more topic levels (must be the last character)

Examples:
- `sensor/+/temperature` matches `sensor/room1/temperature` but not `sensor/room1/sub/temperature`
- `sensor/#` matches `sensor/room1/temperature` and `sensor/room1/sub/anything`

## MQTT 5.0

A client chooses the protocol version in its CONNECT, and versions can be mixed on the same broker. A message published by a 5.0 client reaches a 3.1.1 subscriber without its properties.

### Properties

The PUBLISH properties Payload Format Indicator, Message Expiry Interval, Content Type, Response Topic, Correlation Data and User Properties are passed through to 5.0 subscribers unchanged, with user properties in their original order. The Payload Format Indicator is passed on but not validated.

The Message Expiry Interval is enforced: a message that expires before delivery starts is dropped, and a delivered one carries the time it has left [MQTT-3.3.2-5] [MQTT-3.3.2-6]. Expired messages are removed when they reach the head of the session, so until then they still count towards the session's message count and `max-length`.

### Subscription Options

Each topic filter in a 5.0 SUBSCRIBE carries three options, which are kept with the subscription, also across a restart:

- **No Local**: the client does not receive its own publishes on this subscription [MQTT-3.8.3-3]
- **Retain As Published**: deliveries keep the retain flag they were published with, instead of having it cleared [MQTT-3.3.1-12]
- **Retain Handling**: `0` sends matching retained messages at every SUBSCRIBE, `1` only when the subscription is new, `2` never [MQTT-3.3.1-9] [MQTT-3.3.1-10] [MQTT-3.3.1-11]

### Reason Codes

Acknowledgements carry a reason code. PUBACK and PUBREC answer `0x10` (No matching subscribers) when nothing received the message and `0x87` (Not authorized) when a permission check denied it. The PUBCOMP and PUBREL answering a PUBREL or PUBREC for an [unknown packet ID](#qos-2-exactly-once) carry `0x92` (Packet Identifier not found), and a PUBREC with a failure reason code for an unknown ID is not answered. SUBACK and UNSUBACK carry one code per topic filter. On a protocol error, LavinMQ sends a DISCONNECT with the reason before closing the connection, for example `0x81` (Malformed Packet) or `0x82` (Protocol Error). A keepalive timeout closes with `0x8D` (Keep Alive timeout).

A CONNECT that is refused is answered with the 5.0 reason code, for example `0x88` (Server unavailable) where the [session](#session-limits) or [connection](#connection-limits) limits above say return code 3, and `0x85` (Client Identifier not valid) where they say return code 2.

### Flow Control and Packet Size

- **Receive Maximum**: LavinMQ sends a client no more unacknowledged QoS 1 and QoS 2 messages than the client's Receive Maximum, or `max_inflight_messages` if that is lower [MQTT-3.3.4-9]. LavinMQ advertises `max_awaiting_pubrel` as its own Receive Maximum. A client with more QoS 2 publishes awaiting PUBREL is disconnected with `0x93`; QoS 1 publishes count towards the client's quota but LavinMQ does not enforce that part
- **Maximum Packet Size**: LavinMQ never sends a client a packet larger than the client's Maximum Packet Size [MQTT-3.1.2-24]. A PUBLISH that is too large is dropped for that client only; any other packet that would be too large closes the connection, so a client with a limit below the size of the CONNACK (about 21 bytes) cannot connect. LavinMQ advertises `max_packet_size` as its own limit
- **Assigned Client Identifier**: a client that connects with an empty client ID gets one assigned, and it is returned in the CONNACK

### Unsupported Features

These optional features are advertised as unavailable in the CONNACK, and a client that uses one anyway is disconnected with the matching reason code:

| Feature | CONNACK advertises | Reason code when used |
|---------|--------------------|-----------------------|
| Topic aliases | `Topic Alias Maximum` 0 | `0x94` (Topic Alias invalid) |
| Shared subscriptions (`$share/`) | `Shared Subscription Available` 0 | `0x9E` (Shared Subscriptions not supported) |
| Subscription identifiers | `Subscription Identifiers Available` 0 | `0xA1` (Subscription Identifiers not supported) |
| Enhanced authentication (AUTH) | none | CONNACK `0x8C` (Bad authentication method) |

LavinMQ does not send Reason Strings, User Properties on acknowledgements, Server Keep Alive or a Server Reference.

## MQTT-AMQP Bridge

Internally, MQTT is implemented on top of LavinMQ's AMQP infrastructure:

- A dedicated MQTT exchange handles topic routing
- Each MQTT session is an AMQP queue
- MQTT subscriptions are bindings on the MQTT exchange
- MQTT topic separators (`/`) map directly to AMQP routing key segments
- Message properties are mapped between protocols (e.g., `delivery_mode` maps to QoS, `mqtt.retain` header tracks retain flag)
- MQTT 5.0 PUBLISH properties are carried as `mqtt.*` headers, not mapped onto AMQP properties, so an AMQP consumer sees them as headers. An AMQP queue bound to an MQTT topic does not apply the Message Expiry Interval

## Configuration

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `bind` | `[mqtt]` | `127.0.0.1` | Bind address for MQTT |
| `port` | `[mqtt]` | `1883` | MQTT listen port |
| `tls_port` | `[mqtt]` | `8883` | MQTT over TLS port |
| `unix_path` | `[mqtt]` | (empty) | Unix socket path |
| `max_inflight_messages` | `[mqtt]` | `65535` | Max outstanding packet IDs per session, must be at least `1`. A QoS 2 delivery holds its ID until PUBCOMP |
| `max_awaiting_pubrel` | `[mqtt]` | `1024` | Max QoS 2 packet IDs a publisher may hold between PUBLISH and PUBREL, must be at least `1`. Going over it closes the connection, as MQTT 3.1.1 has no way to reject a single publish. Advertised to MQTT 5.0 clients as Receive Maximum |
| `max_packet_size` | `[mqtt]` | `268435455` | Max MQTT packet size in bytes |
| `default_vhost` | `[mqtt]` | `/` | Default vhost for MQTT connections |
| `client_id_validation` | `[mqtt]` | `none` | Validate client_id against the username: `none` or `username` |

## Topic Permissions

Topic permissions restrict which topics a user's MQTT clients can publish to and receive from. They are defined as permission groups on a vhost.

- A client can only publish to or receive on topics granted by a matching rule
- Every vhost starts with a group named `default`. Its member is `*` and its single rule `#` grants read and write, so any authenticated client can publish and subscribe to any topic. A vhost created by a definitions import is the exception, see [Definitions](#definitions)
- To lock a vhost down, delete the `default` group or narrow its rule. Groups added next to an intact `default` group grant nothing new, because the `default` group already grants everything
- A vhost with no groups denies every topic
- There is no administrator bypass
- A user still needs a permission entry on the vhost to connect
- The `permission_check_enabled` option under `[mqtt]` adds the AMQP permission check in front of the topic check, see [Upgrading](#upgrading)

### Groups

A permission group has a name, a list of members and a list of rules.

```json
{
  "name": "devices",
  "vhost": "/",
  "members": ["alice"],
  "rules": [
    { "identifier": "own-chat", "pattern": "chat/{client_id}/#", "read": true, "write": true }
  ]
}
```

- Group names consist of alphanumerics, hyphens and underscores, at most 255 characters
- Members are usernames. Every connection that authenticates as a member gets the group's rules, so a user with many devices is one member
- The member `"*"` applies the group to every authenticated user
- A user in several groups gets the rules of all of them
- A member name must match the user name as shown in the connections list. For OAuth users that is the claim selected by `preferred_username_claims`
- Each rule has an identifier, a topic filter pattern and `read` and `write` flags
- Rule identifiers consist of alphanumerics and hyphens and are unique within the group. The HTTP API addresses a rule by its identifier

### Patterns

Patterns are MQTT topic filters. They use the `+` and `#` wildcards with subscription semantics, so a rule for `a/#` also grants `a`.

A pattern can contain `{client_id}` as a whole topic level. It is replaced with the client ID of the connection being checked, so one rule gives each of a user's devices its own subtree:

- `chat/{client_id}/#` grants `chat/thermo-1/#` to a device connected as `thermo-1`
- The same rule grants `chat/gate/#` to a device connected as `gate`
- A client ID that contains `/`, `+` or `#` never matches a topic level, so rules with `{client_id}` never match for that connection

The client ID has no other role. Membership is decided by the authenticated username, which the client cannot choose.

### Enforcement

- Publish: the connection needs a write rule for the topic. A denied publish is dropped, a QoS 1 publish is still acknowledged with PUBACK and a QoS 2 one with PUBREC (on MQTT 5.0 with reason `0x87`, Not authorized), and the connection stays open
- Subscribe: always accepted. Read is enforced when a message is accepted into the session, so a subscription to a filter the user cannot read receives no messages. This matches Mosquitto
- Will: the connection needs a write rule for the will topic, otherwise the will is dropped
- Denials are logged at debug level
- Changes to groups take effect immediately, also for connected clients

Read is checked once per message, when the message is accepted into the session, not when it is delivered to the client. A message accepted before read was revoked is still delivered after the revocation. Messages published after the revocation are not.

### Sessions

A session is checked with the username of the client that last attached to it. This also covers messages that arrive while the device is offline.

- The session stores that username on disk, so a session restored after a restart keeps its member rules until the device reconnects
- When another user takes over the session (see [Session Takeover](#session-takeover)), new messages are checked against the new user
- Messages already queued under the previous user are still delivered

### HTTP API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/mqtt/permission-groups` | List group summaries on all vhosts |
| GET | `/api/mqtt/permission-groups/{vhost}` | List group summaries on a vhost |
| GET | `/api/mqtt/permission-groups/{vhost}/{name}` | Get one group summary |
| PUT | `/api/mqtt/permission-groups/{vhost}/{name}` | Create an empty group (no request body) |
| DELETE | `/api/mqtt/permission-groups/{vhost}/{name}` | Delete a group with all its members and rules |
| GET | `/api/mqtt/permission-groups/{vhost}/{name}/members` | List the members of a group |
| PUT | `/api/mqtt/permission-groups/{vhost}/{name}/members/{username}` | Add a member |
| DELETE | `/api/mqtt/permission-groups/{vhost}/{name}/members/{username}` | Remove a member |
| GET | `/api/mqtt/permission-groups/{vhost}/{name}/rules` | List the rules of a group |
| PUT | `/api/mqtt/permission-groups/{vhost}/{name}/rules/{identifier}` | Add or replace a rule; body `{"pattern": "...", "read": bool, "write": bool}` |
| DELETE | `/api/mqtt/permission-groups/{vhost}/{name}/rules/{identifier}` | Remove a rule |

- All routes require the administrator tag
- A group summary has `name`, `vhost`, `member_count` and `rule_count`
- The members route returns one object per member: `{"username": "..."}`
- The group list routes and the members route accept `page`, `page_size`, `name` with optional `use_regex=true`, `sort`, `sort_reverse` and `columns`, like the other list endpoints
- The rules route returns the full rule list with `identifier`, `pattern`, `read` and `write` per rule

Example: allow every user to use only its own device subtrees under `chat/`.

```sh
curl -u admin:pw -X PUT localhost:15672/api/mqtt/permission-groups/%2f/devices
curl -u admin:pw -X PUT localhost:15672/api/mqtt/permission-groups/%2f/devices/members/%2A
curl -u admin:pw -X PUT localhost:15672/api/mqtt/permission-groups/%2f/devices/rules/own-chat \
  -H 'Content-Type: application/json' \
  -d '{"pattern": "chat/{client_id}/#", "read": true, "write": true}'
```

Permission changes are saved to disk before becoming active. If saving fails, the request fails and the previous permissions remain active, including when attempting to revoke access.

### Definitions

Groups are stored per vhost in `mqtt_permissions.json` and are included in definitions export and import under the `mqtt_permissions` key. The file is written when the vhost is created, so it always holds the groups that are in effect, including the `default` group.

An import adds groups and replaces groups with the same name. It never deletes a group, like for users and policies, so importing definitions into an existing vhost keeps its `default` group. To lock that vhost down, delete or narrow its `default` group. `load_definitions` also skips groups whose name already exists on the vhost.

A vhost created by an import only gets the `default` group if the definitions have no `mqtt_permissions` key. With the key, which every export has, the vhost gets only the groups the definitions list for it, so a vhost exported without groups is imported locked down. Definitions from before topic permissions have no such key, and their vhosts stay open.

Definitions imports save and apply permission groups together per vhost; a failure on one vhost does not undo changes already saved for another.

Definitions generated from a data directory include the groups in `mqtt_permissions.json`. If the file is missing, the generator includes the `default` group, which the server creates when it loads the vhost.

### Upgrading

- A vhost without `mqtt_permissions.json` gets the `default` group, which is written to that file at once, so an upgraded server keeps every topic open until an operator locks a vhost down
- The `permission_check_enabled` option under `[mqtt]` is unchanged. When it is set, a publish needs write permission on the `mqtt.default` exchange, and a subscribe needs read permission on that exchange and write permission on the `mqtt.<client_id>` session queue. The topic check runs after this check. A client that fails it is disconnected, except on MQTT 5.0, where a denied QoS 1 or 2 publish is answered with reason `0x87` (Not authorized) and a denied subscribe with a SUBACK of `0x87`, and the connection stays open. A denied QoS 0 publish on MQTT 5.0 gets a DISCONNECT with `0x87`. The session queue itself is created at CONNECT without a permission check, see [Session Limits](#session-limits)
- A persistent session that existed before the upgrade has no stored username until its device reconnects once. Until then it is checked against `"*"` rules only

## Authentication

MQTT clients authenticate using the CONNECT packet's username and password fields. These are validated against the same authentication chain as AMQP (local users, OAuth2). For OAuth2, the password field carries the JWT token.

The username field can include a vhost using the format `vhost:username`. If no colon is present, `default_vhost` is used.

### Client ID Validation

By default any client_id is accepted. Since the client_id is chosen freely by the client, it cannot be trusted for identity purposes on its own. The `client_id_validation` setting ties it to the authenticated username:

- `username`: the client_id must be equal to the username

A CONNECT with a non-conforming client_id is rejected with return code 2 (identifier rejected) and the connection is closed. An empty client_id is automatically assigned a conforming one. When the username includes a vhost (`vhost:username`), the client_id is validated against the username part only.

Note that connecting with a client_id already in use takes over that session, so `username` mode limits each user to one connection at a time.

## Limitations

- MQTT 5.0 topic aliases, shared subscriptions, subscription identifiers and enhanced authentication are not supported, see [Unsupported Features](#unsupported-features).
- A will waiting out its Will Delay Interval, and the time left on a session's expiry, are not persisted: a broker restart drops the will and restarts the session's expiry clock.
- QoS 2 state of a clean session is held in memory only, so it is lost with the session. For a durable session the packet IDs are persisted and replicated, see [QoS 2 exactly-once](#qos-2-exactly-once). The state is gone for a clean session and for a deleted session. In that case a re-sent PUBLISH is routed a second time and that message degrades to at-least-once. A re-sent PUBREL is always answered with PUBCOMP and completes normally.
- A subscriber that answers PUBREC and never PUBCOMP holds its packet ID indefinitely. Enough of them fill the session's in-flight window and delivery to that session stops until the client completes the exchanges or the session is deleted. Nothing times these out, and MQTT 3.1.1 mandates no timeout.
- Federation and shovels operate at the AMQP layer. There is no MQTT-level bridging between brokers.
- AMQP and MQTT components cannot be cross-connected. Exchange-to-exchange bindings between the MQTT exchange and AMQP exchanges are not supported, so an AMQP publisher cannot reach MQTT subscribers (or vice versa) within the same broker.
- MQTT topics are mapped to AMQP routing keys, so AMQP routing key constraints apply (length and encoding).
