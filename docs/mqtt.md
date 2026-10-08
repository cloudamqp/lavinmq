# MQTT

LavinMQ implements MQTT 3.1.0 and 3.1.1 natively. MQTT clients connect directly to the dedicated MQTT port and use the protocol as-is; no plugin or external proxy is required. Internally, LavinMQ maps MQTT concepts onto its AMQP infrastructure (sessions become queues, subscriptions become bindings), but this is invisible to MQTT clients.

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

A message is delivered at the lower of the QoS it was published with and the QoS of the subscription that matched it. Publishing at QoS 2 to a QoS 0 subscriber delivers at QoS 0, and publishing at QoS 0 to a QoS 2 subscriber delivers at QoS 0 as well.

### QoS 2 exactly-once

Exactly-once rests on remembering packet IDs, not messages.

When a client publishes at QoS 2, LavinMQ records the packet ID, routes the message, and answers PUBREC once the message is persisted to disk, like a QoS 1 PUBACK. PUBACKs and PUBRECs are sent in the order the publishes arrived. A re-sent PUBLISH carrying an ID that is still recorded is answered with another PUBREC and is not routed a second time, which is what makes the delivery exactly-once. The client's PUBREL releases the ID and is answered with PUBCOMP. A PUBREL for an ID LavinMQ is not holding is answered with PUBCOMP as well, so a client whose PUBCOMP was lost can always complete the exchange.

When LavinMQ delivers at QoS 2, the packet ID stays outstanding across both round trips. The message itself is released at PUBREC, since the subscriber owns it from that point and it must never be sent again; the ID alone is held until PUBCOMP. `max_inflight_messages` therefore bounds outstanding *packet IDs* rather than outstanding messages, and a QoS 2 subscriber reaches that bound at a lower message rate than a QoS 1 one.

For durable sessions (`clean_session=false`) the QoS 2 packet IDs held in both directions are persisted in a per-session `packet_ids.log`, created the first time the session uses QoS 2, and replicated to followers, so an unfinished exchange resumes after a broker restart or a failover. Clean sessions keep this state in memory only. The persistence has a cost: an inbound PUBREC waits for two persister syncs (the routed message, then the packet ID record) and PUBCOMP waits for the release record. For a durable subscriber, an outbound PUBLISH waits for its packet ID record and PUBREL waits for the delete recorded at PUBREC. QoS 0 and QoS 1 are unaffected. With `sync = false` the waits still go through the persister and the in-sync followers but skip the fsync. Outbound QoS 2 deliveries to one durable session are sent one at a time while each waits for its record, so QoS 2 throughput per subscriber is bounded by persister latency.

A publisher may hold at most `max_awaiting_pubrel` QoS 2 packet IDs between PUBLISH and PUBREL. Going over it closes the connection.

### Acknowledging with the wrong packet type

A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-1] and a QoS 1 delivery by PUBACK. Acknowledging one with the other, or sending PUBCOMP before PUBREC, is a protocol violation, so the connection is closed [MQTT-4.8.0-1] and the client's Will is published, which [MQTT-3.1.2-8] requires for any close that does not follow a DISCONNECT. A client that cannot complete the QoS 2 handshake should subscribe at QoS 1 rather than QoS 2.

A PUBREC, PUBCOMP or PUBREL for a packet ID the session never issued is treated differently: it is logged and ignored. For a durable session the QoS 2 IDs survive a broker restart, but the broker can still meet IDs it has no record of: for a clean session, for QoS 1 (whose IDs are not persisted), and for a session that no longer exists, for example one that was deleted. [MQTT-4.4.0-1] has a resuming client re-send its PUBLISH and PUBREL packets, so such a client legitimately arrives with IDs the broker has no record of. That is a limitation of the broker rather than an error by the client. PUBACK is not covered by this: nothing in the protocol re-sends one, so an unknown ID there closes the connection like any other protocol violation.

## Sessions

Each MQTT session is implemented as an internal AMQP queue named `mqtt.<client_id>`. The queue holds the session's pending QoS 1 and QoS 2 messages and tracks subscriptions as bindings. This is an implementation detail of how LavinMQ stores session state — MQTT clients never see the queue directly, but it explains why session names share the `mqtt.` prefix and why durability and lifetime follow the AMQP queue model.

Every connection has a session, created at CONNECT whether or not the client ever subscribes [MQTT-3.1.2-4]. It holds the client's inbound QoS 2 state as well as its subscriptions and pending messages. Deleting the session queue, for example over the HTTP API, closes the client's connection; a reconnect gets a new, empty session.

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

### Session Takeover

If a client connects with a client ID that already has an active connection, the existing connection is closed and the new client takes over the session.

### Session Limits

Sessions count towards the vhost's `max-queues` [limit](vhosts.md#vhost-limits), together with AMQP queues. Since every connection has a session, a clean session counts while its client is connected and a persistent one counts until it is deleted, whether or not the client subscribes. A CONNECT that would create a session past the limit is answered with a CONNACK carrying return code 3 (server unavailable) and the socket is closed. A persistent client reconnecting to its existing session, or a client taking over its own connection, is still accepted, since that consumes no new resource.

Creating the session is part of accepting the connection, so it is not subject to `permission_check_enabled`: any user allowed to connect to the vhost gets one, and `max-queues` is what bounds them.

AMQP clients and the HTTP API cannot create queues with the `mqtt.` prefix, but a definitions import can. If a queue that is not an MQTT session already has the name `mqtt.<client_id>`, a CONNECT with that client ID is answered with a CONNACK carrying return code 2 (identifier rejected) and the socket is closed, until that queue is removed.

### Message Delivery

- QoS 0 messages are not enqueued if no consumer (client) is currently connected to the session
- QoS 1 and QoS 2 messages are stored in the session queue and tracked with packet IDs
- Unacknowledged messages are requeued when a persistent session client disconnects or a new client takes over, and keep their packet IDs for the redelivery. For clean sessions, unacknowledged messages are discarded.

## Connection Limits

The `max-connections` vhost limit applies to MQTT connections as well as AMQP ones. When the vhost is at its cap, a CONNECT is answered with a CONNACK carrying return code 3 (server unavailable) and the socket is closed. A client reconnecting with a client ID that already has an active connection is still accepted, because [session takeover](#session-takeover) replaces that connection instead of adding one. See [Connections](connections.md#connection-limits).

## Retained Messages

Retained messages are stored per topic and delivered to new subscribers upon subscription.

- When a message is published with the retain flag set, it is stored in the retain store
- When a client subscribes to a topic, any matching retained message is delivered immediately
- Publishing a retained message with an empty payload clears the retained message for that topic
- Retained messages are replicated across cluster nodes

## Topic Matching

MQTT topics use `/` as a level separator. LavinMQ supports the standard MQTT wildcards:

- `+` — matches exactly one topic level
- `#` — matches zero or more topic levels (must be the last character)

Examples:
- `sensor/+/temperature` matches `sensor/room1/temperature` but not `sensor/room1/sub/temperature`
- `sensor/#` matches `sensor/room1/temperature` and `sensor/room1/sub/anything`

## MQTT-AMQP Bridge

Internally, MQTT is implemented on top of LavinMQ's AMQP infrastructure:

- A dedicated MQTT exchange handles topic routing
- Each MQTT session is an AMQP queue
- MQTT subscriptions are bindings on the MQTT exchange
- MQTT topic separators (`/`) map directly to AMQP routing key segments
- Message properties are mapped between protocols (e.g., `delivery_mode` maps to QoS, `mqtt.retain` header tracks retain flag)

## Configuration

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `bind` | `[mqtt]` | `127.0.0.1` | Bind address for MQTT |
| `port` | `[mqtt]` | `1883` | MQTT listen port |
| `tls_port` | `[mqtt]` | `8883` | MQTT over TLS port |
| `unix_path` | `[mqtt]` | (empty) | Unix socket path |
| `max_inflight_messages` | `[mqtt]` | `65535` | Max outstanding packet IDs per session, must be at least `1`. A QoS 2 delivery holds its ID until PUBCOMP |
| `max_awaiting_pubrel` | `[mqtt]` | `1024` | Max QoS 2 packet IDs a publisher may hold between PUBLISH and PUBREL, must be at least `1`. Going over it closes the connection, as MQTT 3.1.1 has no way to reject a single publish |
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

- Publish: the connection needs a write rule for the topic. A denied publish is dropped, a QoS 1 publish is still acknowledged, and the connection stays open
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
- The `permission_check_enabled` option under `[mqtt]` is unchanged. When it is set, a publish needs write permission on the `mqtt.default` exchange, and a subscribe needs read permission on that exchange and write permission on the `mqtt.<client_id>` session queue. A client that fails this check is disconnected. The topic check runs after it. The session queue itself is created at CONNECT without a permission check, see [Session Limits](#session-limits)
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

- Only MQTT 3.1.0 and 3.1.1 are supported. MQTT 5 features (session expiry interval, shared subscriptions, topic aliases, message expiry, user properties, response topics) are not available.
- QoS 2 state of a clean session is held in memory only, so it is lost with the session. For a durable session the packet IDs are persisted and replicated, see [QoS 2 exactly-once](#qos-2-exactly-once). The state is gone for a clean session and for a deleted session. In that case a re-sent PUBLISH is routed a second time and that message degrades to at-least-once. A re-sent PUBREL is always answered with PUBCOMP and completes normally.
- A subscriber that answers PUBREC and never PUBCOMP holds its packet ID indefinitely. Enough of them fill the session's in-flight window and delivery to that session stops until the client completes the exchanges or the session is deleted. Nothing times these out, and MQTT 3.1.1 mandates no timeout.
- Retained messages are delivered at the subscription's QoS, ignoring the QoS they were published with, because the retain store keeps only the topic and the payload. A message retained from a QoS 0 publish runs a full handshake when replayed to a QoS 2 subscriber.
- Federation and shovels operate at the AMQP layer. There is no MQTT-level bridging between brokers.
- AMQP and MQTT components cannot be cross-connected. Exchange-to-exchange bindings between the MQTT exchange and AMQP exchanges are not supported, so an AMQP publisher cannot reach MQTT subscribers (or vice versa) within the same broker.
- MQTT topics are mapped to AMQP routing keys, so AMQP routing key constraints apply (length and encoding).
