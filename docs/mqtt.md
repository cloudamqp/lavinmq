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
| 1 (at least once) | Yes | Messages are acknowledged with PUBACK. |
| 2 (exactly once) | Downgraded to QoS 1 | LavinMQ does not implement the full QoS 2 handshake. |

## Sessions

Each MQTT session is implemented as an internal AMQP queue named `mqtt.<client_id>`. The queue holds the session's pending QoS 1 messages and tracks subscriptions as bindings. This is an implementation detail of how LavinMQ stores session state — MQTT clients never see the queue directly, but it explains why session names share the `mqtt.` prefix and why durability and lifetime follow the AMQP queue model.

### Clean Sessions

When a client connects with `clean_session=true`:

- Any existing session for the client ID is deleted
- A new transient (auto-delete) session is created
- Subscriptions and unacknowledged messages are discarded on disconnect

### Persistent Sessions

When a client connects with `clean_session=false`:

- The session persists across disconnections
- Subscriptions are preserved
- Unacknowledged QoS 1 messages are requeued and redelivered on reconnect
- The session queue is durable

### Session Takeover

If a client connects with a client ID that already has an active connection, the existing connection is closed and the new client takes over the session.

### Message Delivery

- QoS 0 messages are not enqueued if no consumer (client) is currently connected to the session
- QoS 1 messages are stored in the session queue and tracked with packet IDs
- Unacknowledged messages are requeued when a persistent session client disconnects or a new client takes over. For clean sessions, unacknowledged messages are discarded.

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
| `max_inflight_messages` | `[mqtt]` | `65535` | Max unacknowledged messages per session |
| `max_packet_size` | `[mqtt]` | `268435455` | Max MQTT packet size in bytes |
| `default_vhost` | `[mqtt]` | `/` | Default vhost for MQTT connections |
| `client_id_validation` | `[mqtt]` | `none` | Validate client_id against the username: `none` or `username` |

## Topic Permissions

Topic permissions restrict which topics a user's MQTT clients can publish to and receive from. They are defined as permission groups on a vhost.

- With no groups on a vhost, any authenticated client can publish and subscribe to any topic
- When the first group is created, the vhost becomes default deny: a client can only publish to or receive on topics granted by a matching rule
- There is no administrator bypass
- Deleting the last group restores unrestricted topic access
- A user still needs a permission entry on the vhost to connect

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
  -d '{"pattern": "chat/{client_id}/#", "read": true, "write": true}'
```

Groups are stored per vhost in `mqtt_permissions.json` and are included in definitions export and import under the `mqtt_permissions` key.

### Upgrading

- The `permission_check_enabled` option under `[mqtt]` is removed. It applied the AMQP permission model to the MQTT exchange. Topic permissions replace it. A config that still sets the option gets a warning at startup and the option is ignored
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
- QoS 2 is downgraded to QoS 1 — the full four-step QoS 2 handshake (PUBREC/PUBREL/PUBCOMP) is not implemented.
- Federation and shovels operate at the AMQP layer. There is no MQTT-level bridging between brokers.
- AMQP and MQTT components cannot be cross-connected. Exchange-to-exchange bindings between the MQTT exchange and AMQP exchanges are not supported, so an AMQP publisher cannot reach MQTT subscribers (or vice versa) within the same broker.
- MQTT topics are mapped to AMQP routing keys, so AMQP routing key constraints apply (length and encoding).
