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
| `permission_check_enabled` | `[mqtt]` | `false` | Enable coarse ACL checks on MQTT publish/subscribe. Ignored when permission groups exist; topic permissions then govern access |
| `client_id_validation` | `[mqtt]` | `none` | Validate client_id against the username: `none` or `username` |

## Permissions

By default, MQTT permission checks are disabled. When `permission_check_enabled` is set to `true`, LavinMQ enforces the standard AMQP ACL model on MQTT operations:

- **PUBLISH** requires write permission on the MQTT exchange
- **SUBSCRIBE** requires read permission on the MQTT exchange and write permission on the session queue (`mqtt.<client_id>`)

When disabled, any authenticated MQTT client can publish and subscribe to any topic.

## Topic permissions

Topic permissions give MQTT clients fine-grained, per-topic authorization. Instead of a single read/write grant on the whole MQTT exchange (the standard ACL described above), each client is authorized against a set of MQTT topic filters, so a client can be allowed to publish and subscribe only within its own topic subtree.

Topic permissions activate when the first MQTT permission group is created; there is no config flag. With no groups defined, MQTT authorization behaves as described under Permissions above. Once at least one group exists, topic permissions replace the per-operation ACL check for MQTT publish and receive: authorization is decided entirely by the permission groups below, and the feature is default-deny, so a connection may only publish to or receive on topics granted by a matched rule. A user still needs a permission entry on the vhost to establish the connection in the first place.

Because it is default-deny, creating the first group switches every MQTT client to default-deny, members and non-members alike: from that point a client needs a matching rule to publish or receive, and there is no administrator bypass. Deleting the last group restores the behavior described under Permissions.

Because group membership is keyed on client id, and a client chooses its own client id at CONNECT time, **these groups only isolate clients from one another when `client_id_validation` is set to something other than `none`.** Under `none`, any authenticated client can present any client id and inherit that client's permissions.

The only mode currently available besides `none` is `username`, which forces the client id to equal the username, so `{client_id}` in a rule is just the username. Because connecting with a client id that is already in use takes over that session (see Session Takeover above), this also means a user can hold exactly one MQTT connection at a time under `username` mode. This makes per-device isolation with distinct client ids per user currently unreachable: a fleet of devices sharing one set of credentials cannot each get their own client id and their own slice of a topic subtree at the same time.

Permission groups are global objects managed via `/api/permission-groups`. A group has a member list and a set of rules, where each rule is an MQTT topic filter with `read` and `write` booleans:

```json
{
  "members": ["device-1"],
  "rules": [
    { "pattern": "chat/{client_id}/#", "read": true, "write": true }
  ]
}
```

- `members` is a list of client ids the group applies to. The entry `"*"` applies the group to every client.

Patterns use MQTT wildcards (`+`, `#`) and support the `{client_id}` substitution variable, bound per connection to the client id of the connection being authorized, never another client's. The client id must be a single topic level; if it contains `/`, `+`, or `#`, the affected `{client_id}` rules are skipped for that connection so they cannot widen into another client's subtree.

A SUBSCRIBE to a filter that overlaps no read rule is rejected with a SUBACK failure.

Read authorization is decided once, when a message is accepted into a session, not when it is delivered from that session to a client. A message accepted while read access was granted is still delivered later even if read is revoked in the meantime; only messages published after the revocation are refused and never delivered. One consequence of this is worth stating plainly because it looks odd when encountered: a client whose read access has just been revoked can be refused a new SUBSCRIBE on a topic filter, while still receiving older messages on that same filter that were already queued in its session from before the revocation.

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
