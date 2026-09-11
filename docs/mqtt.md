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
| 2 (exactly once) | Yes | Four-step handshake: PUBLISH, PUBREC, PUBREL, PUBCOMP. |

A message is delivered at the lower of the QoS it was published with and the QoS of the subscription that matched it. Publishing at QoS 2 to a QoS 0 subscriber delivers at QoS 0, and publishing at QoS 0 to a QoS 2 subscriber delivers at QoS 0 as well.

### QoS 2 exactly-once

Exactly-once rests on remembering packet IDs, not messages.

When a client publishes at QoS 2, LavinMQ records the packet ID, routes the message, and answers PUBREC. A re-sent PUBLISH carrying an ID that is still recorded is answered with another PUBREC and is not routed a second time, which is what makes the delivery exactly-once. The client's PUBREL releases the ID and is answered with PUBCOMP. A PUBREL for an ID LavinMQ is not holding is answered with PUBCOMP as well, so a client whose PUBCOMP was lost can always complete the exchange.

When LavinMQ delivers at QoS 2, the packet ID stays outstanding across both round trips. The message itself is released at PUBREC, since the subscriber owns it from that point and it must never be sent again; the ID alone is held until PUBCOMP. `max_inflight_messages` therefore bounds outstanding *packet IDs* rather than outstanding messages, and a QoS 2 subscriber reaches that bound at a lower message rate than a QoS 1 one.

The QoS 2 state on both sides is held in memory. It is not persisted and not replicated, so it survives neither a broker restart nor a failover to another node. Persisting it is planned; see [Limitations](#limitations) for what the gap costs today.

### Acknowledging with the wrong packet type

A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-2] and a QoS 1 delivery by PUBACK. Acknowledging one with the other, or sending PUBCOMP before PUBREC, is a protocol violation, so the connection is closed [MQTT-4.8.0-1] and the client's Will is published, which [MQTT-3.1.2-8] requires for any close that does not follow a DISCONNECT. A client that cannot complete the QoS 2 handshake should subscribe at QoS 1 rather than QoS 2.

A PUBREC, PUBCOMP or PUBREL for a packet ID the session never issued is treated differently: it is logged and ignored. Neither the outbound in-flight window nor the set of unreleased inbound IDs survives a broker restart, and [MQTT-4.4.0-1] has a resuming client re-send its PUBLISH and PUBREL packets, so such a client legitimately arrives with IDs the broker has no record of. That is a limitation of the broker rather than an error by the client. PUBACK is not covered by this: nothing in the protocol re-sends one, so an unknown ID there closes the connection like any other protocol violation.

## Sessions

Each MQTT session is implemented as an internal AMQP queue named `mqtt.<client_id>`. The queue holds the session's pending QoS 1 and QoS 2 messages and tracks subscriptions as bindings. This is an implementation detail of how LavinMQ stores session state — MQTT clients never see the queue directly, but it explains why session names share the `mqtt.` prefix and why durability and lifetime follow the AMQP queue model.

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

The reuse of packet IDs on redelivery is remembered in-process only, so after a broker restart the session's outstanding messages are redelivered under fresh packet IDs, and an unfinished QoS 2 exchange is forgotten entirely. If a `max-length` policy or a purge discards a message the session still owes, its packet ID is forgotten along with it; the messages that remain keep theirs.

### Session Takeover

If a client connects with a client ID that already has an active connection, the existing connection is closed and the new client takes over the session.

### Session Limits

Sessions count towards the vhost's `max-queues` [limit](vhosts.md#vhost-limits), together with AMQP queues. A session is created on the client's first SUBSCRIBE, so CONNECT still succeeds when the vhost is at the limit, but the SUBSCRIBE is answered with a SUBACK where every topic filter gets return code `0x80` (failure). Clients that already have a session can keep subscribing, since reusing a session consumes no new resource.

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
| `max_packet_size` | `[mqtt]` | `268435455` | Max MQTT packet size in bytes |
| `default_vhost` | `[mqtt]` | `/` | Default vhost for MQTT connections |
| `permission_check_enabled` | `[mqtt]` | `false` | Enable ACL checks on MQTT publish/subscribe |
| `client_id_validation` | `[mqtt]` | `none` | Validate client_id against the username: `none` or `username` |

## Permissions

By default, MQTT permission checks are disabled. When `permission_check_enabled` is set to `true`, LavinMQ enforces the standard AMQP ACL model on MQTT operations:

- **PUBLISH** requires write permission on the MQTT exchange
- **SUBSCRIBE** requires read permission on the MQTT exchange and write permission on the session queue (`mqtt.<client_id>`)

When disabled, any authenticated MQTT client can publish and subscribe to any topic.

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
- QoS 2 state is held in memory, and is neither written to disk nor replicated to followers, so it is lost on a broker restart and on a failover. Persisting it is planned as follow-up work. Outbound state, a delivery awaiting PUBREC or PUBCOMP, survives a reconnect but not a broker restart; a delivery already past PUBREC is gone with it, and since MQTT 3.1.1 §4.4 has a client re-send only PUBLISH and PUBREL, never PUBREC, that subscriber's packet ID stays outstanding for the life of the session. Inbound state, the packet IDs of QoS 2 publishes awaiting PUBREL, survives a reconnect only for a client that has a session, which means one that has subscribed at least once; for a publish-only client it is discarded on every disconnect. Whenever that state is gone, a re-sent PUBLISH is routed a second time and that message degrades to at-least-once. A re-sent PUBREL is always answered with PUBCOMP and completes normally.
- A subscriber that answers PUBREC and never PUBCOMP holds its packet ID indefinitely. Enough of them fill the session's in-flight window and delivery to that session stops until the client completes the exchanges or the session is deleted. Nothing times these out, and MQTT 3.1.1 mandates no timeout.
- Retained messages are delivered at the subscription's QoS, ignoring the QoS they were published with, because the retain store keeps only the topic and the payload. A message retained from a QoS 0 publish runs a full handshake when replayed to a QoS 2 subscriber.
- Federation and shovels operate at the AMQP layer. There is no MQTT-level bridging between brokers.
- AMQP and MQTT components cannot be cross-connected. Exchange-to-exchange bindings between the MQTT exchange and AMQP exchanges are not supported, so an AMQP publisher cannot reach MQTT subscribers (or vice versa) within the same broker.
- MQTT topics are mapped to AMQP routing keys, so AMQP routing key constraints apply (length and encoding).
