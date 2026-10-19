# WebSockets

LavinMQ supports both AMQP and MQTT over WebSocket connections, enabling browser-based clients and traversal of firewalls that block non-HTTP traffic.

## Endpoints

| Path | Protocol |
|------|----------|
| `/mqtt` | MQTT over WebSocket |
| `/ws/mqtt` | MQTT over WebSocket |
| any other path | AMQP over WebSocket |

WebSocket upgrade requests are accepted on the management HTTP port. Protocol selection follows this order:

1. If the `Sec-WebSocket-Protocol` header is set, the protocol is selected from it:
   - `amqp` (or any token starting with `amqp`, case-insensitive) — AMQP 0-9-1
   - `mqtt` (or any token starting with `mqtt`, case-insensitive) — MQTT
2. Otherwise, the path is used: `/mqtt` and `/ws/mqtt` route to MQTT; any other path routes to AMQP.

Conventionally, clients use `/ws` for AMQP, but the path itself is not significant when the sub-protocol header is set.

## Port

WebSocket connections use the same HTTP port as the management UI and API:

| Config Key | Default |
|-----------|---------|
| `port` in `[mgmt]` | `15672` |
| `tls_port` in `[mgmt]` | `15671` (WSS) |

## How It Works

WebSocket connections are proxied to the internal AMQP or MQTT server. The WebSocket handler:

1. Upgrades the HTTP connection to a WebSocket
2. Detects the protocol from the sub-protocol header or URL path
3. Exposes the WebSocket as a byte stream to the protocol handler

From the protocol handler's perspective, the connection behaves identically to a direct TCP connection. Only binary frames carry protocol data; a message may be split over several frames. Ping frames are answered with pong frames, on top of the protocol's own heartbeat or keepalive.

## Closing

- When the broker closes a connection, after an MQTT `DISCONNECT`, an AMQP `connection.close`, a close from the management UI, API or `lavinmqctl`, a `tcp_send_timeout` or at shutdown, it sends a close frame with status `1000` and then closes the TCP connection. The close frame is skipped if a frame is still being written to a client that stopped reading, and its write is capped at 1 second, so closing never waits for such a client.
- When the client sends a close frame, the broker replies with a close frame carrying the same status code and closes the TCP connection. Data received before the close frame, such as an MQTT `DISCONNECT` sent together with it, is handled first, so a clean MQTT disconnect doesn't publish the client's Will.
- A text frame closes the connection with status `1003` (unsupported data).

## Client IP

The client's remote IP address is taken from the HTTP request's TCP connection.
