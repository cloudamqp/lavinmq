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
3. Creates a bidirectional IO bridge between the WebSocket and the protocol handler

From the protocol handler's perspective, the connection behaves identically to a direct TCP connection.

## Client IP

The client's remote IP address is taken from the HTTP request's TCP connection.
