# WebSockets

LavinMQ supports both AMQP and MQTT over WebSocket connections, enabling browser-based clients and traversal of firewalls that block non-HTTP traffic.

## Endpoints

| Path | Protocol |
|------|----------|
| `/ws` | AMQP over WebSocket (default) |
| `/ws/mqtt` | MQTT over WebSocket |
| `/mqtt` | MQTT over WebSocket (alias) |

The protocol is also negotiated via the `Sec-WebSocket-Protocol` header:

- `amqp` — AMQP 0-9-1
- `mqtt` — MQTT

If no sub-protocol is specified and the path is not explicitly MQTT, AMQP is assumed.

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

The client's remote IP address is taken from the HTTP request. When behind a reverse proxy, use proxy protocol or `X-Forwarded-For` headers to preserve the original client IP.
