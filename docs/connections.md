# Connections

A connection is a TCP link between a client and LavinMQ. Both AMQP and MQTT clients establish connections.

## AMQP Connections

AMQP connections follow the standard 0-9-1 lifecycle. See [AMQP](amqp.md) for protocol details.

## MQTT Connections

MQTT connections follow the MQTT 3.1.x CONNECT/CONNACK handshake. See [MQTT](mqtt.md) for protocol details.

## Heartbeats

AMQP connections use heartbeats to detect dead peers. The heartbeat interval is negotiated during connection setup.

- Default: 300 seconds
- Configurable via `heartbeat` in the `[amqp]` config section
- The lower non-zero value between client and server is used
- Setting heartbeat to 0 disables it (not recommended)
- If no data is received within two heartbeat intervals, the connection is closed

## Proxy Protocol

LavinMQ supports HAProxy PROXY protocol for preserving client IP addresses behind load balancers.

| Config Key | Default | Description |
|-----------|---------|-------------|
| `unix_proxy_protocol` | `1` | PROXY protocol version on Unix sockets (0 = disabled) |
| `tcp_proxy_protocol` | `0` | PROXY protocol version on TCP (0 = disabled) |

Supports PROXY protocol v1 (text) and v2 (binary).

## Blocked Connections

When server resources are constrained (disk space below `free_disk_min`), LavinMQ sends `connection.blocked` to all publishing AMQP connections. Publishing is paused until resources recover, at which point `connection.unblocked` is sent.

## Connection Limits

Connections can be limited per vhost using vhost limits. When the limit is reached, new connections to that vhost are refused.

## TCP Tuning

| Config Key | Default | Description |
|-----------|---------|-------------|
| `tcp_nodelay` | `false` | Disable Nagle's algorithm |
| `tcp_keepalive` | `60,10,3` | Idle, interval, probes |
| `tcp_recv_buffer_size` | (system) | TCP receive buffer |
| `tcp_send_buffer_size` | (system) | TCP send buffer |
| `socket_buffer_size` | `16384` | Application socket buffer (bytes) |

## Unix Domain Sockets

LavinMQ can listen on Unix domain sockets for all protocols:

| Config Key | Description |
|-----------|-------------|
| `unix_path` (in `[amqp]`) | AMQP Unix socket path |
| `mqtt_unix_path` (in `[mqtt]`) | MQTT Unix socket path |
| `http_unix_path` (in `[mgmt]`) | HTTP Unix socket path |

Unix sockets have PROXY protocol v1 enabled by default to receive connection metadata.

## Graceful Shutdown

On SIGTERM or SIGINT, LavinMQ:

1. Stops accepting new connections
2. Closes existing connections gracefully (sends connection.close)
3. Flushes data to disk
4. Exits
