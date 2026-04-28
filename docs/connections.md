# Connections

A connection is a TCP link between a client and LavinMQ. Both AMQP and MQTT clients establish connections.

## AMQP Connections

AMQP connections follow the standard 0-9-1 lifecycle. See [AMQP](amqp.md) for protocol details.

## MQTT Connections

MQTT connections follow the MQTT 3.1.x CONNECT/CONNACK handshake. See [MQTT](mqtt.md) for protocol details.

## Heartbeats

AMQP connections use heartbeats to detect dead peers. The heartbeat interval is negotiated during connection setup.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `heartbeat` | `[amqp]` | `300` | Heartbeat interval in seconds. Set to `0` to disable (not recommended). |

During connection setup, the client and server each propose a heartbeat interval and the lower of the two non-zero values wins. The server then expects to see traffic from the client at least that often; if nothing arrives within that interval plus a 5-second grace period, the connection is closed as dead. Setting the interval to `0` disables heartbeats entirely, which means dead peers are never detected and is not recommended.

## Proxy Protocol

LavinMQ supports HAProxy PROXY protocol for preserving client IP addresses behind load balancers.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `unix_proxy_protocol` | `[amqp]` | `1` | PROXY protocol version on Unix sockets, applies to all protocols (0 = disabled) |
| `tcp_proxy_protocol` | `[amqp]` | `0` | PROXY protocol version on TCP, applies to AMQP and MQTT (0 = disabled) |

Supports PROXY protocol v1 (text) and v2 (binary).

## Low Disk Space

When free disk space drops below `3 * segment_size` or below `free_disk_min`, `basic.publish` returns a `precondition_failed` channel error until resources recover.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `free_disk_min` | `[main]` | `0` | Minimum free disk space in bytes. Publishing is blocked when free space drops below this value. |
| `free_disk_warn` | `[main]` | `0` | Warning threshold in bytes. Emits warnings when free space drops below this value. |
| `segment_size` | `[main]` | `8388608` | Segment file size (bytes). Publishing is also blocked when free space drops below `3 * segment_size`. |

## Connection Limits

Concurrent connections to a vhost can be capped with the `max-connections` vhost limit. Once the cap is reached, new connections to that vhost are refused with `connection.close` carrying reply code 530 (`NOT_ALLOWED`); existing connections are unaffected.

| Limit | Default | Description |
|-------|---------|-------------|
| `max-connections` | (none) | Maximum concurrent connections per vhost. A negative value removes the cap. |

Set via the management API (`PUT /api/vhost-limits/:vhost/max-connections`) or `lavinmqctl set_vhost_limits`. See [Vhosts](vhosts.md#vhost-limits).

## TCP Tuning

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `tcp_nodelay` | `[main]` | `false` | Disable Nagle's algorithm |
| `tcp_keepalive` | `[main]` | `60:10:3` | Idle, interval, probes (colon-separated) |
| `tcp_recv_buffer_size` | `[main]` | (system) | TCP receive buffer |
| `tcp_send_buffer_size` | `[main]` | (system) | TCP send buffer |
| `socket_buffer_size` | `[main]` | `16384` | Application socket buffer (bytes) |

## Unix Domain Sockets

LavinMQ can listen on Unix domain sockets for all protocols:

| Config Key | Section | Description |
|-----------|---------|-------------|
| `unix_path` | `[amqp]` | AMQP Unix socket path |
| `unix_path` | `[mqtt]` | MQTT Unix socket path |
| `unix_path` | `[mgmt]` | HTTP Unix socket path |

Unix sockets have PROXY protocol v1 enabled by default to receive connection metadata.
