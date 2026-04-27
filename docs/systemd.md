# SystemD Integration

LavinMQ supports SystemD socket activation and can be managed as a SystemD service.

## Socket Activation

SystemD socket activation allows SystemD to listen on the configured ports and pass the sockets to LavinMQ on startup. This enables:

- Zero-downtime restarts (SystemD holds the sockets while LavinMQ restarts)
- Lazy startup (LavinMQ starts only when a connection arrives)
- Privilege separation (SystemD binds privileged ports, LavinMQ runs unprivileged)

### Socket Names

| Config Key | Default | Description |
|-----------|---------|-------------|
| `amqp_systemd_socket_name` | `lavinmq-amqp.socket` | AMQP socket unit name |
| `http_systemd_socket_name` | `lavinmq-http.socket` | HTTP socket unit name |

### Setup

Create socket unit files (e.g., `/etc/systemd/system/lavinmq-amqp.socket`):

```ini
[Socket]
ListenStream=5672

[Install]
WantedBy=sockets.target
```

And a service unit (e.g., `/etc/systemd/system/lavinmq.service`):

```ini
[Unit]
Description=LavinMQ
After=network.target

[Service]
ExecStart=/usr/bin/lavinmq --config /etc/lavinmq/lavinmq.ini
Restart=on-failure
User=lavinmq
Group=lavinmq

[Install]
WantedBy=multi-user.target
```

## PID File

LavinMQ can write its PID to a file on startup for process management:

```ini
[main]
pidfile = /var/run/lavinmq/lavinmq.pid
```

The PID file is removed on graceful shutdown.

## Graceful Restart

Send `SIGTERM` to gracefully shut down LavinMQ. SystemD will restart it automatically if `Restart=on-failure` is set. With socket activation, connections are preserved during the restart window.
