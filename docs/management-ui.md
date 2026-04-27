# Management UI

LavinMQ includes a built-in web dashboard for managing and monitoring the server.

## Access

- Default URL: `http://localhost:15672/`
- Authentication: same credentials as the AMQP/HTTP API
- Users need the `management` tag (or higher) to access

## Features

The management UI provides visual access to:

- **Overview** — server version, uptime, message rates, connection/channel/queue counts
- **Connections** — list and inspect active connections, close connections
- **Channels** — list active channels with prefetch and message rate info
- **Exchanges** — declare, inspect, and delete exchanges; publish test messages
- **Queues** — declare, inspect, purge, and delete queues; get/peek messages; pause/resume
- **Streams** — inspect stream queues
- **Unacked** — view unacknowledged messages
- **Consumers** — view active consumers across the server
- **Nodes** — view node information and resource usage
- **Users** — create, edit, and delete users; manage tags
- **Vhosts** — create and delete virtual hosts
- **Policies** — create, edit, and delete policies
- **Operator Policies** — manage operator policies separately from regular policies
- **Logs** — view and stream server logs
- **Shovels** — view shovel status
- **Federation** — view federation link status

## Relationship to the API

The management UI is a client of the [Management API](management-api.md). All operations performed in the UI use the same API endpoints available programmatically.

## Configuration

The UI is served on the same port as the HTTP API:

| Config Key | Default | Description |
|-----------|---------|-------------|
| `port` in `[mgmt]` | `15672` | HTTP port |
| `tls_port` in `[mgmt]` | `15671` | HTTPS port |
| `bind` in `[mgmt]` | `127.0.0.1` | Bind address |
