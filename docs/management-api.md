# Management API

LavinMQ provides an HTTP API for managing and monitoring the server. The API uses JSON for request and response bodies.

## Access

- Default URL: `http://localhost:15672/api/`
- Authentication: HTTP Basic Auth (same credentials as AMQP)
- TLS: Available on port 15671 when TLS is configured

Users need the `management` tag (or higher) to access the API.

## Endpoints

### Overview and Nodes

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/overview` | Server overview (version, rates, listeners, node info) |
| GET | `/api/nodes` | List nodes |
| GET | `/api/nodes/:name` | Node details |
| GET | `/api/whoami` | Current authenticated user |
| GET | `/api/aliveness-test/:vhost` | Health check (declares an `aliveness-test` queue, publishes and retrieves a test message) |
| GET | `/api/extensions` | List supported server extensions |

### Vhosts

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/vhosts` | List all vhosts |
| GET | `/api/vhosts/:name` | Vhost details |
| PUT | `/api/vhosts/:name` | Create vhost |
| DELETE | `/api/vhosts/:name` | Delete vhost |
| GET | `/api/vhosts/:name/permissions` | List permissions for a vhost |

### Vhost Limits

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/vhost-limits` | List all vhost limits |
| GET | `/api/vhost-limits/:vhost` | Limits for a vhost |
| PUT | `/api/vhost-limits/:vhost/:type` | Set a limit (`:type` is `max-connections` or `max-queues`) |
| DELETE | `/api/vhost-limits/:name/:type` | Remove a limit |

### Users

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/users` | List users |
| GET | `/api/users/:name` | User details |
| PUT | `/api/users/:name` | Create/update user |
| DELETE | `/api/users/:name` | Delete user |
| POST | `/api/users/bulk-delete` | Delete multiple users |
| GET | `/api/users/without-permissions` | List users with no permissions |
| GET | `/api/users/:name/permissions` | List permissions for a user |
| PUT | `/api/auth/hash_password` | Hash a password |

### Permissions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/permissions` | List all permissions |
| GET | `/api/permissions/:vhost/:user` | User permissions for vhost |
| PUT | `/api/permissions/:vhost/:user` | Set permissions |
| DELETE | `/api/permissions/:vhost/:user` | Remove permissions |

### Exchanges

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/exchanges` | List all exchanges |
| GET | `/api/exchanges/:vhost` | Exchanges in a vhost |
| GET | `/api/exchanges/:vhost/:name` | Exchange details |
| PUT | `/api/exchanges/:vhost/:name` | Declare exchange |
| DELETE | `/api/exchanges/:vhost/:name` | Delete exchange |
| POST | `/api/exchanges/:vhost/:name/publish` | Publish a message |

### Queues

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/queues` | List all queues |
| GET | `/api/queues/:vhost` | Queues in a vhost |
| GET | `/api/queues/:vhost/:name` | Queue details |
| PUT | `/api/queues/:vhost/:name` | Declare queue |
| DELETE | `/api/queues/:vhost/:name` | Delete queue |
| DELETE | `/api/queues/:vhost/:name/contents` | Purge queue |
| POST | `/api/queues/:vhost/:name/get` | Get messages |
| PUT | `/api/queues/:vhost/:name/pause` | Pause queue |
| PUT | `/api/queues/:vhost/:name/resume` | Resume queue |
| PUT | `/api/queues/:vhost/:name/restart` | Restart a closed queue |
| GET | `/api/queues/:vhost/:name/unacked` | List unacknowledged messages |
| POST | `/api/queues/:vhost/:name/stream` | Read from a stream queue (with offset and filter support) |
| GET | `/api/queues/:vhost/:name/bindings` | List bindings for a queue |

### Bindings

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/bindings` | List all bindings |
| GET | `/api/bindings/:vhost` | Bindings in a vhost |
| GET | `/api/bindings/:vhost/e/:name/q/:queue` | Bindings between exchange and queue |
| POST | `/api/bindings/:vhost/e/:name/q/:queue` | Create binding |
| GET | `/api/bindings/:vhost/e/:name/q/:queue/:props` | Get specific binding |
| DELETE | `/api/bindings/:vhost/e/:name/q/:queue/*props` | Delete binding |
| GET | `/api/bindings/:vhost/e/:name/e/:destination` | Bindings between two exchanges |
| POST | `/api/bindings/:vhost/e/:name/e/:destination` | Create exchange-to-exchange binding |
| GET | `/api/bindings/:vhost/e/:name/e/:destination/:props` | Get specific e2e binding |
| DELETE | `/api/bindings/:vhost/e/:name/e/:destination/*props` | Delete e2e binding |
| GET | `/api/exchanges/:vhost/:name/bindings/source` | Bindings where exchange is source |
| GET | `/api/exchanges/:vhost/:name/bindings/destination` | Bindings where exchange is destination |

### Connections and Channels

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/connections` | List connections |
| GET | `/api/vhosts/:vhost/connections` | List connections for a vhost |
| GET | `/api/connections/:name` | Connection details |
| GET | `/api/connections/:name/channels` | List channels for a connection |
| GET | `/api/connections/username/:username` | List connections by username |
| DELETE | `/api/connections/:name` | Close connection |
| DELETE | `/api/connections/username/:username` | Close all connections for a username |
| GET | `/api/channels` | List channels |
| GET | `/api/vhosts/:vhost/channels` | List channels for a vhost |
| GET | `/api/channels/:name` | Channel details |
| PUT | `/api/channels/:name` | Update channel settings (e.g. prefetch count) |
| GET | `/api/consumers` | List consumers |
| GET | `/api/consumers/:vhost` | Consumers in a vhost |
| DELETE | `/api/consumers/:vhost/:connection/:channel/:consumer_tag` | Cancel a consumer |

### Policies and Parameters

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/policies` | List all policies |
| GET | `/api/policies/:vhost` | Policies in a vhost |
| GET | `/api/policies/:vhost/:name` | Policy details |
| PUT | `/api/policies/:vhost/:name` | Create/update policy |
| DELETE | `/api/policies/:vhost/:name` | Delete policy |
| GET | `/api/operator-policies` | List all operator policies |
| GET | `/api/operator-policies/:vhost` | Operator policies in a vhost |
| GET | `/api/operator-policies/:vhost/:name` | Operator policy details |
| PUT | `/api/operator-policies/:vhost/:name` | Create/update operator policy |
| DELETE | `/api/operator-policies/:vhost/:name` | Delete operator policy |
| GET | `/api/parameters` | List all parameters |
| GET | `/api/parameters/:component` | Parameters by component (all vhosts) |
| GET | `/api/parameters/:component/:vhost` | Parameters by component and vhost |
| GET | `/api/parameters/:component/:vhost/:name` | Parameter details |
| PUT | `/api/parameters/:component/:vhost/:name` | Set parameter |
| DELETE | `/api/parameters/:component/:vhost/:name` | Delete parameter |
| GET | `/api/global-parameters` | List global parameters |
| GET | `/api/global-parameters/:name` | Global parameter details |
| PUT | `/api/global-parameters/:name` | Set global parameter |
| DELETE | `/api/global-parameters/:name` | Delete global parameter |

### Shovels and Federation

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/shovels` | List all shovels |
| GET | `/api/shovels/:vhost` | Shovels in a vhost |
| GET | `/api/shovels/:vhost/:name` | Shovel details |
| PUT | `/api/shovels/:vhost/:name/pause` | Pause a shovel |
| PUT | `/api/shovels/:vhost/:name/resume` | Resume a shovel |
| GET | `/api/federation-links` | List federation links |
| GET | `/api/federation-links/:vhost` | Federation links in a vhost |

### Definitions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/definitions` | Export all definitions |
| POST | `/api/definitions` | Import definitions |
| POST | `/api/definitions/upload` | Import definitions via form upload |
| GET | `/api/definitions/:vhost` | Export vhost definitions |
| POST | `/api/definitions/:vhost` | Import vhost definitions |
| POST | `/api/definitions/:vhost/upload` | Import vhost definitions via form upload |

### Monitoring

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/logs` | Download recent server logs as plain text |
| GET | `/api/livelog` | Live log stream (Server-Sent Events) |

### Prometheus Metrics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics` | Prometheus-format metrics (on port 15692) |
| GET | `/metrics/detailed` | Detailed per-queue Prometheus metrics |
