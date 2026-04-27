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
| GET | `/api/aliveness-test/:vhost` | Health check (creates temp queue, publishes and retrieves a test message) |
| GET | `/api/extensions` | List supported server extensions |

### Vhosts

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/vhosts` | List all vhosts |
| GET | `/api/vhosts/:name` | Vhost details |
| PUT | `/api/vhosts/:name` | Create vhost |
| DELETE | `/api/vhosts/:name` | Delete vhost |

### Vhost Limits

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/vhost-limits` | List all vhost limits |
| GET | `/api/vhost-limits/:vhost` | Limits for a vhost |
| PUT | `/api/vhost-limits/:vhost/:name` | Set a limit |
| DELETE | `/api/vhost-limits/:vhost/:name` | Remove a limit |

### Users

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/users` | List users |
| GET | `/api/users/:name` | User details |
| PUT | `/api/users/:name` | Create/update user |
| DELETE | `/api/users/:name` | Delete user |
| POST | `/api/users/bulk-delete` | Delete multiple users |
| GET | `/api/users/without-permissions` | List users with no permissions |

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

### Bindings

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/bindings` | List all bindings |
| GET | `/api/bindings/:vhost` | Bindings in a vhost |
| GET | `/api/bindings/:vhost/e/:exchange/q/:queue` | Bindings between exchange and queue |
| POST | `/api/bindings/:vhost/e/:exchange/q/:queue` | Create binding |
| DELETE | `/api/bindings/:vhost/e/:exchange/q/:queue/:props` | Delete binding |

### Connections and Channels

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/connections` | List connections |
| GET | `/api/connections/:name` | Connection details |
| DELETE | `/api/connections/:name` | Close connection |
| GET | `/api/channels` | List channels |
| GET | `/api/channels/:name` | Channel details |
| GET | `/api/consumers` | List consumers |
| GET | `/api/consumers/:vhost` | Consumers in a vhost |

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
| GET | `/api/parameters/:component/:vhost` | Parameters by component |
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
| PUT | `/api/shovels/:vhost/:name/pause` | Pause a shovel |
| PUT | `/api/shovels/:vhost/:name/resume` | Resume a shovel |
| GET | `/api/federation-links` | List federation links |
| GET | `/api/federation-links/:vhost` | Federation links in a vhost |

### Definitions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/definitions` | Export all definitions |
| POST | `/api/definitions` | Import definitions |
| GET | `/api/definitions/:vhost` | Export vhost definitions |
| POST | `/api/definitions/:vhost` | Import vhost definitions |

### Monitoring

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/logs` | Stream server logs (SSE) |
| GET | `/api/livelog` | Live log stream |

### Prometheus Metrics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics` | Prometheus-format metrics (on port 15692) |
| GET | `/metrics/detailed` | Detailed per-queue Prometheus metrics |
