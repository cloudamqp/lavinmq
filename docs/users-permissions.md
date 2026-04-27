# Users and Permissions

## Users

Each user has:

- **Name** — unique identifier
- **Password hash** — hashed with a supported algorithm (SHA256, SHA512, Bcrypt, MD5)
- **Tags** — roles that control access to management features

### User Tags

| Tag | Description |
|-----|-------------|
| `administrator` | Full access: manage users, vhosts, permissions, policies, and all management features |
| `monitoring` | Access to monitoring data (connections, channels, queues, node info) |
| `management` | Access to the management UI/API for resources the user has permissions on |
| `policymaker` | Can manage policies and parameters in vhosts they have access to |
| `impersonator` | Can publish messages with any `user_id` property (bypasses user_id validation) |

Tags are cumulative. A user can have multiple tags.

## Permissions

Permissions control what a user can do within a specific vhost. Each permission entry consists of three regex patterns:

| Permission | Controls |
|-----------|----------|
| `configure` | Declare, delete, and alter queues, exchanges, and bindings |
| `read` | Consume from queues, bind queues to exchanges, purge queues |
| `write` | Publish to exchanges, bind exchanges |

Each pattern is a regex matched against the resource name. An empty string `""` denies all access. `".*"` grants access to all resources.

### Permission Mapping

| AMQP Operation | Permission Required |
|---------------|-------------------|
| `exchange.declare` | `configure` on the exchange |
| `exchange.delete` | `configure` on the exchange |
| `queue.declare` | `configure` on the queue |
| `queue.delete` | `configure` on the queue |
| `queue.bind` | `read` on the exchange, `write` on the queue |
| `queue.unbind` | `read` on the exchange, `write` on the queue |
| `exchange.bind` | `read` on the source, `write` on the destination |
| `exchange.unbind` | `read` on the source, `write` on the destination |
| `basic.publish` | `write` on the exchange |
| `basic.consume` | `read` on the queue |
| `basic.get` | `read` on the queue |
| `queue.purge` | `read` on the queue |

### Default Permissions

The default user (`guest`) is created with full permissions (`".*"` for configure, read, and write) on the default vhost (`/`).

## Managing Users

Users can be managed via:

- The HTTP API (`/api/users`)
- The CLI (`lavinmqctl`)
- The management UI
