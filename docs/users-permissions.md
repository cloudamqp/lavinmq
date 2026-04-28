# Users and Permissions

## Users

Each user has:

- **Name** — unique identifier
- **Password hash** — hashed with a supported algorithm (SHA256, SHA512, Bcrypt, MD5)
- **Tags** — roles that control access to management features

### User Tags

Tags primarily control access to the management HTTP API and UI. AMQP/MQTT messaging access is governed by permissions (below), not tags — a user with no tags can still publish and consume as long as they have AMQP/MQTT permissions on the relevant vhost. A user with no tags simply cannot use the management API or UI at all.

| Tag | Grants |
|-----|--------|
| `administrator` | Full management access. Passes every API check, including vhost-scoped ones, regardless of permissions entries. Can manage users, vhosts, permissions, policies, parameters, federation, shovels, and all server-wide settings. |
| `monitoring` | Management API access. Vhost listings (and other "full view" endpoints) include every vhost on the server. Per-vhost endpoints still require a permissions entry on the vhost being inspected — monitoring users without a permissions entry are refused on those routes. |
| `management` | Management API access scoped to vhosts the user has a permissions entry on. Cannot see vhosts they have no entry for. |
| `policymaker` | Manage policies, operator policies, and parameters in vhosts the user has a permissions entry on. The tag also passes the general management gate, so a policymaker user can use the management API more broadly, though many specific endpoints have stricter checks of their own. |
| `impersonator` | AMQP-level only: bypasses the server's `user_id` property validation on publish. Has no effect on management API access, and `administrator` does not imply it. |

Tags are cumulative — a user can have any combination, and the union of all granted abilities applies.

## Permissions

Permissions are stored per (user, vhost) pair. Each entry contains three regex patterns:

| Permission | Controls |
|-----------|----------|
| `configure` | Declare, delete, and alter queues, exchanges, and bindings |
| `read` | Consume from queues, bind queues to exchanges, purge queues |
| `write` | Publish to exchanges, bind exchanges |

Each AMQP/MQTT operation is checked by running the relevant regex against the resource name (queue or exchange). LavinMQ uses a partial match — the pattern matches if it occurs anywhere in the name, so `orders` would allow access to `orders`, `orders-archive`, and `daily-orders` alike. Anchor with `^...$` (e.g., `^orders$`) for an exact match.

Two regex values are treated as deny-all and cannot be used to grant access: the empty regex `//` and the explicit empty string regex `/^$/`. Submitting an empty string for a permission therefore denies all access for that operation type. `.*` grants access to every resource in the vhost.

If a user has no permission entry for a vhost at all, they cannot connect to it; the connection is rejected at handshake time. Permissions can only be granted per-existing-vhost — there is no "all vhosts" wildcard.

Permission changes take effect immediately on existing connections. Each connection caches its permission lookups, but every user carries a revision counter that is bumped when their permissions change, which invalidates the caches and forces a fresh evaluation on the next operation.

### `user_id` validation on publish

If a published message carries the `user_id` property, the server checks it matches the connection's authenticated user name and rejects the publish with a channel error if it does not. Users with the `impersonator` tag bypass this check and can publish messages on behalf of other users.

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
