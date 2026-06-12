# Vhosts

Virtual hosts (vhosts) provide logical isolation within a single LavinMQ instance. Each vhost has its own set of exchanges, queues, bindings, permissions, policies, and parameters, and clients are scoped to one vhost per connection.

## Default Vhost

LavinMQ creates a default vhost named `/` on first start. The default user (`guest`) is granted full permissions on `/`. The default vhost can be deleted; nothing in the server depends on it existing.

## Isolation

Resources in different vhosts are completely independent:

- Exchanges and queues in one vhost are invisible to other vhosts
- Bindings only connect resources within the same vhost — there is no cross-vhost binding. To move messages between vhosts use a [shovel](shovels.md) or [federation](federation.md).
- Policies and parameters are scoped to a vhost
- Users must be granted permissions per vhost

A client connection is bound to a single vhost. AMQP clients select the vhost in `connection.open`; MQTT clients can encode it in the username as `vhost:username` (otherwise `default_vhost` is used).

## Vhost Limits

Vhost limits cap resource consumption per vhost. They are stored as part of the vhost definition and managed via the API or CLI.

| Limit | Description |
|-------|-------------|
| `max-connections` | Maximum concurrent connections to this vhost. New connections beyond the cap are refused with `connection.close` reply code 530 (`NOT_ALLOWED`). Existing connections are unaffected if the limit is later lowered below the current count. |
| `max-queues` | Maximum number of queues in this vhost. `queue.declare` fails when the cap is reached. Existing queues are unaffected if the limit is later lowered below the current count. |

Limits are optional. When not set, there is no cap. Setting a limit to a negative value removes it.

Limits are managed via `PUT /api/vhost-limits/:vhost/:type` (where `:type` is `max-connections` or `max-queues`) or the `lavinmqctl set_vhost_limits` command.

## Management

Vhosts can be managed via the HTTP API (`/api/vhosts`), the CLI (`lavinmqctl add_vhost` / `delete_vhost` / `list_vhosts`), or the management UI.

Deleting a vhost closes all its connections, removes all its resources (exchanges, queues, bindings, policies, parameters), removes all user permissions for that vhost, and recursively deletes the vhost's data directory from disk.
