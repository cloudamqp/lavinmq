# Vhosts

Virtual hosts (vhosts) provide logical isolation within a single LavinMQ instance. Each vhost has its own set of exchanges, queues, bindings, users, permissions, policies, and parameters.

## Default Vhost

LavinMQ creates a default vhost named `/` on first start.

## Isolation

Resources in different vhosts are completely independent:

- Exchanges and queues in one vhost are invisible to other vhosts
- Bindings only connect resources within the same vhost
- Policies and parameters are scoped to a vhost
- Users must be granted permissions per vhost

A client connection is bound to a single vhost, specified during connection negotiation.

## Vhost Limits

| Limit | Description |
|-------|-------------|
| `max-connections` | Maximum concurrent connections to this vhost. New connections are refused when the limit is reached. |
| `max-queues` | Maximum number of queues in this vhost. Queue declarations fail when the limit is reached. |

Limits are optional. When not set, there is no limit.

Limits are managed via the API at `/api/vhost-limits/:vhost/:name` or through the CLI. Setting a limit to `-1` removes it.

## Data Storage

Each vhost stores its data in a subdirectory under the main data directory, named by the SHA1 hash of the vhost name. The definitions file within each vhost directory tracks all declared resources.

## Management

Vhosts can be managed via:

- The HTTP API (`/api/vhosts`)
- The CLI (`lavinmqctl`)
- The management UI

Deleting a vhost removes all its resources (exchanges, queues, bindings, etc.) and all user permissions for that vhost.
