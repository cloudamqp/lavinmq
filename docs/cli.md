# CLI (lavinmqctl)

`lavinmqctl` is the command-line tool for managing a running LavinMQ server. It communicates with the server via the HTTP management API.

## Connection

By default, `lavinmqctl` connects to `http://127.0.0.1:15672`. Override with:

```
lavinmqctl --host http://host:port ...
```

Or set the `LAVINMQCTL_HOST` environment variable.

Authentication uses `--username` and `--password` flags (default: `guest`/`guest`).

## Commands

### User Management

| Command | Description |
|---------|-------------|
| `add_user <username> <password>` | Create a new user |
| `delete_user <username>` | Delete a user |
| `change_password <username> <password>` | Change user password |
| `list_users` | List all users and their tags |
| `set_user_tags <username> <tags>` | Set user tags |
| `set_permissions <user> <configure> <write> <read>` | Set vhost permissions |
| `hash_password <password>` | Hash a password |

### Vhost Management

| Command | Description |
|---------|-------------|
| `list_vhosts` | List all vhosts |
| `add_vhost <vhost>` | Create a vhost |
| `delete_vhost <vhost>` | Delete a vhost |
| `set_vhost_limits <json>` | Set vhost limits (max-connections, max-queues) |

### Queue Management

| Command | Description |
|---------|-------------|
| `list_queues` | List all queues |
| `create_queue <name>` | Create a queue (supports `--durable`, `--auto-delete`, `--expires`, `--max-length`, `--message-ttl`) |
| `delete_queue <queue>` | Delete a queue |
| `purge_queue <queue>` | Purge all messages from a queue |
| `pause_queue <queue>` | Pause all consumers on a queue |
| `resume_queue <queue>` | Resume consumers on a queue |
| `restart_queue <queue>` | Restart a closed queue |

### Exchange Management

| Command | Description |
|---------|-------------|
| `list_exchanges` | List all exchanges |
| `delete_exchange <name>` | Delete an exchange |

### Connection Management

| Command | Description |
|---------|-------------|
| `list_connections` | List all AMQP connections |
| `close_connection <pid> <reason>` | Close a specific connection |
| `close_all_connections <reason>` | Close all connections |

### Policy Management

| Command | Description |
|---------|-------------|
| `list_policies` | List all policies |
| `set_policy <name> <pattern> <definition>` | Create/update a policy (supports `--priority`, `--apply-to`) |
| `clear_policy <name>` | Delete a policy |

### Shovel and Federation

| Command | Description |
|---------|-------------|
| `list_shovels` | List all shovels |
| `delete_shovel <name>` | Delete a shovel |
| `list_federations` | List federation upstreams |
| `delete_federation <name>` | Delete a federation upstream |

### Definitions

| Command | Description |
|---------|-------------|
| `export_definitions` | Export all definitions as JSON |
| `import_definitions <file>` | Import definitions from a JSON file |

### Server Control

| Command | Description |
|---------|-------------|
| `stop_app` | Stop the AMQP broker |
| `start_app` | Start the AMQP broker |

## Global Options

| Flag | Description |
|------|-------------|
| `--host=URL` | Management API URL |
| `--vhost=VHOST` | Target vhost (default: `/`) |
| `--username=USER` | API username |
| `--password=PASS` | API password |
| `--quiet` | Suppress output |
| `--format=FORMAT` | Output format |
