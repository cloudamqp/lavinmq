# lavinmqctl

`lavinmqctl` is the command-line tool for managing a running LavinMQ server. It communicates with the server via the HTTP management API.

## Connection

By default, `lavinmqctl` connects to `http://127.0.0.1:15672`. Override with:

```
lavinmqctl --uri http://host:port ...
```

Or set the `LAVINMQCTL_HOST` environment variable.

When no connection flag is given, `lavinmqctl` talks to the server over its local control socket (default `/tmp/lavinmqctl.sock`). If the server was started with a custom `--control-unix-path`, point `lavinmqctl` at it with `--control-unix-path` or the `LAVINMQCTL_CONTROL_UNIX_PATH` environment variable.

Authentication uses `--user` and `--password` flags (default: `guest`/`guest`).

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
| `create_queue <name>` | Create a queue (supports `--durable`, `--auto-delete`, `--expires`, `--max-length`, `--message-ttl`, `--delivery-limit`, `--reject-on-overflow`, `--dead-letter-exchange`, `--dead-letter-routing-key`, `--stream-queue`) |
| `delete_queue <queue>` | Delete a queue |
| `purge_queue <queue>` | Purge all messages from a queue |
| `pause_queue <queue>` | Pause all consumers on a queue |
| `resume_queue <queue>` | Resume consumers on a queue |
| `restart_queue <queue>` | Restart a closed queue |

### Exchange Management

| Command | Description |
|---------|-------------|
| `list_exchanges` | List all exchanges |
| `create_exchange <type> <name>` | Create an exchange (supports `--auto-delete`, `--durable`, `--internal`, `--delayed`, `--alternate-exchange`, `--persist-messages`, `--persist-ms`) |
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
| `add_shovel <name>` | Create a shovel (supports `--src-uri`, `--dest-uri`, `--src-queue`, `--src-exchange`, `--src-exchange-key`, `--dest-exchange`, `--dest-exchange-key`, `--dest-queue`, `--src-prefetch-count`, `--ack-mode`, `--src-delete-after`, `--reconnect-delay`) |
| `delete_shovel <name>` | Delete a shovel |
| `list_federations` | List federation upstreams |
| `add_federation <name>` | Create a federation upstream (supports `--uri`, `--expires`, `--message-ttl`, `--max-hops`, `--prefetch-count`, `--reconnect-delay`, `--ack-mode`, `--queue`, `--exchange`) |
| `delete_federation <name>` | Delete a federation upstream |

### Definitions

| Command | Description |
|---------|-------------|
| `export_definitions` | Export all definitions as JSON |
| `import_definitions <file>` | Import definitions from a JSON file |

### Server Control

| Command | Description |
|---------|-------------|
| `status` | Display server status |
| `cluster_status` | Display cluster status |
| `tui [-i seconds]` | Start the interactive dashboard |
| `stop_app` | Stop the AMQP broker |
| `start_app` | Start the AMQP broker |
| `definitions` | Generate definitions JSON from a data directory (offline, does not use API) |

The TUI refreshes every `-i`/`--interval` seconds (default `1.0`, must be positive).
It waits longer when the broker is slow to answer, so that at most a tenth of the
broker's time goes to the TUI's requests, and the header then shows the interval used.
It's styled like the management UI. The Overview page shows message rate and queue
depth graphs, node resources, network rates, cluster followers and the queues with
the most messages. The graphs start from the history kept by the management API and
roll forward with each refresh. The other pages are tables that fetch as many rows
as fit in the terminal, one page at a time. A resized terminal is redrawn right away,
and the rows for its new size are fetched once it stops changing size. The TUI needs
a terminal of at least 40x10. `Enter` shows every field of the selected row, which follows the row
through refreshes and is scrolled with the same keys. Passwords in shovel and
federation URIs are masked, password hashes are not shown, and control characters
in names (for example in consumer tags or MQTT client ids) are shown as `?`.

| Key | Action |
|-----|--------|
| `1`-`9`, `0`, `s`, `f`, `u` | Overview, Queues, Connections, Channels, Exchanges, Consumers, Vhosts, Nodes, Parameters, Policies, Shovels, Federation, Users |
| `Tab`, `Shift-Tab`, `←`, `→` | Next or previous page |
| `↑`, `↓`, `j`, `k` | Move the selection |
| `PgUp`, `PgDn` | Previous or next page of rows |
| `Home`, `End`, `g`, `G` | First or last row |
| `Enter`, `Esc` | Show all fields of the selected row, back to the table |
| `o`, `r` | Sort by the next column, reverse the sort order |
| `/`, `Esc` | Filter by name, clear the filter |
| `p`, `Space` | Pause or resume refreshing |
| `?` | Show the keys |
| `q`, `Ctrl-C` | Quit |

For local TUI inspection without a broker, run `extras/tui_inspect.sh`. It starts a mock management API, runs the TUI in `tmux`, captures each page to text files, and exits.

## Global Options

| Flag | Description |
|------|-------------|
| `-U`, `--uri=URI` | Management API URI |
| `--hostname=HOST` | Management API hostname |
| `-P`, `--port=PORT` | Management API port |
| `--scheme=SCHEME` | URI scheme (http/https) |
| `--control-unix-path=PATH` | Control socket to use when not connecting via `--uri`/`--hostname` (default `/tmp/lavinmqctl.sock`, env `LAVINMQCTL_CONTROL_UNIX_PATH`) |
| `-p`, `--vhost=VHOST` | Target vhost (default: `/`) |
| `--user=USER` | API username |
| `--password=PASS` | API password |
| `-s`, `--silent` | Suppress informational messages and table formatting |
| `-q`, `--quiet` | Suppress output |
| `-f`, `--format=FORMAT` | Output format (`text` or `json`) |
| `--host=URL` | Deprecated, use `--uri` or `--hostname` |
| `-h`, `--help` | Show help and exit |
| `-v`, `--version` | Print version and exit |
| `--build-info` | Print build information and exit |
