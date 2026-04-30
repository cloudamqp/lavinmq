# Definitions

Definitions are the complete declarative state of a LavinMQ server: vhosts, users, permissions, exchanges, queues, bindings, policies, and parameters.

## Export

Export all definitions as JSON:

```
# Via API
GET /api/definitions

# Via CLI
lavinmqctl export_definitions

# Via CLI (offline, from data directory)
lavinmqctl definitions /var/lib/lavinmq
```

Per-vhost export:
```
GET /api/definitions/<vhost>
```

## Import

Import definitions from a JSON file:

```
# Via API
POST /api/definitions
Content-Type: application/json
<json body>

# Via CLI
lavinmqctl import_definitions definitions.json
```

Import is additive: new resources are created and bindings/policies/parameters/users with the same name are replaced. Re-declaring an existing queue or exchange with mismatching properties (durable, auto-delete, arguments) returns a `precondition_failed` error rather than overwriting.

## Load on startup

Set `load_definitions` in the `[main]` section of the config file to import a JSON definitions file every time the broker boots:

```ini
[main]
load_definitions = /etc/lavinmq/definitions.json
```

Behavior:

- The file is imported before listeners start accepting connections.
- Existing users, permissions, vhosts, policies, and parameters are preserved. The file only adds to or updates state, never deletes. Queues and exchanges with mismatching properties return `precondition_failed` and skip without overwriting.
- On a fresh data directory, the default vhost (`/`) and default user are not seeded if `load_definitions` is set; the file is expected to declare the desired bootstrap state.
- If the file is missing, unreadable, or contains invalid JSON, the broker logs an error and exits with a non-zero status instead of starting.

## JSON Format

```json
{
  "lavinmq_version": "2.x.x",
  "vhosts": [
    { "name": "/" }
  ],
  "users": [
    {
      "name": "guest",
      "password_hash": "...",
      "hashing_algorithm": "SHA256",
      "tags": "administrator"
    }
  ],
  "permissions": [
    {
      "user": "guest",
      "vhost": "/",
      "configure": ".*",
      "read": ".*",
      "write": ".*"
    }
  ],
  "exchanges": [...],
  "queues": [...],
  "bindings": [...],
  "policies": [...],
  "parameters": [...]
}
```
