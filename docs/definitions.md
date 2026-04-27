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
lavinmqctl export_definitions --data-dir /var/lib/lavinmq
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

Import is additive: existing resources are preserved, new ones are created, and matching ones are updated.

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
      "name": "guest",
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

## Use Cases

- **Backup** — periodically export definitions for disaster recovery
- **Migration** — export from one server, import to another
- **Reproducible setup** — version-control a definitions file and import on deploy
- **Staging/production parity** — share the same topology across environments
