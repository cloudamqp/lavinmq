# Authentication

LavinMQ supports multiple authentication backends that can be chained together.

## Authentication Chain

`auth_backends` defines an ordered list of authentication backends. When a client connects, each backend is tried in order; the first backend to return a successful authentication wins. If empty, only local authentication is used.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `auth_backends` | `[main]` | `[]` (local) | Ordered, comma-separated list of authentication backends |

```ini
[main]
auth_backends = local,oauth
```

Available backends:

| Backend | Description |
|---------|-------------|
| `local` | Username/password stored in the definitions file |
| `oauth` | JWT/OAuth2 token validation |

## Local Authentication

The default authentication method. Users and their password hashes are stored in the server's definitions file.

### Password Hashing

Supported hashing algorithms:

| Algorithm | Description |
|-----------|-------------|
| SHA256 | Default. Salted SHA-256 hash. |
| SHA512 | Salted SHA-512 hash |
| Bcrypt | Bcrypt with configurable cost |
| MD5 | Salted MD5 (legacy, not recommended) |

### Default User

LavinMQ creates a default user on first start with the `administrator` tag.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `default_user` | `[main]` | `guest` | Username of the default user |
| `default_password_hash` | `[main]` | (hash of `guest`) | Hashed password for the default user. Expects a hash value, not plaintext. |
| `default_user_only_loopback` | `[main]` | `true` | If true, the default user can only connect from loopback (127.0.0.1, ::1) |

## OAuth2 / OIDC Authentication

JWT-based authentication using an external identity provider.

### Configuration

```ini
[oauth]
issuer = https://auth.example.com
resource_server_id = lavinmq
preferred_username_claims = sub,client_id
verify_aud = true
audience = lavinmq
jwks_cache_ttl = 3600
```

| Option | Default | Description |
|--------|---------|-------------|
| `issuer` | (required) | OIDC issuer URL. Used to discover JWKS endpoint. |
| `resource_server_id` | (none) | Resource server identifier |
| `preferred_username_claims` | `sub,client_id` | JWT claims to extract the username from (tried in order) |
| `additional_scopes_keys` | `[]` | Additional JWT claims to check for scope strings |
| `scope_prefix` | (none) | Prefix to strip from scope strings |
| `verify_aud` | `true` | Verify the JWT `aud` claim |
| `audience` | (none) | Expected JWT audience value |
| `jwks_cache_ttl` | `3600` | How long to cache the JWKS keys (seconds) |

### How It Works

1. Client connects with a JWT token as the password
2. The server validates the token signature against cached JWKS keys and verifies the audience (if `verify_aud` is enabled)
3. Username is extracted from the claims listed in `preferred_username_claims` (tried in order; first non-empty value wins)
4. Tags and permissions are derived from the token's scopes
5. The token's expiration time is tracked. Long-lived connections must refresh the token via `connection.update-secret` before it expires.

### Scope Sources

LavinMQ collects scopes from several places in the JWT, in this order:

1. **`resource_access.<resource_server_id>.roles`** — if `resource_server_id` is configured (Keycloak-style claims).
2. **`scope`** — the standard JWT scope claim, parsed as a space-separated list.
3. **Additional claims** listed in `additional_scopes_keys`. Values can be strings (space-separated), arrays of strings, or nested objects keyed by `resource_server_id`.

### Scope Filtering

Scopes are filtered by a prefix to isolate LavinMQ-specific entries from a larger token:

- If `scope_prefix` is set, only scopes starting with that prefix are kept (prefix stripped).
- If `scope_prefix` is unset but `resource_server_id` is set, the prefix defaults to `<resource_server_id>.`.
- If neither is set, all scopes are used as-is.

### Scope Format

After filtering, each scope is interpreted as either a tag assignment or a permission grant.

**Tag scopes:**

```
tag:<tag-name>
```

Where `<tag-name>` is one of `administrator`, `monitoring`, `management`, `policymaker`, `impersonator`. Unknown tags are ignored. See [Users and Permissions](users-permissions.md) for what each tag grants.

**Permission scopes:**

```
<configure|read|write>:<vhost>/<resource-pattern>
```

Or with an optional routing-key segment (accepted for compatibility but ignored):

```
<configure|read|write>:<vhost>/<resource-pattern>/<routing-key>
```

- `<vhost>` and `<resource-pattern>` are URL-decoded.
- `<resource-pattern>` supports `*` as a wildcard (translated to a regex). A bare `*` matches everything.
- Multiple permission scopes for the same vhost and permission type are combined into a single regex (alternation).
- Permissions for vhosts not granted are denied by default (empty regex `^$`).

### Examples

A token with these filtered scopes:

```
tag:management
read:%2F/.*
write:%2F/orders
configure:staging/temp.*
```

Grants the user the `management` tag, full read access on the `/` vhost, write access only to resources starting with `orders` on `/`, and configure access to resources matching `temp*` on the `staging` vhost.

### Token Refresh

When the token expires, LavinMQ closes the connection unless the client sends `connection.update-secret` with a fresh token first. The username in the new token must match the original; tags and permissions are taken from the new token.

