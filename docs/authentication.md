# Authentication

LavinMQ supports multiple authentication backends that can be chained together.

## Authentication Chain

The `auth_backends` config option defines an ordered list of authentication backends. When a client connects, each backend is tried in order. The first backend to return a successful authentication wins.

```ini
[main]
auth_backends = local,oauth
```

If `auth_backends` is empty (the default), only local authentication is used.

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

LavinMQ creates a default user on first start:

- Username: `guest` (configurable via `default_user`)
- Password: `guest` (configurable via `default_password_hash`)
- Tags: `administrator`

By default, the guest user can only connect from loopback addresses (127.0.0.1, ::1). This is controlled by `default_user_only_loopback` (default: `true`).

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
| `jwks_cache_ttl` | `1h` | How long to cache the JWKS keys |

### How It Works

1. Client connects with a JWT token as the password
2. The server validates the token signature against cached JWKS keys
3. Username is extracted from the configured claims
4. Scopes in the token are mapped to LavinMQ permissions

### Scope Mapping

JWT scopes are mapped to LavinMQ permissions. The scope format depends on the identity provider configuration and the `scope_prefix` setting.

## mTLS Authentication

Client certificate authentication via mutual TLS. When mTLS is configured, the client's certificate CN (Common Name) can be used for authentication. See [TLS](tls.md) for TLS configuration.
