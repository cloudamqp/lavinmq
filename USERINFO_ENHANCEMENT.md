# Userinfo Endpoint Authentication Enhancement

**Branch**: `temp-user-userinfo`
**Base**: PR #1172 (`temp-user` branch by Christina)
**Date**: 2025-11-28

## Overview

This branch adds **userinfo endpoint authentication** as a complement to the existing JWT/JWKS OAuth authentication in PR #1172. This creates a hybrid OAuth2 system that supports both JWT tokens and opaque tokens.

## What This Adds

### New Authenticator: `UserinfoAuthenticator`

**Location**: `src/lavinmq/auth/authenticators/userinfo.cr`

**Purpose**: Validates opaque OAuth tokens (non-JWT) by calling the OAuth provider's userinfo endpoint.

**Use Cases**:
- GitHub personal access tokens
- OAuth providers that issue opaque tokens
- Simple OAuth flows without JWT complexity
- Legacy OAuth systems

### Configuration

New INI configuration options under `[oauth]` section:

```ini
[oauth]
# Userinfo endpoint validation (for opaque tokens)
userinfo_url = https://api.github.com/user
userinfo_username_claim = login
userinfo_username_prefix = github:
userinfo_default_vhost = /
```

### Auth Chain Integration

The authenticator can be enabled in the auth chain:

```ini
[main]
# Try JWT first, fallback to userinfo, then local
auth_backends = oauth,userinfo,local
```

## Architecture

### Hybrid Authentication Flow

```
Client connects with token
         ↓
┌────────────────────────────┐
│   LocalAuthenticator       │
│   (username/password)      │
└────────────────────────────┘
         ↓ (if no match)
┌────────────────────────────┐
│   OAuthAuthenticator       │
│   (JWT with JWKS)          │
│   • Fastest (< 1ms)        │
│   • No HTTP call           │
│   • Full scope support     │
└────────────────────────────┘
         ↓ (if not JWT/invalid)
┌────────────────────────────┐
│   UserinfoAuthenticator    │
│   (Opaque tokens)          │
│   • HTTP call (~100ms)     │
│   • Simple validation      │
│   • Default permissions    │
└────────────────────────────┘
```

### Comparison with JWT OAuth

| Feature | JWT (OAuthAuthenticator) | Userinfo (UserinfoAuthenticator) |
|---------|-------------------------|----------------------------------|
| **Token Type** | JWT only | Opaque tokens |
| **Validation** | Local (JWKS) | Remote (HTTP) |
| **Performance** | < 1ms (cached) | ~100ms per auth |
| **Permissions** | From token scopes | Default only |
| **UpdateSecret** | ✅ Supported | ❌ Not supported |
| **Token Refresh** | ✅ Yes | ❌ No |
| **Providers** | Auth0, Keycloak, etc. | GitHub, simple OAuth |

## Implementation Details

### Changes Made

1. **New File**: `src/lavinmq/auth/authenticators/userinfo.cr`
   - Implements `UserinfoAuthenticator` class
   - Calls OAuth provider's userinfo endpoint
   - Extracts username from configurable claim
   - Creates `OAuthUser` with default permissions

2. **Config Updates**: `src/lavinmq/config.cr`
   - Added 4 new config properties for userinfo
   - Added INI parsing for userinfo settings
   - Maintains backward compatibility

3. **Auth Chain**: `src/lavinmq/auth/chain.cr`
   - Added "userinfo" backend option
   - Can be used alongside or instead of "oauth"

### User Creation

Userinfo-authenticated users are created as `OAuthUser` instances with:
- Username extracted from configured claim (default: "sub")
- Optional username prefix (e.g., "github:")
- Default permissions (full access to default vhost)
- Token expiration set to 1 hour (opaque tokens don't have exp claim)
- Empty tags array (no admin privileges by default)

### Limitations

- **No scope parsing**: Userinfo responses typically don't include permission scopes
- **No token refresh**: Opaque tokens can't be refreshed via UpdateSecret frame
- **Performance**: Requires HTTP call for each authentication
- **Default permissions only**: All users get same permissions

## Testing

### Test with GitHub OAuth

```ini
[main]
auth_backends = userinfo,local

[oauth]
userinfo_url = https://api.github.com/user
userinfo_username_claim = login
userinfo_username_prefix = github:
userinfo_default_vhost = /
```

Then connect with:
- Username: anything (ignored)
- Password: GitHub personal access token

### Test Hybrid Mode

```ini
[main]
auth_backends = oauth,userinfo,local

[oauth]
# JWT validation (primary)
oauth_issuer_url = https://auth.example.com
resource_server_id = lavinmq
preferred_username_claims = preferred_username,email

# Userinfo fallback
userinfo_url = https://api.github.com/user
userinfo_username_claim = login
userinfo_username_prefix = github:
```

JWT tokens will be validated first (fast), opaque tokens fall back to userinfo.

## Benefits Over Base PR #1172

1. **Broader OAuth Support**: Works with GitHub, legacy OAuth providers
2. **Simpler Setup**: No need for OIDC/.well-known configuration for simple cases
3. **Backward Compatible**: Doesn't change any existing JWT functionality
4. **Hybrid Flexibility**: Can use both JWT and opaque tokens simultaneously
5. **Zero Backend**: Still no custom auth server needed

## Comparison with RabbitMQ

| Feature | RabbitMQ OAuth2 | LavinMQ (temp-user) | LavinMQ (this branch) |
|---------|----------------|---------------------|----------------------|
| JWT Validation | ✅ | ✅ | ✅ |
| Opaque Tokens | ❌ | ❌ | ✅ (unique!) |
| Scope Parsing | ✅ | ✅ | ✅ (JWT only) |
| Userinfo Fallback | ❌ | ❌ | ✅ (unique!) |
| UpdateSecret | ✅ | ✅ | ✅ (JWT only) |
| Configuration | Complex | Simple | Simple |

## Future Enhancements

Possible future improvements to userinfo authentication:

1. **Token Caching**: Cache validated tokens with TTL to reduce HTTP calls
2. **Batch Validation**: Support multiple tokens in one HTTP call
3. **Custom Permission Mapping**: Extract permissions from userinfo response
4. **Scope Support**: If userinfo includes scopes, parse them
5. **Metrics**: Track userinfo vs JWT authentication rates

## Notes for Reviewers

- This is a **complementary** feature, not a replacement
- JWT authentication (OAuthAuthenticator) remains the primary/recommended method
- Userinfo is for edge cases where JWT isn't available
- Code follows the same patterns as existing authenticators
- No changes to core authentication logic or User abstraction

## Related Work

- **Base**: PR #1172 by Christina (temp-user branch)
- **Previous Attempt**: oauth2-support and oauth2-enterprise-hybrid branches (abandoned in favor of building on temp-user)
- **Related PR**: #956 (HTTP auth backend - different approach)

## Questions?

- Should userinfo authentication support token caching?
- Should we add rate limiting for userinfo HTTP calls?
- Should default permissions be more restrictive?
