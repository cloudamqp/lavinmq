# Policies

Policies apply configuration to queues and exchanges dynamically, matching by name pattern. Unlike queue/exchange arguments, policies can be changed after declaration without deleting and recreating resources.

## How Policies Work

Policies are stored per vhost and matched against existing and newly declared resources. When a queue or exchange is declared, or when a policy is added/updated/removed, the server scans all policies in the vhost, picks the single highest-priority policy whose `pattern` matches the resource name and whose `apply-to` includes its type, and applies that policy's `definition` to the resource. Resources that don't match any policy keep just their declared arguments.

A policy consists of:

| Field | Description |
|-------|-------------|
| `name` | Unique policy name (within a vhost) |
| `pattern` | Regex pattern matched against queue/exchange names |
| `apply-to` | `queues`, `exchanges`, or `all` |
| `priority` | Integer priority. Higher priority wins when multiple policies match. |
| `definition` | Key-value pairs of policy settings to apply |

Only one regular policy applies to a given resource at a time — there is no merging across multiple matching policies. If two regular policies have the same priority and both match, the resolution is unspecified, so use distinct priorities when patterns can overlap.

Operator policies (see below) are evaluated independently and can layer on top of the regular policy.

## Supported Policy Keys

| Key | Applies To | Description |
|-----|-----------|-------------|
| `max-length` | Queues | Maximum number of messages |
| `max-length-bytes` | Queues | Maximum total message bytes |
| `message-ttl` | Queues | Default message TTL (ms) |
| `expires` | Queues | Queue expiration after inactivity (ms) |
| `overflow` | Queues | Overflow behavior (drop-head, reject-publish) |
| `dead-letter-exchange` | Queues | Dead letter exchange name |
| `dead-letter-routing-key` | Queues | Dead letter routing key |
| `delivery-limit` | Queues | Max redelivery attempts |
| `max-age` | Streams | Retention period (e.g., `7D`, `1h`) |
| `alternate-exchange` | Exchanges | Alternate exchange for unroutable messages |
| `federation-upstream` | Both | Federation upstream name |
| `federation-upstream-set` | Both | Federation upstream set name |
| `delayed-message` | Exchanges | Enable delayed message behavior |

## Policy vs Queue Arguments

When both a policy and a queue argument set the same property, the more restrictive value applies. For example, if a policy sets `max-length: 1000` and the queue argument sets `x-max-length: 500`, the effective limit is 500 (the lower value).

Policies cannot override all queue arguments. Arguments like `x-queue-type` and `x-max-priority` are set-once at declaration and are not affected by policies.

## Operator Policies

Operator policies are a restricted subset of policies intended for infrastructure operators. They are applied independently from regular policies and merged with them.

Operator policies use the same matching logic (pattern, apply-to, priority) but are managed separately. When a regular policy and an operator policy both set the same key, the lower (more restrictive) value applies.

Operator policy keys are limited to: `max-length`, `max-length-bytes`, `message-ttl`, `expires`.

## Parameters

Parameters are a generic key-value store organized by component. They are used internally by federation upstreams, shovels, and other features that need per-vhost configuration.

Parameters are managed via the HTTP API or CLI under `/api/parameters/{component}/{vhost}/{name}`.
