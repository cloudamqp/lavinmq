# Policies

Policies apply configuration to queues and exchanges dynamically, matching by name pattern. Unlike queue/exchange arguments, policies can be changed after declaration without deleting and recreating resources.

## How Policies Work

A policy consists of:

| Field | Description |
|-------|-------------|
| `name` | Unique policy name (within a vhost) |
| `pattern` | Regex pattern matched against queue/exchange names |
| `apply-to` | `queues`, `exchanges`, or `all` |
| `priority` | Integer priority. Higher priority wins when multiple policies match. |
| `definition` | Key-value pairs of policy settings to apply |

When multiple policies match a resource, only the highest-priority policy applies. Policies are re-evaluated when created, updated, or deleted.

## Supported Policy Keys

| Key | Applies To | Description |
|-----|-----------|-------------|
| `max-length` | Queues | Maximum number of messages |
| `max-length-bytes` | Queues | Maximum total message bytes |
| `message-ttl` | Queues | Default message TTL (ms) |
| `expires` | Queues | Queue expiration after inactivity (ms) |
| `overflow` | Queues | Overflow behavior (drop-head, reject-publish, reject-publish-dlx) |
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

Operator policies use the same matching logic (pattern, apply-to, priority) but are managed separately and take precedence over regular policies for the keys they define.

Operator policy keys are limited to the operational subset: `max-length`, `max-length-bytes`, `message-ttl`, `expires`, `overflow`, `delivery-limit`, `max-age`.

## Parameters

Parameters are a generic key-value store organized by component. They are used internally by federation upstreams, shovels, and other features that need per-vhost configuration.

Parameters are managed via the HTTP API or CLI under `/api/parameters/{component}/{vhost}/{name}`.
