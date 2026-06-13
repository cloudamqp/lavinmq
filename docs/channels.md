# Channels

Channels are lightweight virtual connections multiplexed over a single AMQP connection. All AMQP operations (publish, consume, declare, etc.) happen on a channel.

Channels are an AMQP concept and do not apply to MQTT.

## Why Channels Exist

Opening a TCP connection is expensive. Channels allow multiple independent streams of communication over one connection, avoiding the overhead of multiple TCP connections while maintaining isolation between operations.

## Channel Limits

The maximum number of channels per connection is negotiated during connection setup; the lower of the client and server values wins.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `channel_max` | `[amqp]` | `2048` | Maximum channels per connection |

## Prefetch (QoS)

Prefetch controls how many unacknowledged messages the server will deliver to a consumer before waiting for acknowledgments. Set via `basic.qos`.

| Parameter | Description |
|-----------|-------------|
| `prefetch_count` | Max unacknowledged messages. `0` means unlimited. |
| `global` | If `false` (default): per-consumer prefetch. If `true`: per-channel prefetch shared across all consumers on the channel. |

The server-wide default applies when a consumer does not call `basic.qos`:

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `default_consumer_prefetch` | `[main]` | `65535` | Default per-consumer prefetch |

## Channel Errors

When a channel error occurs (e.g., accessing a non-existent queue, argument mismatch), the server closes the channel with an error code and message. The connection remains open and other channels are unaffected.

Common channel-level errors:
- `NOT_FOUND` — resource does not exist
- `PRECONDITION_FAILED` — argument mismatch on redeclaration
- `ACCESS_REFUSED` — permission denied
- `RESOURCE_LOCKED` — exclusive queue owned by another connection

## Flow Control

Clients can pause message delivery on a channel by sending `channel.flow` with `active=false`. The server responds with `channel.flow-ok`. When re-enabled (`active=true`), delivery resumes.

When the server runs low on disk space, `basic.publish` returns a `precondition_failed` channel error until resources recover.

## Consumer Limit

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `max_consumers_per_channel` | `[amqp]` | `0` | Max consumers per channel. `0` means unlimited. |
