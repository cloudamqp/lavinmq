# Channels

Channels are lightweight virtual connections multiplexed over a single AMQP connection. All AMQP operations (publish, consume, declare, etc.) happen on a channel.

Channels are an AMQP concept and do not apply to MQTT.

## Why Channels Exist

Opening a TCP connection is expensive. Channels allow multiple independent streams of communication over one connection, avoiding the overhead of multiple TCP connections while maintaining isolation between operations.

## Channel Limits

The maximum number of channels per connection is negotiated during connection setup.

- Default: 2048
- Configurable via `channel_max` in the `[amqp]` config section
- The lower value between client and server is used

## Prefetch (QoS)

Prefetch controls how many unacknowledged messages the server will deliver to a consumer before waiting for acknowledgments. Set via `basic.qos`.

| Parameter | Description |
|-----------|-------------|
| `prefetch_count` | Max unacknowledged messages |
| `global` | If `false` (default): per-consumer prefetch. If `true`: per-channel prefetch shared across all consumers on the channel. |

- Default prefetch: 65535 (configurable via `default_consumer_prefetch`)
- Setting prefetch to 0 means unlimited (the server will deliver as fast as possible)
- Prefetch is essential for fair dispatch across multiple consumers

## Channel Lifecycle

1. `channel.open` — opens a new channel on the connection
2. Normal operation — declare resources, publish, consume
3. `channel.close` — closes the channel

## Channel Errors

When a channel error occurs (e.g., accessing a non-existent queue, argument mismatch), the server closes the channel with an error code and message. The connection remains open and other channels are unaffected.

Common channel-level errors:
- `NOT_FOUND` — resource does not exist
- `PRECONDITION_FAILED` — argument mismatch on redeclaration
- `ACCESS_REFUSED` — permission denied
- `RESOURCE_LOCKED` — exclusive queue owned by another connection

## Flow Control

The server can pause message delivery on a channel via `channel.flow`. When flow is disabled (`active=false`), the server stops delivering messages to consumers on that channel. When re-enabled (`active=true`), delivery resumes.

This is used for back-pressure when consumers or the server cannot keep up with the delivery rate.

## Consumer Limit

The maximum number of consumers per channel can be configured with `max_consumers_per_channel` in the `[amqp]` config section. Default is 0 (unlimited).
