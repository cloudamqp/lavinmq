# Consumers

Consumers receive messages from queues. A consumer is created by calling `basic.consume` on a channel, specifying the queue to consume from.

## Consumer Lifecycle

1. `basic.consume` — registers a consumer on a queue, returns a consumer tag
2. Messages are pushed to the consumer as they become available
3. `basic.cancel` — unregisters the consumer

The consumer tag is a client-chosen string that identifies the consumer. If not provided, the server generates one.

## Acknowledgment Modes

### Auto-Ack (no_ack=true)

Messages are considered acknowledged as soon as they are delivered. No explicit ack is needed. Messages are removed from the queue immediately on delivery.

Use when message loss on consumer failure is acceptable.

### Manual Ack (no_ack=false)

The consumer must explicitly acknowledge each message:

- `basic.ack` — acknowledge (remove from queue)
- `basic.reject` — reject a single message (optionally requeue)
- `basic.nack` — reject one or more messages (optionally requeue). Supports the `multiple` flag to ack/nack all messages up to the delivery tag.

Unacknowledged messages are requeued if the consumer disconnects or the channel closes.

## Prefetch (QoS)

Prefetch limits the number of unacknowledged messages the server will deliver to a consumer. Set via `basic.qos`:

| Parameter | Description |
|-----------|-------------|
| `prefetch_count` | Max unacknowledged messages |
| `global=false` | Per-consumer limit (default) |
| `global=true` | Per-channel limit, shared across all consumers on the channel |

Prefetch of `0` means unlimited delivery. The server-wide default applies when a consumer does not call `basic.qos`:

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `default_consumer_prefetch` | `[main]` | `65535` | Default per-consumer prefetch |

## Single Active Consumer

When a queue is declared with `x-single-active-consumer: true`, only one consumer receives messages at a time. If that consumer disconnects, the next registered consumer becomes active.

This is useful for ordered processing where only one consumer should handle messages from the queue.

## Consumer Priority

Consumers can declare a priority via the `x-priority` argument on `basic.consume`. Higher-priority consumers receive messages before lower-priority ones. Default priority is 0.

When a high-priority consumer has reached its prefetch limit, messages are delivered to the next highest-priority consumer with available capacity.

## Consumer Timeout

If the oldest unacknowledged message on a consumer has been waiting longer than the configured timeout, LavinMQ closes the channel. The timeout is measured from when the message was delivered, not from last channel activity, so acknowledging other messages does not reset the timer.

A per-queue override can be set with the `x-consumer-timeout` queue argument (milliseconds).

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `consumer_timeout` | `[main]` | (none) | Server-wide consumer idle timeout (milliseconds) |
| `consumer_timeout_loop_interval` | `[main]` | `60` | How often the timeout check runs (seconds) |

## basic.get (Polling)

`basic.get` retrieves a single message from a queue synchronously. Unlike `basic.consume`, it does not register a persistent consumer. Each call fetches one message (or returns empty if the queue is empty).

`basic.consume` is preferred for throughput. `basic.get` is useful for batch processing or infrequent polling.

## basic.recover

`basic.recover` asks the server to redeliver all unacknowledged messages on the channel.

- `requeue=true` — messages are requeued and may be delivered to a different consumer
- `requeue=false` — the server attempts to redeliver messages to the original consumer. If the consumer is no longer active, messages are requeued instead.

## Exclusive Consumers

A consumer declared with `exclusive: true` ensures no other consumers can consume from the queue while it is active. If another consumer tries to consume from the same queue, a `403 ACCESS_REFUSED` error is returned.
