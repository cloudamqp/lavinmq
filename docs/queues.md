# Queues

Queues store messages and deliver them to consumers. They are the primary destination for routed messages.

## Queue Types

LavinMQ supports several queue types, selected via the `x-queue-type` argument:

| Type | `x-queue-type` | Description |
|------|----------------|-------------|
| Standard | (default) | FIFO queue with optional durability |
| Priority | (use `x-max-priority`) | Delivers messages by priority |
| Stream | `stream` | Append-only log for multiple consumers |
| MQTT Session | `mqtt` | Internal, used by MQTT sessions |

See also: [Priority Queues](priority-queues.md), [Streams](streams.md), [Delayed Queues](delayed-queues.md)

## Queue Properties

| Property | Description |
|----------|-------------|
| `durable` | Queue survives server restart. Messages in a durable queue are persisted to disk. |
| `exclusive` | Queue is exclusive to the declaring connection. Deleted when the connection closes. Cannot be accessed by other connections. |
| `auto_delete` | Queue is deleted when the last consumer unsubscribes. |

A non-durable, non-exclusive queue is called a transient queue. Its messages are still written to disk but the queue is removed on server restart.

## Queue Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `x-message-ttl` | Int (>= 0) | Default message TTL in milliseconds. See [TTL](ttl.md). |
| `x-expires` | Int (>= 1) | Queue expiration after inactivity in milliseconds. See [TTL](ttl.md). |
| `x-max-length` | Int (>= 0) | Maximum number of messages in the queue |
| `x-max-length-bytes` | Int (>= 0) | Maximum total size of messages in bytes |
| `x-overflow` | String | Overflow behavior (see below) |
| `x-dead-letter-exchange` | String | Dead letter exchange. See [Dead Lettering](dead-lettering.md). |
| `x-dead-letter-routing-key` | String | Routing key for dead-lettered messages |
| `x-delivery-limit` | Int (>= 0) | Max redelivery attempts before dead-lettering |
| `x-consumer-timeout` | Int (>= 0) | Consumer idle timeout in milliseconds |
| `x-single-active-consumer` | Bool | Only one consumer receives messages at a time |
| `x-max-priority` | Int (0-255) | Enable priority queue with this many priority levels |
| `x-message-deduplication` | Bool | Enable message deduplication. See [Deduplication](deduplication.md). |
| `x-cache-size` | Int (>= 0) | Deduplication cache size |
| `x-cache-ttl` | Int (>= 0) | Deduplication cache TTL in milliseconds |
| `x-deduplication-header` | String | Header to use for deduplication key |

## Overflow Behavior

When a queue reaches its `x-max-length` or `x-max-length-bytes` limit, the overflow policy determines what happens:

| Policy | Behavior |
|--------|----------|
| `drop-head` (default) | The oldest message is removed from the head of the queue |
| `reject-publish` | New messages are rejected (basic.nack sent to publisher) |

Overflow behavior can be set via the `x-overflow` queue argument or the `overflow` policy.

## Requeue Behavior

When a consumer rejects or nacks a message with `requeue=true`:

- The message is placed back in the queue
- It may be delivered to a different consumer
- The `redelivered` flag is set to `true` on the next delivery

When `requeue=false`, the message is either dead-lettered (if a DLX is configured) or discarded.

## Queue Declaration

Declaring a queue that already exists succeeds only if the properties and arguments match the existing queue. A mismatch results in a `PRECONDITION_FAILED` channel error.

## Queue Deletion

Deleting a queue removes it and all its messages. Options:

- `if_unused` — only delete if no consumers
- `if_empty` — only delete if no messages

## Purge

`queue.purge` removes all messages from a queue without deleting the queue itself.

## Queue States

A queue can be in one of the following states:

| State | Description |
|-------|-------------|
| `running` | Normal operation, delivering messages to consumers |
| `paused` | Queue stops delivering messages but continues accepting publishes. Resume via the API. |
| `closed` | Queue is closed due to an error. Durable, non-exclusive queues can be restarted via the API. |
| `deleted` | Queue has been deleted |

Pause and resume are available via the management API (`PUT /api/queues/:vhost/:name/pause` and `/resume`). A closed queue can be restarted with `PUT /api/queues/:vhost/:name/restart`.

## Reserved Queue Name Prefixes

Queue names starting with `amq.` or `mqtt.` are reserved for server-internal use. Client queue declarations using these prefixes will be rejected, except for `amq.direct.reply-to.*` queues used for direct reply-to consumers.
