# Queues

A queue is a named, ordered store of messages, owned by a vhost, that delivers those messages to one or more consumers. It is the destination that exchanges route messages to via bindings.

## Queue Types

The queue type is selected at declaration time and cannot be changed afterwards.

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
| `x-delayed-retry-min` | Int (>= 1) | Enable automatic retries with this initial backoff delay in milliseconds. See [Automatic Retries](automatic-retries.md). |
| `x-delayed-retry-multiplier` | Int (>= 1) | Backoff shape: omit for linear, 1 for constant, 2 or more for exponential |
| `x-delayed-retry-max` | Int (>= 1) | Cap on a single retry delay in milliseconds |

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
| `closed` | Queue is being shut down, with the broker or before it is deleted |
| `deleted` | Queue has been deleted |

Pause and resume are available via the management API (`PUT /api/queues/:vhost/:name/pause` and `/resume`), the management UI and `lavinmqctl`. Both routes need `configure` permission on the queue, and are refused with 403 without it. A pause is kept when the broker restarts.

The queue list endpoints (`GET /api/queues` and `GET /api/queues/:vhost`) accept a `state` query parameter to only return queues in the given state(s), comma separated, e.g. `?state=paused` or `?state=running,paused`.

### Message store errors

A queue whose message store fails is paused, not closed. Consumers stay connected and get no messages, `basic.get` returns empty, and publishes are still confirmed and stored. The log has an error from the queue that says which of the two cases below it is. This pause isn't stored: a restart of the broker loads the queue's data again.

| Cause | What to do |
|-------|------------|
| A file operation failed, for example an ack couldn't be written or the broker is out of file descriptors | Fix the cause and resume the queue. |
| Message data can't be read: a corrupt message, or a segment that can't be loaded when the broker starts | Purge the queue, or delete it. Resuming it doesn't help, it stays paused. In a cluster, a failover to a node with a good copy of the data also works. |

A purge of a queue with data it can't read removes all of the queue's messages, whatever count is given, and resumes the queue unless it was also paused through the API. That includes the messages that can still be read and those that are delivered but not acked, whose acks and rejects are then ignored. While the queue waits for the purge, `max-length`, `max-length-bytes`, `delivery-limit` and message TTL aren't applied, so a queue with publishers keeps growing.

A queue's retry queue and the queue of a delayed exchange are paused the same way. Purging a queue also purges its retry queue.

Streams follow the same rule. A purge of a stream with data it can't read removes all its segments, and its consumers continue with what is published next. Offsets are never reused: if the last offset can't be read from the data, the new ones start well above it.

MQTT sessions are paused the same way, with their client still connected, and are shown as `paused`: resume the session after a failed file operation, purge it if its data can't be read. What the client has in flight when the purge removes it stays booked, so its acks aren't refused. A session can't be paused through the API.

## Reserved Queue Name Prefixes

Queue names starting with `amq.` or `mqtt.` are reserved for server-internal use. Client queue declarations using these prefixes will be rejected, except for `amq.direct.reply-to.*` queues used for direct reply-to consumers.

## Internal Queues

Some features create internal, broker-managed queues, for example the delayed message exchange (`amq.delayed-<exchange_name>`) and a queue's retry queue (`amq.retry-<queue>`). Internal queues cannot be operated on over AMQP: passive declare, delete, purge, consume, basic get, bind and unbind are all refused with `ACCESS_REFUSED`. They remain visible in the management UI and HTTP API, where operators can inspect and purge them. A delete through the HTTP API is refused with 403: an internal queue is deleted together with the exchange or queue it belongs to. Bindings to internal queues are refused in the HTTP API as well, since messages cannot be published to them and such a binding could never deliver anything.
