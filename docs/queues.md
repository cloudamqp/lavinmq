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

A queue whose message store fails is paused, not closed. Consumers stay connected and get no messages, and `basic.get` returns empty. The log has an error from the queue that says which of the two cases below it is. This pause isn't stored: a restart of the broker loads the queue's data again.

| Cause | What to do |
|-------|------------|
| A file operation failed, for example an ack couldn't be written or the broker is out of file descriptors | Fix the cause and resume the queue. Publishes are still confirmed and stored meanwhile. |
| Message data can't be read: a corrupt message, or a segment that can't be loaded when the broker starts | Delete the queue, and declare it again. In a cluster, a failover to a node with a good copy of the data also works. Resuming it doesn't help, and neither does a restart of the broker, which loads the same data. |

A queue with data it can't read nacks publishes, as they would be lost with the rest of its messages when it's deleted. Publishers without confirms lose them silently. Acks of messages delivered before are still taken. A purge is refused, with `PRECONDITION_FAILED` over AMQP and 400 in the HTTP API: what can still be read can't be told from what can't.

A queue's retry queue and the queue of a delayed exchange are paused the same way. They can't be deleted on their own: a retry queue with data it can't read goes when its queue is deleted, the queue of a delayed exchange when the exchange is deleted. Meanwhile a rejected message is requeued at once instead of being delayed, and a publish to the delayed exchange is nacked.

Streams follow the same rule: delete a stream with data it can't read. Its consumer offsets go with it, and a stream declared again starts from offset 1.

MQTT sessions are paused the same way, with their client still connected, and are shown as `paused`: resume the session after a failed file operation, delete it if its data can't be read. Publishes to a session with data it can't read aren't stored for it. A client that connects after the delete gets a new session, without its subscriptions. Like a queue, a session can also be paused and resumed through the API and the management UI, and that pause is kept when the broker restarts.

## Reserved Queue Name Prefixes

Queue names starting with `amq.` or `mqtt.` are reserved for server-internal use. Client queue declarations using these prefixes will be rejected, except for `amq.direct.reply-to.*` queues used for direct reply-to consumers.

## Internal Queues

Some features create internal, broker-managed queues, for example the delayed message exchange (`amq.delayed-<exchange_name>`) and a queue's retry queue (`amq.retry-<queue>`). Internal queues cannot be operated on over AMQP: passive declare, delete, purge, consume, basic get, bind and unbind are all refused with `ACCESS_REFUSED`. They remain visible in the management UI and HTTP API, where operators can inspect and purge them. A delete through the HTTP API is refused with 403: an internal queue is deleted together with the exchange or queue it belongs to. Bindings to internal queues are refused in the HTTP API as well, since messages cannot be published to them and such a binding could never deliver anything.
