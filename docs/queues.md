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

Pause and resume are available via the management API (`PUT /api/queues/:vhost/:name/pause` and `/resume`), the management UI and `lavinmqctl`. Both routes need `configure` permission on the queue, and are refused with 403 without it. A pause is kept when the broker restarts, and in a cluster when another node takes over.

The queue list endpoints (`GET /api/queues` and `GET /api/queues/:vhost`) accept a `state` query parameter to only return queues in the given state(s), comma separated, e.g. `?state=paused` or `?state=running,paused`.

### Message data that can't be read

A queue whose message store fails is paused instead of closed. That's nearly always message data that can't be decoded. Nothing is delivered past the data, and consumers stay connected. Publishes are still stored, in a new segment: nothing more is written to a segment that can't be read.

#### When a queue is paused

- **A delivery**: the next message for a consumer or a `basic.get` can't be decoded, also a requeued one.
- **A damaged segment header**: a read reaches a segment whose schema version isn't this version's. The broker doesn't start on a data directory of another version, so this is damage, not an upgrade or a downgrade.
- **Message TTL**: the check of when the first message expires can't read it. That check also runs when the queue starts, so a queue can be paused right after a restart.
- **Limits**: `max-length`, `max-length-bytes` or `delivery-limit` have to drop the first message and can't read it.
- **A reject, requeue or `basic.recover`** of a message that can't be read again, which its TTL, dead-lettering and retries need. The message goes back in the queue.
- **A failed delete** of a message that was dead-lettered or moved to the retry queue, for example when the disk is full.
- **Retry queues and the queues of delayed exchanges**, when releasing a message fails in one of these ways.

A queue isn't paused for a message that was half written when the broker was killed, which is dropped at start, or for a segment that ends in the middle of a message, where the reader continues with the next segment and logs a warning. A segment that can't be read doesn't pause a queue when it's loaded either, only when it's read.

#### What it looks like

- The queue's state is `paused`, in the HTTP API (`GET /api/queues?state=paused` lists them) and the management UI. The API doesn't tell this pause from one made by an operator.
- The log has an error from the queue, `Paused due to a message store error`, with the segment's path and the position in it.
- Consumers get no messages and aren't cancelled. `basic.get` returns empty.
- Publishes are confirmed and stored, in a new segment. `max-length`, `max-length-bytes` and `delivery-limit` aren't enforced until the queue is resumed, as the first message can't be read, so a queue with publishers keeps growing.

#### What to do

| Situation | Action |
|-----------|--------|
| The cause was temporary or is fixed, such as a full disk or a disk error | Resume the queue. The data is read again, and the queue is paused again if that still fails. |
| In a cluster, when this node's copy may be the only bad one | Fail over to another node, which reads its own copy of the files. The pause isn't replicated. |
| The data can be given up | Purge the queue and resume it, or delete the queue. |

- A purge removes the whole segment that can't be read, as where the next message in it starts is unknown. That includes its messages that are delivered but not acked yet, whose acks and rejects are then ignored.
- Resuming a queue also resumes its retry queue, unless that was paused through the API, and purging a queue purges its retry queue.
- Editing segment files by hand isn't supported.

#### Streams and MQTT sessions

- **Streams** aren't paused. A consumer that reaches a segment it can't read continues with the next segment, which is logged, and other consumers aren't held back. The segment is kept until retention removes it, so after a restart or a failover that can read it, it's delivered to consumers that start before it.
- **MQTT sessions** can't be paused. A session with a message that can't be read delivers nothing, with its client still connected, and logs an error. Purge or delete the session, or restart the broker to read the message again.

## Reserved Queue Name Prefixes

Queue names starting with `amq.` or `mqtt.` are reserved for server-internal use. Client queue declarations using these prefixes will be rejected, except for `amq.direct.reply-to.*` queues used for direct reply-to consumers.

## Internal Queues

Some features create internal, broker-managed queues, for example the delayed message exchange (`amq.delayed-<exchange_name>`) and a queue's retry queue (`amq.retry-<queue>`). Internal queues cannot be operated on over AMQP: passive declare, delete, purge, consume, basic get, bind and unbind are all refused with `ACCESS_REFUSED`. They remain visible in the management UI and HTTP API, where operators can inspect and purge them. A delete through the HTTP API is refused with 403: an internal queue is deleted together with the exchange or queue it belongs to. Bindings to internal queues are refused in the HTTP API as well, since messages cannot be published to them and such a binding could never deliver anything.
