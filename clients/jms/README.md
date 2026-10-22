# LavinMQ JMS Client

A [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/3.1/) (JMS) client for
[LavinMQ](https://lavinmq.com) with its own AMQP 0-9-1 implementation and no runtime dependencies
beyond the `jakarta.jms-api`. It supports the classic API (`Connection`, `Session`, `MessageProducer`,
`MessageConsumer`) and the simplified API (`JMSContext`, `JMSProducer`, `JMSConsumer`), and recovers
from lost connections automatically.

Requires Java 17 or later.

## Installation

```xml
<dependency>
  <groupId>com.lavinmq</groupId>
  <artifactId>lavinmq-jms-client</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Build from source with `mvn install -DskipITs` in this directory.

## Quick start

Simplified API:

```java
import com.lavinmq.jms.LavinMQConnectionFactory;
import jakarta.jms.*;

ConnectionFactory factory = new LavinMQConnectionFactory("amqp://guest:guest@localhost:5672/%2F");

try (JMSContext ctx = factory.createContext()) {
    Queue queue = ctx.createQueue("orders");
    ctx.createProducer().send(queue, "hello");
    String body = ctx.createConsumer(queue).receiveBody(String.class, 5000);
}
```

Classic API:

```java
try (Connection connection = factory.createConnection()) {
    Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic("news.europe");
    MessageConsumer consumer = session.createConsumer(session.createTopic("news.*"));
    consumer.setMessageListener(message -> System.out.println(message));
    connection.start();
    session.createProducer(topic).send(session.createTextMessage("hello"));
}
```

## Configuration

`LavinMQConnectionFactory` has a public no-argument constructor and bean setters, and accepts an
`amqp://` or `amqps://` URL. Query parameters in the URL set the same properties
(`amqp://u:p@host/vhost?heartbeat=30&prefetch=50`). Several hosts can be listed for fail-over:
`amqp://u:p@host1:5672,host2:5672/vhost`.

| Property | Default | Description |
|---|---|---|
| `host`, `port`, `addresses` | `localhost:5672` | Broker address(es); `addresses` is a comma-separated `host[:port]` list tried in order |
| `useTls`, `sslContext` | `false`, JVM default | TLS (`amqps://` enables it; port 5671 by default) |
| `username`, `password`, `virtualHost` | `guest`, `guest`, `/` | Credentials and vhost |
| `clientID` | none | Client identifier (required for unshared durable subscriptions) |
| `heartbeat` | `60` | Heartbeat interval in seconds, `0` disables |
| `connectionTimeout`, `rpcTimeout` | `60000` | Connect/handshake and synchronous-method timeouts (ms) |
| `prefetch` | `100` | Unacknowledged messages the broker pushes to each session (`basic.qos`) |
| `publisherConfirms` | `PERSISTENT` | When a synchronous `send` waits for the broker's confirm: `PERSISTENT`, `ALWAYS` or `NEVER` |
| `confirmTimeout` | `30000` | How long a send waits for the confirm (ms) |
| `autoDeclareQueues` | `true` | Declare a durable queue when a JMS queue does not exist yet |
| `topicExchange` | `jms.topic` | Topic exchange used for JMS topics (set to `amq.topic` for interop with plain AMQP) |
| `delayedQueueExchange`, `delayedTopicExchange` | `jms.delayed`, `jms.delayed.topic` | Exchanges used for `setDeliveryDelay` |
| `dupsOkBatchSize` | `10` | Messages acknowledged per batch in `DUPS_OK_ACKNOWLEDGE` sessions |
| `objectMessageSerialFilter` | `java.base/*;java.lang.*;java.util.*;java.math.*;java.time.*;!*` | `ObjectInputFilter` pattern applied when reading `ObjectMessage` bodies |
| `connectionNameSuffix` | none | Appended to the connection name shown in the management UI |
| `automaticRecovery` | `true` | Reconnect and restore sessions and consumers after a connection loss |
| `recoveryInterval`, `recoveryMaxInterval` | `2000`, `30000` | Delay between reconnection attempts (ms), doubling up to the maximum |
| `recoveryAttempts` | `0` | Maximum attempts, `0` means unlimited |
| `recoveryTimeout` | `60000` | How long sends and administrative calls wait for a recovery in progress (ms) |
| `recoveryListener` | none | Optional `RecoveryListener` notified when recovery starts, succeeds or is given up |

The factory implements `Referenceable`, so it can be bound in JNDI; `LavinMQObjectFactory` rebuilds it.

## How JMS maps to AMQP

**Queues** are AMQP queues of the same name on the default exchange. A queue is checked with a
passive declare on first use; when it does not exist and `autoDeclareQueues` is on, a durable queue
without arguments is declared. Queues provisioned by operators (priority queues, dead-letter settings,
`x-delivery-limit` ...) are used as they are. Sends to queues are `mandatory`, so a message that
cannot be routed fails the send with `InvalidDestinationException`.

**Topics** are routing keys on the topic exchange (`jms.topic` by default, declared durable by the
client). Topic names are AMQP topic patterns: `.` separates words and `*`/`#` are wildcards in
subscriptions. Each subscription is a queue bound to the exchange:

| Subscription | Queue |
|---|---|
| non-durable, unshared | server-named exclusive auto-delete queue |
| non-durable, shared | `jms.shared.<clientID>.<name>`, auto-delete |
| durable (shared or not) | `jms.durable.<clientID>.<name>`, durable |

Unshared durable subscriptions consume exclusively, so a second active subscriber is refused. The
subscription queue carries the arguments `x-jms-topic` and `x-jms-shared`; creating a subscription
with the same name for another topic deletes and recreates the queue, as the specification requires.
`unsubscribe` deletes the queue.

**Temporary queues** are server-named exclusive queues (`amq.gen-...`); other connections may send to
them. **Temporary topics** are unique routing keys (`jms.tmp.<connection>.<n>`).

**Messages** map to AMQP properties: `JMSMessageID` → `message_id`, `JMSCorrelationID` →
`correlation_id`, `JMSType` → `type`, `JMSPriority` → `priority`, `JMSDeliveryMode` → `delivery_mode`,
`JMSReplyTo` → `reply_to` (plus the header `JMSReplyToType`), `JMSExpiration` → per-message TTL
(`expiration`) plus the header `JMSExpiration`, `JMSTimestamp` → `timestamp` plus the millisecond header
`JMSTimestamp`. User properties are entries of the AMQP headers table with their types preserved.
The header `JMSMessageType` records the message type (`text`, `bytes`, `map`, `stream`, `object`,
`message`); messages from non-JMS publishers become `TextMessage` for `text/*`, JSON and XML content
types and `BytesMessage` otherwise. `MapMessage` and `StreamMessage` bodies use a small self-describing
binary format; `ObjectMessage` bodies use Java serialization and are read through the configured
deserialization filter.

`JMSXDeliveryCount` is exact on queues with `x-delivery-limit` (LavinMQ's `x-delivery-count` header)
and otherwise 1 or 2 depending on the `redelivered` flag.

**Delivery delay** (`setDeliveryDelay`) publishes through an `x-delayed-message` exchange with the
`x-delay` header: `jms.delayed` (bound to the queue) for queues and `jms.delayed.topic` (bound to the
topic exchange) for topics.

**Sessions** use one AMQP channel each. In `AUTO_ACKNOWLEDGE` a message is acknowledged after it is
returned from `receive` or after `onMessage` returns; a `RuntimeException` from a listener requeues
the message for immediate redelivery. `CLIENT_ACKNOWLEDGE` acknowledges every consumed message of the
session on `acknowledge()`. `DUPS_OK_ACKNOWLEDGE` acknowledges in batches. Transacted sessions use
AMQP transactions: publishes and acknowledgements take effect on `commit()`; `rollback()` discards the
publishes and requeues the consumed messages.

**Queue browsers** fetch messages with `basic.get` without acknowledging them on a separate channel and
release them, in order, when the browser is closed. Browsed messages are not visible to other consumers
until then and come back with `JMSRedelivered` set.

## Automatic recovery

When the connection is lost (socket error, missed heartbeats, or a close initiated by the broker),
the client notifies the `ExceptionListener` with error code `RECOVERING`, reconnects with an
exponential back-off over the configured addresses, and then recreates sessions, consumers (including
server-named subscription queues) and temporary queues. Synchronous receives keep waiting; sends and
administrative calls block for up to `recoveryTimeout`. What is lost with the old connection:

- unacknowledged messages are requeued by the broker and come back with `JMSRedelivered` set;
  `acknowledge()` on a message from before the loss throws `IllegalStateException`;
- an open transaction: the next `commit()` throws `TransactionRolledBackException`;
- sends waiting for a publisher confirm fail with `JMSException` (no automatic resend);
- temporary queues get a new name; a `JMSReplyTo` sent before the loss is stale;
- an open `QueueBrowser` enumeration ends with an error.

When `recoveryAttempts` is exhausted the connection is closed, the `ExceptionListener` is notified
once more, and blocked calls fail. With `automaticRecovery=false` the connection is closed on the
first loss.

## Limitations

- Message selectors are not supported: `createConsumer(destination, selector)` with a non-empty
  selector throws `InvalidSelectorException`.
- No XA transactions, no `ConnectionConsumer` / `ServerSessionPool`.
- `setJMSCorrelationIDAsBytes` is not supported.
- `noLocal` is implemented in the client: messages of the same connection are acknowledged and dropped.
- A requeued message (`recover()`, `rollback()`, a failed listener) may be delivered to another
  consumer of the same queue, and on queues with retries enabled it is delayed by the retry back-off.
- `receiveNoWait()` only returns messages already buffered in the client.

## Running the tests

Unit tests need no broker: `mvn verify -DskipITs`. The integration tests (`*IT`) run against a LavinMQ
given by `LAVINMQ_URL` (default `amqp://guest:guest@localhost:5672/%2F`) with the HTTP API on port
15672 (`LAVINMQ_HTTP_URL` to override):

```
mvn verify
```
