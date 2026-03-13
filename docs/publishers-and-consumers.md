
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a publisher?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          A publisher refers to a client application or system that creates and sends messages to LavinMQ, generating information or events to be distributed to other systems. When applications send messages, they _publish_ or _produce_ them.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What is a consumer?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        A consumer connects to LavinMQ to retrieve and process messages. It acts as a receiver, handling messages published by a producer. Designed for asynchronous processing, it fetches and works on messages independently, without real-time interaction with the publisher.
      </p>
    </div>
  </div>
</div>

## Publishing messages

To publish messages, clients establish a connection with LavinMQ. This connection serves as a pathway for communication between the client and the message broker. Within this connection, clients create a channel, which is a virtual communication pathway that allows them to interact with the message broker. For more information, refer to the documentation on [connections and channels](/documentation/amqp-connections-and-channels).

**Example:** Clients can start publishing messages once the connection and channel are established.

<!-- prettier-ignore-start -->

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
   queue="test_queue",
   durable=True,
)
channel.basic_publish(
	exchange="", # Use the default exchange
	routing_key="test_queue",
	body="Hello World"
)

connection.close()
```

- Messages are sent to an [exchange](/documentation/amqp-exchanges), not directly to a queue.
- The exchange receives messages from publishers and routes them to the correct queue(s) based on [bindings](/documentation/amqp-bindings).
- The routing key, set by the publisher, determines the message's destination.

Refer to the documentation on [exchanges](/documentation/amqp-exchanges) and [queues](/documentation/amqp-queues) for more information.

### Publishing to a non-existing exchange

If a message is published to a non-existing exchange, the message broker typically discards the message. This happens because the exchange is responsible for routing messages, so if it doesn’t exist, there is no way for the message to reach its intended destination. However, an [Alternate Exchange](/documentation/alternate-exchange) can be configured to handle undelivered messages.

### The publisher lifecycle

Publishers can be short-lived or long-lived. In a short-lived scenario, a publisher connects, publishes messages, and disconnects. In most cases, publishers are long-lived, maintaining a persistent connection, reusing the same channel, and publishing throughout the application's lifetime.

## Consuming messages

To consume messages, clients connect to LavinMQ and create channels. Once connected, clients subscribe to specific queues to receive messages. A user-defined handler (function or object based on the client library) processes messages when delivered.

**Example:** A consumer subscribes to a queue named `test_queue` and uses a handler called `call_back`:

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")

channel.basic_consume(
    "test_queue",
    callback,
    auto_ack=True,
)
connection.close()
```

### Consumer tag

Consumer tags uniquely identify each consumer, allowing the message broker to manage their state and correctly handle actions like acknowledgments or rejections. When a subscription is successful, a subscription identifier (consumer tag) is returned, which can later be used to cancel the consumer if needed.

### Consumer exclusivity

Consumer exclusivity assigns one consumer as the sole processor for a specific queue, ensuring only that consumer handles messages. If the exclusive consumer disconnects or stops, the application must register a new consumer to continue processing.

**Example:** Consumer exclusivity

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")
		ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    "ex_queue",
    callback,
    auto_ack=False,
    exclusive**=**True

)
connection.close()
```

### Consuming from non-existing queues

If a consumer tries to consume from a queue that doesn’t exist, LavinMQ typically responds with a `404, "NOT_FOUND"` error, indicating that the queue is unavailable. In this case, the consumer needs to ensure that the queue exists before attempting to consume from it.

### The Consumer lifecycle

Consumers are designed to be persistent, receiving multiple messages over time. They are typically registered at application startup and remain active once the connection is established, often throughout the application's lifetime.

### More resources on consumers

For more information about consumers and their various applications, refer to the following resources:

- [Single Active Consumer](/documentation/single-active-consumer)
- [Consumer priorities](/documentation/consumer-priorities)
- [The Competing Consumer Pattern](/documentation/task-queue-python)

<!-- prettier-ignore-end -->
