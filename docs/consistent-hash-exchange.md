
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is Consistent Hash Exchange?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>The Consistent Hash Exchange routes messages using the publisher's routing key and a hashing process. This ensures that messages related to the same identifier are always sent to the same consumer, keeping a predictable order and maintaining causal relationships.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        When should I use Consistent Hash Exchange?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>Manually distributing messages can be complex, as publishers often lack visibility into the number of queues and their bindings. Messages are typically distributed sequentially, but to prevent long queues, data is often spread across multiple queues or handled by several consumers. However, this distribution can lead to the loss of order.</p>
      <p>In contrast, messages sent to a consistent hash exchange are distributed to bound queues based on the hash computed from the routing key or header value. This method ensures that messages sharing the same computed hash—such as those linked to a specific booking ID or client ID—are directed to the same queue, thereby maintaining causal order for that ID as long as the bindings remain unchanged.</p>
    </div>
  </div>
</div>

The routing key value determines how messages are distributed across bound queues based on their computed hash. Queues can be dynamically added or removed at runtime, allowing flexible scaling while maintaining message order for specific keys.

1. **Declare queues.** In this case, two queues (`q1` and `q2`) are declared with durability.
2. **Define the exchange.** Declare a consistent hash exchange (`ce`), which will handle the routing of messages based on the hash of the routing key.
3. **Bind each queue to the exchange using a routing key.** The routing key on the binding must be set as a numeric value, which determines how many times the queue is added to the hash ring. This acts like a weight, impacting the likelihood of a queue receiving messages. A queue with a higher binding key value is present more frequently in the hash ring and is therefore more likely to be selected.

   Here, `q2` is added to the hash ring twice as many times as `q1`, meaning it has a higher chance of receiving messages—but the actual distribution depends on the hash function applied to the published routing keys.

4. **Publishing messages:** When a message is published, its routing key is hashed, and the exchange selects a queue based on its position in the hash ring.

   Since `user123` always hashes to the same value, this message will be routed to the same queue, ensuring that messages for a given user remain ordered. If more queues are added dynamically, the distribution of messages may shift, but messages for the same routing key will continue to be routed consistently.

**Example:** Set up a consistent hash exchange in LavinMQ to route messages based on the routing key's hash.

<!-- prettier-ignore-start -->

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='q1', durable=True)
channel.queue_declare(queue='q2', durable=True)

channel.exchange_declare(exchange="ce", exchange_type="x-consistent-hash", durable=True)

channel.queue_bind(exchange="ce", queue='q1', routing_key="1")
channel.queue_bind(exchange="ce", queue='q2', routing_key="2")

channel.basic_publish(
    exchange="ce",
    routing_key="user123",  # Routing key for hashing
    body="Message for user123",
)

connection.close()
```

<!-- prettier-ignore-end -->
