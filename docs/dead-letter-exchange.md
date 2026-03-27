
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is the Dead Letter Exchange?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>The Dead Letter Exchange (DLX) can handle messages that cannot be delivered due to:</p>
        <ul>
          <li>Negative acknowledgment or rejection with requeuing disabled.</li>
          <li>Expiration due to TTL.</li>
          <li>Exceeding queue length limits.</li>
        </ul>
        <p>Instead of being lost, these messages are published to the DLX, which then routes them to a queue for later processing.</p>
        <img src="img/docs/dead-letter-exchange-illustration.png" class="border border-[#414040]" />
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What benefits does the Dead Letter Exchange have?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>Queues attached to a Dead Letter Exchange collect undeliverable messages, preventing message loss and allowing further handling based on the desired outcome.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect3" id="accordion3">
        When to use the Dead Letter Exchange?
      </button>
    </h3>
    <div id="sect3" class="accordion-content" role="region" aria-labelledby="accordion3">
      <p>Use a Dead Letter Exchange for messages that cannot be processed immediately but still require handling. This prevents message loss and allows for inspection or retries.</p>
    </div>
  </div>
</div>

Dead Letter Exchanges are similar to regular exchanges. They are declared as basic exchanges and can be used for any exchange type.

1. **Declare a Dead Letter Exchange.** In this example, it's named `dlx_exchange`.
2. **Declare the Dead Letter Queue.** In this example, it's named `dlx_queue`.
3. **Bind the Dead Letter Exchange to the Dead Letter Queue.**
4. **Declare the main queue.** Once the Dead Letter Exchange is set up, it can be used by any queue that needs to handle messages that may become undeliverable. Declare the main queue with the `x-dead-letter-exchange` argument and a routing key.
5. **Publish a message.** This example adds a TTL to the message, ensuring it moves to the Dead Letter Queue once the TTL expires.

<!-- prettier-ignore-start -->

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='dlx_exchange', exchange_type='direct')

channel.queue_declare(queue='dlx_queue', durable=True)
channel.queue_bind(exchange='dlx_exchange', queue='dlx_queue', routing_key='dlx_key')

channel.queue_declare(
    queue="main_queue",
    arguments={
        "x-dead-letter-exchange": "dlx_exchange",
        "x-dead-letter-routing-key": "dlx_key"
    }
)

channel.basic_publish(
    exchange='',
    routing_key='main_queue',
    body='Hello, DLX with TTL!',
    properties=pika.BasicProperties(
        expiration='5000'  # Expire the message after 5sec.
    )
)
```

### Publish a message with a DLX Header:

A message can be published to the main queue with `x-dead-letter-exchange` and `x-dead-letter-routing-key` arguments in the headers. These headers do not trigger dead-lettering but provide routing information. Dead-lettering occurs only if the main queue’s DLX settings are activated.

```python
channel.basic_publish(
    exchange='',
    routing_key='main_queue',
    body='Message with DLX headers!',
    properties=pika.BasicProperties(
        headers={
            'x-dead-letter-exchange': 'dlx_exchange',
            'x-dead-letter-routing-key': 'dlx_key'
        }
    )
)
```

### Define a policy that applies to any matching queues:

```python
lavinmqctl set_policy dead_lettering ".*" '{"dead-letter-exchange":"dlx_exchange", "x-dead-letter-routing-key": "dead_routing_key"}' --apply-to queues
```

The policy would attach the dead letter exchange, `dlx_exchange` to all queues. Using the lavinmqctl is just one way of defining policies. Read the [documentation on policies](policies.md) for more information on other approaches.


<!-- prettier-ignore-end -->
