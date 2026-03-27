
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a binding?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          A binding connects a queue to an exchange, specifying how messages should be routed based on the defined routing keys. It determines which messages a queue receives from an exchange.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What is a routing key?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        A routing key is a string that helps the broker route messages to the right queue. Think of it as a label that matches messages with their intended queues.
      </p>
    </div>
  </div>
</div>

In order to route messages from an exchange to a queue there needs to be a binding between the two.

1. Ensure the exchange and queue exist: The exchange acts as a message router, while the queue stores messages.
2. Attach the queue to the exchange: Establish a link between the queue and the exchange. A routing key may be needed for the binding depending on the exchange type.

Example: Bind a queue to an exchange.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

exchange_name = 'my_exchange'
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

queue_name = 'my_queue'
channel.queue_declare(queue=queue_name)

channel.queue_bind(exchange=exchange_name, queue=queue_name)
```

### Routing keys

Bindings can include an optional routing key, while messages have their routing keys. When a message arrives at the exchange, the exchange checks its routing key against the routing keys of the binding. If there’s a match, the message is delivered to the corresponding queue(s).

Example: Bind a queue to an exchange using a defined routing key.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

exchange_name = 'my_exchange'
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

queue_name = 'my_queue'
channel.queue_declare(queue=queue_name)

routing_key = "test"
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)
```

Read more about how different [exchanges](amqp-exchanges.md) are using routing keys.

### Header attributes

Headers can be added to a message to provide additional context and control how it is routed.

A headers exchange is a type of exchange that routes messages based on these headers instead of the routing key. In a headers exchange, the routing logic is determined by the presence and values of headers in the message.

When using the [header exchange](amqp-exchanges.md#headers-exchange), an argument called `x-match` is used as part of the binding. The message's headers are matched to the binding's headers. This header specifies whether all headers must match or just one.

- The `x-match` header can have two values: `all` or `any`.
  - `all`: The message must match all the headers specified in the binding.
  - `any`: The message must match at least one of the headers specified in the binding.
