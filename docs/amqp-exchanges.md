
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is an exchange?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          An exchange receives messages from producers and routes them to the correct queues using bindings and routing keys based on predefined rules.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What benefits do exchanges have?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        Exchanges provide flexible message routing by applying specified rules, making it easy to direct messages to one or more queues based on the routing key and the type of exchange.
      </p>
    </div>
  </div>
</div>

LavinMQ supports four types of exchanges, each routing messages differently based on binding configurations and parameters: Direct, Topic, Fanout, and Header exchanges <internal links down the page>. Default exchanges are automatically created when the server starts, but clients also have the option to define custom exchanges.

## Setting up exchanges

1. **Declare the exchange:** Define the exchange's name and specify its exchange type (e.g., fanout, direct, topic), which determines how it routes messages.
2. **Declare the queue.**
3. **Establish a binding between the exchange and the queue.**
4. **Publish messages:** Send a message to the exchange, which routes it to the appropriate queue(s).
5. **Delete the exchange:** Remove the exchange when it's no longer needed.

**Example**: Declaring a fanout exchange, a queue, creating a binding between them, and finally deleting the exchange:

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='notifications', exchange_type='fanout')
channel.queue_declare(queue='notification_queue')
channel.queue_bind(exchange='notifications', queue='notification_queue')

channel.basic_publish(exchange='notifications', routing_key='', body='New notification from LavinMQ!')

channel.exchange_delete(exchange='notifications')

connection.close()
```

## Direct Exchange

A direct exchange routes messages to a specific queue based on a matching routing key. The routing key in the message is compared with the routing keys defined in the bindings. When they match, the message is delivered to the corresponding queue.

**Example:** Declare a direct exchange named `direct_exchange` and publish a message to it.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')
channel.queue_declare(queue='direct_queue')
channel.queue_bind(exchange='direct_exchange', queue='direct_queue', routing_key='direct_key')

channel.basic_publish(exchange='direct_exchange', routing_key='direct_key', body='Message to direct_exchange')

channel.exchange_delete(exchange='direct_exchange')

connection.close()
```

The default direct exchange in an AMQP broker is named `amq.direct`.

### Default exchange (nameless direct exchange)

The default direct exchange is a nameless exchange, often referred to by an empty string (`""`). When using this exchange, messages are routed to queues whose names match the message's routing key, as queues are automatically bound to it based on their names.

**Example**: Publish a message to the default exchange.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='queue1')
channel.basic_publish(exchange='', routing_key='queue1', body='Hello, queue1!')

connection.close()
```

## Topic Exchange

A topic exchange routes messages to one or more queues based on the routing key. The routing key in the message is compared against the routing key patterns defined in the bindings. Messages with routing keys that match the binding patterns are routed to the corresponding queues, where they remain until a consumer retrieves them or they are removed for other reasons.

The topic exchange supports strict routing key matching, like a direct exchange, but will also perform wild-card matching using `*` and `#` as placeholders. Routing keys must delimit the list of words by a period.

- The asterisk (`*`) is a wildcard for precisely one word.
- The hash (`#`) is a wildcard for zero or more words.

**Example**: Declare a topic exchange, create a queue, bind them with a routing key pattern (`*.error`), and publish a message with a matching routing key (`app.error`). The messages will be routed to the queue.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='topic')
channel.queue_declare(queue='log_queue')
channel.queue_bind(exchange='logs', queue='log_queue', routing_key='*.error')
channel.basic_publish(exchange='logs', routing_key='app.error', body='An error.')

connection.close()
```

The topic exchange and AMQP broker must provide `amq.topic`.

## Fanout Exchange

A fanout exchange routes and copies messages to all queues bound to it, disregarding the routing key. Fanout exchanges are helpful when the same message must be sent to multiple queues, where consumers may process the message differently.

**Example**: Declare a fanout exchange.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

connection.close()
```

The fanout exchange and AMQP broker must provide `amq.fanout`.

## Headers Exchange

Messages in a headers exchange are routed based on header values, rather than routing keys, using optional arguments. A message matches if the header's value equals the value specified in the routing key on the binding, defined by the `x-match` argument.

The `x-match` argument uses the binding to determine whether headers should match. The message's headers are compared with the binding's headers, and the `x-match` specifies whether all headers must match or just one.

- The `x-match` header can have two values: `all` or `any`.
  - `all`: The message header must match all the headers specified in the binding.
  - `any`: The message header must match at least one of the headers specified in the binding.

**Example**: Declare a headers exchange and set up a binding. In this example, the message will only be routed to the `my_queue` if it contains both `header1` with `value1` and `header2` with `value2` because of the `x-match: all` condition.

<!-- prettier-ignore-start -->
```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='headers_exchange', exchange_type='headers')
channel.queue_declare(queue='my_queue')
channel.queue_bind(exchange='headers_exchange', queue='my_queue', arguments={
    'x-match': 'all',
    'header1': 'value1',
    'header2': 'value2'
})
channel.basic_publish(
    exchange='headers_exchange',
    routing_key='',
    body='Message with headers',
    properties=pika.BasicProperties(headers={'header1': 'value1', 'header2': 'value2'})
)

connection.close()
```
<!-- prettier-ignore-end -->

The headers Exchange an AMQP broker must provide `amq.headers`.

## Exchange properties

An exchange is created with properties. These properties must be set individually for each exchange and cannot be applied using policies.

### Exchange Name

Every exchange must have a name when declared. If no name is provided, most client libraries will raise an error. Some libraries may allow temporary exchanges with a randomly or automatically assigned name.

Names starting with the `amq.` prefix are reserved for internal use and cannot be used for user-defined exchanges.

Example: Declare a named exchange.

```python
channel.exchange_declare(exchange='direct_ex', exchange_type='direct')
```

### Exchange Type

Exchange types define how messages are routed to queues:

- **`direct`**: Routes messages with a specific routing key.
- **`fanout`**: Routes messages to all bound queues (ignores routing keys).
- **`topic`**: Routes messages based on a pattern in the routing key.
- **`headers`**: Routes messages based on header values.

**Example:** Declare exchanges.

```python
channel.exchange_declare(exchange='direct_ex', exchange_type='direct')
channel.exchange_declare(exchange='ex', exchange_type='fanout')
channel.exchange_declare(exchange='topic_ex', exchange_type='topic')
channel.exchange_declare(exchange='headers_ex', exchange_type='headers')
```

### Durable Exchanges

Durable exchanges remain intact even after a server restart, ensuring continued availability and reducing the risk of data loss.

**Example:** Declare a durable direct exchange.

```python
channel.exchange_declare(exchange='durable_exchange', exchange_type='direct', durable=True)
```

### Auto-delete Exchanges

Auto-delete exchanges are automatically removed when no queues remain bound to them, freeing up system resources.

**Example:** Declare an auto-delete fanout exchange.

```python
channel.exchange_declare(exchange='auto_delete_exchange', exchange_type='fanout', auto_delete=True)
```

LavinMQ ignores the `auto-delete` field if the exchange already exists.

### Internal Exchanges

Internal exchanges are designed for internal use within the messaging system and are not intended for direct interaction with external components.

The internal exchange should be set if the exchange is going to be used in the exchange to exchange bindings.

### Delayed Exchanges

Delayed exchanges introduce a delay before delivering messages, enabling delayed processing or scheduling of tasks. Read more in the [Delayed Exchanges documentation](delayed-message.md).

### Exchange arguments (optional properties)

The specifications of AMQP 0.9.1 enable support for various features called [arguments](arguments-and-properties.md). Depending on the argument, their settings can be changed after the exchange declaration.

It is recommended that exchange arguments are set using policies to configure multiple queues simultaneously and ensure they are updated automatically when the policy definition changes.

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Argument</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Alternate Exchange <br/>(<code>x-alternate-exchange</code>: String)</td>
					<td>If messages to this exchange cannot otherwise be routed, send them to the alternate exchange named here.</td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

## Dead Letter Exchange

A queue declared with the x-dead-letter-exchange property will send rejected messages, negatively acknowledged (nacked) without a requeue setting, or expired (due to TTL) to the specified dead-letter-exchange. For more information, read about the [Dead Letter Exchange](dead-letter-exchange.md).

## Exchange to Exchange bindings

Bindings can be created between exchanges in LavinMQ.

**Example**: Create an exchange to exchange binding.

```python
channel.exchange_bind(destination='destination_exchange', source='source_exchange')
```
