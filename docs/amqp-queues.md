
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a queue?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          An AMQP queue is a buffer where messages are held until a consuming client retrieves them for processing. Messages remain in the queue until consumed or removed due to expiration, purging, deletion, or conditions such as reaching a size limit. In LavinMQ, all messages sent to a queue are persistently stored on disk. Queues deliver messages in a first-in, first-out (FIFO) order.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What features do queues hold?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        A queue serves as a temporary storage area for messages, acting as a buffer before they are consumed. When created, queues can be configured with various attributes that determine their lifecycle and behavior. For instance, an auto-delete queue is automatically removed when its last connection closes, while an exclusive queue is restricted to a single connection and is deleted once that connection ends.
      </p>
    </div>
  </div>
</div>

LavinMQ supports two types of queues: standard queues and stream queues. Standard queues are designed for short-term message storage, where messages are discarded once handled. If multiple message processing times are required, LavinMQ offers stream queues. In stream queues, messages are not deleted after consumption, allowing multiple consumers to read the same message and enabling message replay. Read the [Stream queues documentation](/documentation/streams) for more information.

## Using a queue

When setting up a message queue, begin by declaring it with a name. If the queue does not already exist, it will be created.

The sending service declares the queue, and the LavinMQ server confirms the declaration by responding `declare-ok`. The service then connects a consumer to the message queue. Messages can now be sent via the queue from one service (producer) to another (consumer). The consumer can explicitly disconnect from a queue or disconnect by closing the channel and/or connection.

**Example**: Declare a durable queue.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a durable queue
channel.queue_declare(queue='test', durable=True)

connection.close()
```

## Queue properties

A queue can be created with some given queue properties. Queue properties cannot be applied with policies and must be enabled per queue. These properties are available in LavinMQ:

- Name
- Durable
- Exclusive
- Auto-delete
- Arguments

## Queue Name

A queue can have a given name, if no name is specified, most client libraries assign a random name to the queue. Queue names starting with the `amq.` prefix are reserved for internal use and cannot be used for user-defined queues.

Example: Declare a queue with the queue name `the_queue_name`.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='the_queue_name', durable=True)
```

## Durable queues

A queue can be marked as durable, which specifies whether it should persist a LavinMQ restart. To create a durable queue, specify the property `durable` as `True` during the queue's creation as done in the example above.

**Example**: Declare a durable queue.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='the_queue_name', durable=True)
```

## Exclusive queues

Setting the properties to `exclusive` is useful when limiting a queue to only one consumer. Exclusive queues can only be used by one connection, meaning that all actions to and from that queue happen over and follow the preset settings of one specific connection. When the connection closes, the exclusive queue is deleted.

**Example**: Declare an exclusive queue.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='test', exclusive=True)
```

## Auto-delete queues

A queue can be declared with the `auto-delete` property. This is useful when queues should not stay open when the final connection to the queue closes. The auto-delete property requires at least one connected consumer to succeed. Otherwise, it will not be deleted.

Note: Different consumer and broker communication methods can impact the auto-delete property. For example, the `basic.get` method gives direct access to a queue instead of setting up a consumer, so consuming a message with `basic.get` will not affect `auto_delete`.

**Example**: Declare an auto-delete queue.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='test', auto_delete=True)

connection.close()
```

## Queue arguments (optional properties)

The specifications of AMQP 0.9.1 enable support for various features called arguments. Depending on the argument, their settings can be changed after the queue declaration.

It is recommended that queue arguments are set using policies to configure multiple queues simultaneously and ensure they are updated automatically when the policy definition changes.

Read more about [policies](/documentation/policies).

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
					<td><span class="font-semibold">Auto Expire</span> <br/>(<code class="language-plaintext highlighter-rouge">x-expires</code> : Int)</td>
					<td>Sets the time (in milliseconds) after which an <strong>idle queue</strong> (one without consumers) is automatically deleted.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Max Length</span> <br/>(<code class="language-plaintext highlighter-rouge">x-max-length</code> : Int)</td>
					<td>Limits the <strong>number of messages</strong> a queue can hold. If exceeded, messages are dropped based on the overflow policy.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Message TTL</span> <br/>(<code class="language-plaintext highlighter-rouge">x-message-ttl</code> : Int)</td>
					<td>Defines how long (in milliseconds) a message can stay in the queue before being automatically removed.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Delivery Limit</span> <br/>(<code class="language-plaintext highlighter-rouge">x-delivery-limit</code> : Int)</td>
					<td>Specifies the <strong>maximum number of delivery attempts</strong> for a message before it is discarded or sent to a dead-letter exchange.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Overflow Behaviour</span> <br/>(<code class="language-plaintext highlighter-rouge">x-overflow</code> : String)</td>
					<td>Determines what happens when a queue reaches its max length: By default this is set to <code class="language-plaintext highlighter-rouge">"drop-head"</code> (removes oldest messages). Also available is <code class="language-plaintext highlighter-rouge">"reject-publish"</code> (rejects new messages).</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Dead Letter Exchange</span> <br/>(<code class="language-plaintext highlighter-rouge">x-dead-letter-exchange</code> : String)</td>
					<td>Specifies an <strong>exchange</strong> where messages are sent when they expire, exceed delivery attempts, or are rejected.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Dead Letter Routing Key</span> <br/>(<code class="language-plaintext highlighter-rouge">x-dead-letter-routing-key</code> : String)</td>
					<td>Defines a <strong>custom routing key</strong> for messages sent to the dead-letter exchange.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Max Priority</span> <br/>(<code class="language-plaintext highlighter-rouge">x-max-priority</code> : Int)</td>
					<td>Enables <strong>message prioritization</strong> with a priority range between <strong>1 and 255</strong>. Read more about message priority <a href="/documentation/message-priority">here</a>.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Stream Queue</span> <br/>(<code class="language-plaintext highlighter-rouge">x-queue-type=stream</code>)</td>
					<td>Turns the queue into a <strong>stream queue</strong>, storing messages for history-based retrieval instead of traditional message queuing.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Max Age</span> <br/>(<code class="language-plaintext highlighter-rouge">x-max-age</code> : String)</td>
					<td>Limits how long messages are <strong>retained</strong> in a stream queue. Older messages are automatically deleted.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Message Deduplication</span> <br/>(<code class="language-plaintext highlighter-rouge">x-deduplication-enabled</code> : Bool)</td>
					<td>Enables deduplication to avoid storing duplicate messages.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Deduplication Cache Size</span> <br/>(<code class="language-plaintext highlighter-rouge">x-cache-size</code> : Int)</td>
					<td>Sets how many messages to store for deduplication checks. A larger cache uses more memory.</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Deduplication Cache TTL</span> <br/>(<code class="language-plaintext highlighter-rouge">x-cache-ttl</code> : Int)</td>
					<td>Cache entry time-to-live in milliseconds (default: unlimited).</td>
				</tr>
				<tr>
					<td><span class="font-semibold">Deduplication Header</span> <br/>(<code class="language-plaintext highlighter-rouge">x-deduplication-header</code> : String)</td>
					<td>Specifies what header to guard for deduplication, defaults to x-deduplication-header.</td>
				</tr>
			</tbody>
		</table>
	</div>
</div>

## Pausing a queue

Pausing a queue will pause all consumers. All clients will remain connected but receive no messages from the queue. The “pause queue” option can be found in queue details in the LavinMQ management interface or via the HTTP API.

Pausing a queue using RabbitMQ HTTP API:

`curl -X PUT [http://guest:guest@localhost:15672/api/queues/vhost_name/queue_name/pause](http://guest:guest@localhost:15672/api/queues/vhost/queue/pause)`

Resuming a queue using RabbitMQ HTTP API:

`curl -X PUT [http://guest:guest@localhost:15672/api/queues/vhost_name/queue_name/](http://guest:guest@localhost:15672/api/queues/vhost/queue/pause)resume`

![Management Interface pause queue](/img/docs/pause-queue.png)
