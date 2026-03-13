
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What are LavinMQ Streams?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          LavinMQ Streams buffer messages from producers for consumers, like traditional queues. However, unlike queues, streams are immutable; messages can't be erased; they can only be read. While retention settings offer some control, Streams are designed for long-term message storage. This allows consumers to subscribe and read the same message multiple times.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        When to use LavinMQ Streams?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>Streams are great for:</p>
      <ul>
        <li><strong>Fan-out architectures:</strong> Where many consumers need to read the same message. Implementing a fan-out arrangement with LavinMQ Streams is remarkably straightforward. Merely declare a Stream and bind as many consumers as required.
        </li>
        <li><strong>Replay & time-travel:</strong> Where consumers need to re-read the same message or start reading from any point in the Stream.</li>
      </ul>
    </div>
  </div>
</div>

<!-- prettier-ignore-start -->

Client applications could talk to a Stream via an AMQP client library, just as they do with queues. Like queues, there are three steps to working with LavinMQ Streams:

1. Declare a Stream
2. Publish messages to the Stream
3. Consume messages from the Stream

### 1. Declaring a stream

Streams are declared with the AMQP client libraries the same way queues are created. Set the **`x-queue-type`** queue argument to **`stream`**, and provide this argument at declaration time. Also make sure to declare the Stream with **`durable=true`** . LavinMQ does not allow the creation of non-durable Streams.

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
    queue='test_stream',
    durable=True,
    arguments={"x-queue-type": "stream"}
)

connection.close()
```

### 2. Publishing to the stream

As an example, below, the previous snippet has been extended to publish a message to the **`test_stream`** declared.

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
    queue="test_stream",
    durable=True,
    arguments={"x-queue-type": "stream"}
)

channel.basic_publish(
	exchange="", # Use the default exchange
	routing_key="test_stream",
	body="Hello World"
)

connection.close()
```

In addition to the **`x-queue-type`** argument, Streams support three additional queue arguments that can be specified at queue declaration or via a policy.

- **`x-max-length`** - Sets the maximum number of messages allowed in the stream at any given time. See [**retention**](/documentation/streams#data-retention). Default: not set.
- **`x-max-length-bytes`** - Sets the maximum size of the Stream in bytes. See [**retention**](/documentation/streams#data-retention). Default: not set.
- **`x-max-age`** - This argument will control how long a message survives in a LavinMQ Stream. The unit of this configuration could either be in years (Y), months (M), days (D), hours (h), minutes (m), or seconds (s). See [**retention**](/documentation/streams#data-retention). Default: not set.

### 3. Consuming from the stream

Three key things to note about consuming messages from a Stream queue:

- An offset can be specified to start reading from any point in the Stream.
- Consuming messages in LavinMQ Streams requires setting the QoS prefetch.
- LavinMQ does not allow consuming messages from a Stream with **`auto_ack=True`**

As mentioned, when consuming from a Stream , clients have the ability to specify a starting point by using an offset. The **`x-stream-offset`** consumer argument controls this behaviour.

LavinMQ supports the following offset values:

- **`first`**: This starts reading from the beginning of the Stream, **`offset 1`**.
- **`last`**: This starts reading from the beginning of the last segment file.
- **`next`**: Initiates reading from the latest offset once the consumer is initiated - essentially, the **`next`** offset won’t attempt to read all the messages present in the Stream prior to the consumer’s activation.
- **A specific numerical offset value**: Clients can specify an exact offset to attach to the log. If the specified offset does not exist, it will be adjusted to either the start or end of the log accordingly.
- **Timestamp:** This value represents a specific point in time to attach to the log.

**Note:** Connecting a consumer to a Stream that already contains messages, without specifying an offset, will configure the consumer to read from beginning of the stream.

**Example**: Sets the consumers prefetch to 100 and reads from the **`test_stream`** via two approaches:

- Reading from the 5000th offset
- Reading from the first message in the Stream.

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=100)

# Reading from the beginning of the Stream.
channel.basic_consume(
    "test_stream",
    callback,
    auto_ack=False,
    arguments={"x-stream-offset": 1}
)
connection.close()
```

<div class="my-6"></div>

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=100)

# Reading from the 5000th offset
channel.basic_consume(
    "test_stream",
    callback,
    auto_ack=False,
    arguments={"x-stream-offset": 5000}
)
connection.close()
```

## Offsets

Offsets in Streams serve a similar purpose as indexes in arrays. To start reading from a specific index in a Stream, simply specify an offset in the consumer query. Essentially, every message in a Stream has an offset. For instance, the following image illustrates how messages and their corresponding offsets would appear in a given Stream:

![Streams](/img/docs/lavinmq-streams-offsets.png)

## Data retention

A stream can be configured to discard old messages using data retention settings. Retention settings allow a Stream to automatically remove messages once they exceed a specified size or age.

Message truncation involves deleting an entire segment file. Instead of storing messages in a single large file, LavinMQ uses smaller 8MB files called segment files. When truncation occurs, a segment file and all its messages are deleted.

**Note:** Retention is evaluated per segment. A Stream applies retention limits only when an existing segment file reaches its maximum size (default: 8MB) and is closed in favour of a new one.

A Stream’s retention strategy can be configured using a **size-based**, **time-based**, or combined approach.

### Size-based retention strategy

Here, the Stream is set up to discard segment files once the total size or number of messages in the Stream reaches a specified upper limit. Setting up the sized-based retention strategy requires providing any of the following arguments when declaring the Stream:

- **`x-max-length-bytes`**
- **`x-max-length`**

Example: Setting stream capacity to one thousand messages.

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
    queue='test_stream',
    durable=True,
    arguments={"x-queue-type": "stream", "x-max-length": 1000}
)

connection.close()
```

In the example above, if the Stream reaches the 1000 message limit, old segment files are deleted until the limit is met.

### Time-based retention strategy

The Stream truncates segment files exceeding a set age. To enable time-based retention, specify **`x-max-age`** when declaring the Stream or via policy.

Units: years (Y), months (M), days (D), hours (h), minutes (m), or seconds (s).

**Example**: Expire messages that have been in the queue longer than 30 days

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
    queue='test_stream',
    durable=True,
    arguments={"x-queue-type": "stream", "x-max-age": "30D"}
)

connection.close()
```

The snippet demonstrates a time-based retention strategy, where segment files older than 30 days are discarded.

### Automatic offset tracking

When consuming from a stream, it is possible to configure LavinMQ to handle tracking of offsets. Offset tracking helps a consumer remember where it left off in a stream.

For example, if a consumer has processed messages up to position 5000, that means it has successfully handled everything before that point. If it stops and later comes back online, it can resume from position 5001 instead of starting over.

**How it works**

Offset tracking can be enabled in two ways:

1. By setting **`x-stream-offset = null`** .
2. Or by setting **`x-stream-automatic-offset-tracking = true`**.

In both cases, setting the  **`consumer tag`**  is _required_.

When setting **`x-stream-offset = null`**, the consumer will start reading the stream from the beginning at first connection. At a later reconnect the consumer will resume from where it left off automatically.

It is also possible to combine the automatic tracking with all possible **`x-stream-offset`** values by providing both a valid **`x-stream-offset`** and setting **`x-stream-automatic-offset-tracking = true`**. In this case, the consumer will start reading the stream from the provided **`x-stream-offset`** at first connection. At a later reconnect the consumer will resume from where it left off automatically.

**Important:** When a message is acknowledged, the tracked offset is set to the offset of the **latest message delivered** to the consumer, not the offset of the specific message being acknowledged. To avoid skipping messages, consumers using automatic offset tracking should process messages **sequentially with a prefetch (QoS) of 1**. If the consumer disconnects after receiving multiple prefetched messages but before acknowledging all of them, the unacknowledged messages will be skipped upon reconnection.

For use cases requiring parallel processing or higher prefetch values, manage offsets manually using the `x-stream-offset` consumer argument instead.

The snippet below shows a consumer script with **`x-stream-automatic-offset-tracking`** set to **`true`** and consumer **`tag`** set to **`test-consumer`**.

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
    print(f"[✅]  { msg }")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    "test_stream",
    callback,
    auto_ack=False,
    consumer_tag="**test-consumer"**
    arguments={"x-stream-automatic-offset-tracking": true}
)

connection.close()
```

Server side tracking can be disabled at any point by reconnecting the consumer and providing a valid value for **`x-stream-offset`** (and not providing **`x-stream-automatic-offset-tracking`**).

### Stream filtering

LavinMQ Streams support server-side filtering, allowing consumers to receive only the messages they need without scanning the entire stream. This reduces network usage and improves efficiency, making it easier to work with specific data.

**How it works**

When publishing messages to a stream, you can add filter values in several ways:

1. By setting the **`x-stream-filter-value`** header on the message
2. By using any other message header as a filter criterion

When consuming from a stream, you can filter messages by providing the **`x-stream-filter`** argument. This filter can be:

- A string (comma-separated values)
- An `AMQP::Table` of filter key-value pairs
- An array combination of the above options

For example, to publish a message with filter values:

```python
# Publishing with x-stream-filter-value header
properties = pika.BasicProperties(
    headers={"x-stream-filter-value": "application1,info"}
)
channel.basic_publish(exchange="", routing_key="test_stream", properties=properties, body="Hello World")

# Publishing with custom headers that can be filtered against
properties = pika.BasicProperties(
    headers={"log_level": "error", "service": "payment-api"}
)
channel.basic_publish(exchange="", routing_key="test_stream", properties=properties, body="Hello World")
```

Consuming messages with filters: 
```python
channel.basic_consume(
    "test_stream",
    callback,
    auto_ack=False,
    arguments={"x-stream-filter": "application1,info"}
)
```

When consuming, you can filter in multiple ways:

```python
# Filtering by x-stream-filter header values
arguments = {"x-stream-filter": "application1,info"}

# Filtering by specific header fields
arguments = {"x-stream-filter": {"log_level": "error", "service": "payment-api"}}

# To receive messages without a specific filter
arguments = {"x-stream-match-unfiltered": True}
```

You can also control how multiple filters are matched by setting the **`x-filter-match-type`** argument:
- **`all`** (default): Message must match all provided filters
- **`any`**: Message must match at least one provided filter

```python
# Message must match either log_level OR service
arguments = {
    "x-stream-filter": {
        "log_level": "error",
        "service": "payment-api",
    },
    "x-filter-match-type": "any"
}
```

If no filter is provided when consuming, all messages in the stream are returned as if no filters existed.

### Feature comparison with queues

Due to their design and intended use cases, Streams lack some features found in normal queues.The table below compares queues vs streams.

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Features</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Queue</span></th>
          <th><span class="font-semibold">Stream</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Non-durability</td>
					<td class="border-r border-[#414040]">Queues can be non-durable</td>
          <td>A stream must be durable. <code>durable: false</code> will fail.</td>
				</tr>
        <tr>
					<td>Exclusivity</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported</td>
				</tr>
        <tr>
					<td>Consumer priority</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-priority</code> argument will fail.</td>
				</tr>
        <tr>
					<td>Single Active Consumer</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-single-active-consumer</code> argument will fail.</td>
				</tr>
        <tr>
					<td>Consumer acknowledgement</td>
					<td class="border-r border-[#414040]">Not required</td>
          <td>Consumers must acknowledge messages when reading from Streams. <code>noAck: true</code> will fail.</td>
				</tr>
        <tr>
					<td>Dead Letter Exchange</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-dead-letter-exchange</code> argument will fail.</td>
				</tr>
        <tr>
					<td>Per-message TTL</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-expires</code> will fail.</td>
				</tr>
        <tr>
					<td>Delivery limit</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-delivery-limit</code> will fail.</td>
				</tr>
        <tr>
					<td>Reject on overflow</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported. <code>x-overflow</code> will fail.</td>
				</tr>
        <tr>
					<td>Channel Prefetch</td>
					<td class="border-r border-[#414040]">Not required</td>
          <td>A consumer must specify its channel prefetch.</td>
				</tr>
        <tr>
					<td>Global Prefetch</td>
					<td class="border-r border-[#414040]">Supported</td>
          <td>Not supported</td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

<!-- prettier-ignore-end -->
