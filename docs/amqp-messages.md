<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a message?
    	</button>
    </h3>
  	<div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
      <p>
				An AMQP message is the data transported from a producer to a consumer through the LavinMQ message broker. It can for example contain information that tells a system to perform a task, reports on a finished task, or simply carries data. Every message consists of a payload and a set of attributes.
			</p>
    </div>
  </div>
</div>

<!--  -->

## Message Components

An AMQP message is built from two essential parts: the payload and a set of attributes. The payload is the actual data, while the attributes are the metadata that tell the broker how to handle it.

### Message payload

This is the actual data to send, it can be any binary format. The broker does not inspect or modify the payload; it only transports it.

#### Payload encoding

Payload encoding is a key used by some client tools and APIs, like the LavinMQ HTTP API, to define how the payload should be interpreted before it is sent to the broker. This key specifies whether the payload should be handled as a `string` (UTF-8 encoded) or as `base64`. Payload encoding is not part of the standard AMQP protocol.

### Delivery mode

In LavinMQ, every message sent is persistent by default. This ensures that all messages are saved to disk and will survive a broker restart, guaranteeing message durability.

### Routing key

The routing key is a message attribute that the exchange looks at when deciding how to route the message to one or more queues, depending on its type.

### Attributes

These are the meta-information that tell the broker how to handle the message. This includes both standard properties and custom headers. Properties are a predefined set of fields from the AMQP protocol, such as content-type and delivery-mode.

#### [Properties]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-gray-300 conf-table">
      <tr>
        <td class="font-semibold">content_type</td>
        <td>
        	Describes the content of the message payload, using a MIME type (e.g., <code class="language-plaintext">application/json</code>, <code class="language-plaintext">text/plain</code>). Content type is used by applications, not core LavinMQ.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">content_encoding</td>
        <td>
        	Specifies the encoding of the payload (e.g., <code class="language-plaintext">gzip</code>, <code class="language-plaintext">deflate</code>). Content encoding is used by applications, not core LavinMQ.
					<br/>
        	<br/>
        	Default: <code class="language-plaintext">application/json</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">expiration</td>
        <td>
					A string representing the message's time-to-live (TTL) in milliseconds.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">priority</td>
        <td>
					A number from 0 to 9 to set message priority. Read more about [message priority](message-priority.md).
        </td>
      </tr>
      <tr>
        <td class="font-semibold">message_id</td>
        <td>
        A unique identifier for the message.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">timestamp</td>
        <td>
					Application-provided timestamp.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">type</td>
        <td>
        The name of the message type, such as a command or event name.
        </td>
      </tr>
			<tr>
        <td class="font-semibold">user_id</td>
        <td>
					The ID of the user who published the message.
        </td>
			</tr>
			<tr>
        <td class="font-semibold">app_id</td>
        <td>
					The name of the application that published the message.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">correlation_id</td>
        <td>
					Used to correlate a response message with a request message.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">reply_to</td>
        <td>
					The name of a queue where the consumer should send a reply. This property is essential for implementing a request-response pattern, allowing a client to receive a specific response to a message it has sent.
        </td>
      </tr>
    </table>
  </div>
</div>

#### Custom headers

Custom headers are a flexible key-value map for adding application-specific metadata.

## Example message

<!-- prettier-ignore -->
{% highlight shell %}
{% raw %}
{
  "properties": {
    "content_type": "application/json",
    "content_encoding": "text/plain"
  },
  "routing_key": "my-key",
  "payload": "my-body",
  "payload_encoding": "string"
}
{% endraw %}
{% endhighlight %}

### Message acknowledgements

Messages in transit between LavinMQ and the consumer might get lost and have
to be re-sent. This can occur when a connection fails or during other events
that disconnect the receiving service (consumer) from LavinMQ. The use of
consumer acknowledgments assures that messages have been delivered. When it
comes to message publishing, a publish confirm is the same concept.

Read more about
[consumer acknowledgments](consumer-acknowledgements.md).

### Message ordering in LavinMQ

Messages in LavinMQ are placed onto the queue in the sequential order in
which they are received. The first message is placed in position 1, the second
message in position 2, and so on. Messages are then consumed from the head of
the queue, meaning that message 1 gets consumed first. This is called the FIFO
method (first in first out).

Read more about [message ordering](message-ordering.md) in
LavinMQ.

### Details on the message store in LavinMQ

All routed messages in LavinMQ are written directly to the disk into
something called a Message Store. The Message Store is a series of files
(segments). A routed message is located in the Message Store, with a reference
from the queue’s index to the [message store](message-store.md).

### Message size

The default message size limit in LavinMQ is 128 MB. This limit is on the message payload and can be configured.
It's generally recommended to keep messages small. Larger messages can consume more memory and disk resources, potentially affecting broker performance and stability.
Read more about [message configurations](configuration-files.md).
