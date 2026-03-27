
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a delayed message exchange?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>A <strong>delayed message exchange</strong> in LavinMQ temporarily holds messages for a specified delay period before routing them to their destination. This delay is set using the <code class="language-plaintext">x-delay</code> header in the message.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        When to use the delayed message exchange?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>Use it when you need to control message delivery timing, such as in scheduled tasks, retry attempts, or time-sensitive workflows.</p>
    </div>
  </div>
</div>

## Delayed message exchange

LavinMQ can delay message delivery by adding a controlled waiting period between the time an exchange receives a message and when it is routed to its queue.

When a delayed exchange is declared, LavinMQ automatically creates an internal queue that stores the delayed messages. This internal queue is configured with a `dead-letter-exchange` argument that handles message routing once the delay period expires.

![Delayed Exchange](img/docs/delayed-exchange.png)

When a message is published to a delayed exchange, it is routed to an internal queue and stored with a delay specified in the message header, using the `x-delay` header. The internal queue assigns a Time-to-Live (TTL) equal to the value of `x-delay`. The message remains in the queue during this TTL until the delay period expires. Unlike a traditional FIFO queue, where messages are processed in order, this internal queue prioritises messages based on their individual delay values. This means messages can expire independently, without waiting for other messages to expire first. After expiration, the internal queue re-publishes the message to a dead-letter exchange set up to match the delayed exchange. Since the message is re-published with no delay, it will be routed directly to the queues bound to the routing key.

### Using the delayed exchange

**Example: Declaring a delayed exchange in LavinMQ**

To configure a delayed exchange, you must specify the `x-delayed-type` argument, which indicates the underlying exchange type. LavinMQ supports allexchange types (`direct`, `fanout`, `topic`, and `headers`) for delayed messaging. This flexibility allows delayed exchanges can seamlessly integrate with various routing mechanisms.

<!-- prettier-ignore-start -->

For example:

```ruby
require "amqp-client"
require "amq-protocol"

# Connect to LavinMQ
connection = AMQP::Client::Connection.new("amqp://localhost")
channel = connection.channel

# Define the exchange details
exchange_name = "delayed_fanout_exchange"
exchange_type = "x-delayed-message"
arguments = {
  "x-delayed-type" => "fanout"  # Specify the underlying exchange type
}

# Declare the delayed exchange
channel.exchange_declare(
  exchange: exchange_name,
  type: exchange_type,
  durable: true,
  arguments: arguments
)

puts "Delayed exchange '#{exchange_name}' declared successfully."

# Close the channel and connection
channel.close
connection.close
```

This example declares an exchange named “`delayed_fanout_exchange`” that uses the fanout strategy. Messages will be delayed by the specified duration in the message before being broadcast to all queues bound to this exchange.

**Example: Publish a message to LavinMQ**

To publish a message with a delay, include the delay time in the `x-delay` header when sending the message:

```ruby
require "amqp-client"
require "amq-protocol"

# Connect to LavinMQ
connection = AMQP::Client::Connection.new("amqp://localhost")
channel = connection.channel

message = "This is a delayed message for LavinMQ."
headers = {
  "x-delay" => 2500_i64  # Delay in milliseconds, required as Int64
}

# Publish the message to the delayed exchange
channel.basic_publish(
  exchange: exchange_name,
  routing_key: "", # Fanout exchanges ignore the routing key
  body: message,
  properties: AMQP::Client::BasicProperties.new(headers: headers)
)

puts "Message published to LavinMQ with a 2,500ms delay."

# Close the channel and connection
channel.close
connection.close
```

The target queue attached to the delayed_route receives the message after 2500 milliseconds. Set the header for whichever delay is needed.

<!-- prettier-ignore-end -->
