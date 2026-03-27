
Messages in transit between LavinMQ and the consumer might get lost and have to be re-sent. This can occur when a connection fails or during other events that disconnect the receiving service (consumer) from LavinMQ. The use of consumer acknowledgements assures that messages have been delivered. When it comes to message publishing, a publish confirm is the same concept.

### What are Consumer Acknowledgements?

Consumer acknowledgements (acks) provide notice if and when messages between LavinMQ and the consumer have been sent to or received and handled by the intended destination. The response of a consumer acknowledgement setting can also trigger actions that may re-queue and resend messages if needed. There are many ways of using consumer acks, depending on the desired result.

### What benefits do Consumer Acknowledgments have?

If for some reason, the consumer cannot process a message, the use of consumer acknowledgement can prevent a message loss by letting another available consumer receive and handle it, or retrying the process at a later time. Once an acknowledgement is received by LavinMQ, the message can be discarded from the queue.

### When should I use Consumer Acknowledgments?

The use of consumer acknowledgements may be seen as a data safety measure that helps secure your service from unnoticed message loss or protecting services or applications that can not afford to lose any messages.

## Ensure that the message has been sent from LavinMQ

For LavinMQ to securely deliver messages to a consumer, the LavinMQ broker needs to understand when a message is considered successfully sent.
A message can be considered successfully delivered in two ways, _Manual Acknowledgement_, and _Automatic Acknowledgement_.

### Manual Acknowledgement in LavinMQ

One way for LavinMQ to receive message delivery confirmation is when an explicit acknowledgment is received from the consumer. This means that LavinMQ considers the message to be successfully sent once it has been consumed and acked by a consumer. Manual Acks are the default setting for consumer acknowledgements in LavinMQ.

### Automatic Acknowledgment in LavinMQ

Another way for LavinMQ to be noticed about message delivery is via Automatic Acknowledgment. This means that Acks are received once the message is sent out and written to a TCP socket. LavinMQ considers the message to be successfully sent once it has left the broker. Automatic acknowledgements are enabled by setting `auto_ack` to `True`:

{% highlight python %}
default.channel.basic_consume('hello', callback, auto_ack=True)
{% endhighlight %}

Notice that _Automatic Acknowledgement_ should be considered as unsafe, as an unexpected message loss in case of connection failure to the consumer might result in a message being dropped after leaving LavinMQ. Because of this, this setup is not suitable for all workloads.

## Consumer Acknowledgement settings

When a message is consumed it removes the segment-position from the queue's in-memory array and writes the segment-position to an "ack" file.

The manually sent acknowledgement can be positive or negative.

- `basic.ack` is used for _positive acknowledgements_
- `basic.nack` is used for _negative acknowledgements_
- `basic.reject` is used for _negative acknowledgements_ but can not nack bulk messages

### Positive acknowledgments

A positive acknowledgment simply instructs LavinMQ to register a message as delivered and the message can be discarded from the queue.

### Negative Acknowledgments

During times when a consumer is unable to process a delivered message immediately, but other consuming services might be able to, it may be a good option to requeue the message and hand it over for another consumer to handle it.

With setting `basic.reject` or `basic.nack`, LavinMQ will either drop or requeue the messages depending on the desired behavior, which is controlled by the requeue field:

{% highlight python %}
// requeue the delivery
channel.basicReject(deliveryTag, true);
{% endhighlight %}

The broker will requeue the delivery with the specified tag when the field is set to true.

### Difference between Basic.reject and Basic.nack

Basic react was in the original AMQP specification, while basic.nack is an LavinMQ extension, allowing for multiple message negative acknowledgments.

### Requeues and Redelivers with LavinMQ

A requeued message is placed back into its original position in the queue if possible, and can therefore immediately be ready for redelivery. If for some reason all consumers requeue messages and are unable to process the deliveries, they create a requeue/redelivery loop.

Redelivered messages are trackable and can be either scheduled for;

1. Requeuing after a potential delay or
2. Forever rejected and discarded from the loop after x number of times.

LavinMQ detects deviant behaviors of consumers. For example, if the consumer is unavailable to send acks due to connection loss. LavinMQ understands that the message wasn't fully processed and will re-queue the message for other available services to consume, ensuring that no messages are lost even in case of a consumer failure.

### Prevent react loops and poisonous messages in LavinMQ

LavinMQ implements an x-delivery-limit policy that prevents unacked messages from staying in a loop without ever being acknowledged. Messages that are always returned can cause trouble for the service if they grow in numbers.

It is possible to keep track of unsuccessful delivery attempts though the x-delivery-count header. Removed messages can either be removed completely or sent to a dead letter exchange.

### Ack or nack one or many messages

By setting delivery tags, and using the multiple field setting, message acknowledgements for messages using the same channel can be acknowledged.

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Example 1</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Delivery tags 2, 3, 4, 5</td>
        </tr>
        <tr>
					<td>delivery_tag set to 5</td>
        </tr>
        <tr>
          <td><code>Multiple</code> field <code>true</code></td>
        </tr>
        <tr>
          <td><strong>Result: All tags from 2 to 5 acknowledged.</strong></td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Example 2</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Delivery tags 2, 3, 4, 5</td>
        </tr>
        <tr>
					<td>delivery_tag set to 5</td>
        </tr>
        <tr>
          <td><code>Multiple</code> field <code>false</code></td>
        </tr>
        <tr>
          <td><strong>Result: Tags 2, 3, and 4 stay unacknowledged.</strong></td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

The delivery tag is sent while sending the ack/nack. This code will requeue all unacknowledged deliveries up to the delivery tag:

{% highlight python %}
channel.basicNack(deliveryTag, true, true);
{% endhighlight %}
