
<!-- prettier-ignore-start -->

Messages in transit might get lost if the broker goes down before LavinMQ has the chance to receive it. This can happen in the event of a connection failure or during a broker restart. Sometimes you want these messages to be retransmitted from the publisher to the LavinMQ server. Publish confirm lets the client knows when to retransmit messages. [Consumer acknowledgements](consumer-acknowledgements.md) are the same concept but on the consumer side; the client confirms when it has received a message from the LavinMQ.

### What is Publisher Confirms?

LavinMQ can acknowledge that a message has been received, with the use of publisher confirms.
### What benefits do Publisher Confirms have?

The client can ensure that a message has been received by LavinMQ.

### When should I use Publisher Confirms?

Use publisher confirms when you must not lose messages and when you want to ensure that LavinMQ has received the message sent by the publisher.

## Ensure that LavinMQ has received a message

Publisher confirms are not enabled by default. It can be enabled by turning on delivery confirms on the channel, which tells LavinMQ and the client to start counting messages:

{% highlight python %}
channel.confirm_delivery()
{% endhighlight %}

Once a channel is in confirm mode, the publisher should expect to receive `basic.ack`. LavinMQ can confirm messages as it handles them by sending a basic.ack on the same channel. The delivery-tag field contains the sequence number of the confirmed message.

{% highlight python %}
try:
  channel.basic_publish(exchange='test',
                        routing_key='test',
                        body='Hello World!',
                        properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1)):
  print('Message publish was confirmed')
  except pika.exceptions.UnroutableError:
  print('Message could not be confirmed')
{% endhighlight %}

The basic.ack is sent when a message reaches LavinMQ and has been accepted by all the queues. The broker may also set the multiple field in basic.ack to indicate that all messages up to and including the one with the sequence number have been handled.

Note: A persistent message is confirmed when written to disk or ack:ed on all the queues it was delivered to.

## Negative Acknowledgment
When LavinMQ is unable to handle messages successfully, it can reply with a `basic.nack`. Fields in a basic.nack correspond with those in a basic.ack, and the requeue field is ignored. When LavinMQ nacks one or more messages, this indicates to the client that it was unable to process the messages and will not retry. At that point, the client may choose to republish them or take some other action.

<!-- prettier-ignore-end -->
