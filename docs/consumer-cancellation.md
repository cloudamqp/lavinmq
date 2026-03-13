
It is easy to kill an LavinMQ consumer by destroying the channel but this leaves messages undelivered. Performing LavinMQ consumer cancellation the right way requires handling shutdown gracefully.

An LavinMQ consumer waits for messages interacts with the broker, and processes received data. There is an extensive amount of communication between the two components. Message acknowledgement and prefetching require a degree of communication. Brokers, consumers, and publishers interact in an intricate dance over exchanges and queues. A graceful shutdown means responding to any communications, processing messages in transit, and then terminating the connection gracefully.

## Cancelling a LavinMQ Consumer

The official way to cancel a consumer in LavinMQ is over its own channel or connection. Externally stopping the process often leads to delays and can cause messages to drop or redeliver.

Using the _basic.cancel_ command built into AMQP allows your consumer to finish processing messages in flight. It also tells the broker to stop sending messages to the target. This form of graceful shutdown gives you a powerful tool to avoid unexpected bugs.

## Handling Cancel Commands

LavinMQ libraries allow you to easily implement logic to terminate consumers. Clients in Java, .Net, Python, Rust, and many other languages expose the basic cancel command for this purpose. Call the command from a client or the consumer itself.

The process is similar for most clients. The Java client includes a specific cancellation handler. You can do the same in Pika’s asyncio consumer by overriding the following methods:

<!-- prettier-ignore-start -->

{% highlight python %}
{% raw %}
def start_consuming(self):
        self.add_on_cancel_callback()
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

def on_consumer_canceled(self):
        LOGGER.info('Consumer Cancelled')
{% endraw %}
{% endhighlight %}

The _on_consumer_canceled_ method provides a custom callback for dealing with standard cancellations.

## Cancelling the Consumer
You can cancel a consumer after receiving a certain message or store the tag in a way that it can be accessed by your entire system. The
(basic_consume)[https://pika.readthedocs.io/en/stable/modules/channel.html] method in Pika allows you to create a custom tag as well.

{% highlight python %}
{% raw %}
  class MyAsyncConsumer:

      def __init__(self):
          self._channel = None

     def handle_message(self, _unused_channel, basic_deliver, properties, body):
         print(“Handling Message”)

      def start_consuming(self):
          self_channel.basic_consume(“my_queue”, self.handle_message), consumer_tag=”my_custom_tag”)

{% endraw %}
{% endhighlight %}

You can now cancel the consumer from the same or a different client using the tag _my_custom_tag_:

{% highlight python %}
{% raw %}
  def handle_cancel(msg):
    print(“Canceled”)

  channel.basic_cancel(“my_consumer_tag”)
{% endraw %}
{% endhighlight %}

In some clients, it is possible to avoid receiving a response from the broker with no-wait. However, if using a publisher, more messages may be passed to the consumer if not handled carefully. Pika does not offer this option.

## LavinMQ Consumer Cancellation the Right Way
An LavinMQ consumer does not operate in a vacuum. There are many factors that influence how a consumer runs. Gracefully terminate the consumer to avoid lost messages and other complications. Using the basic.cancel command is a best practice you should not avoid.

<!-- prettier-ignore-end -->
