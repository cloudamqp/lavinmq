
Unroutable messages are an issue in a number of ways. They slow down processing times if applications make multiple attempts at delivery. Unroutable messages can also monopolize the system because they must be logged, which takes up resources. There are methods to deal with unrouted messages, which must eventually be handled or dropped.

### What is the LavinMQ alternate exchange?

LavinMQ lets you define an alternate exchange to apply logic to unroutable messages.

### What benefits does LavinMQ alternate exchange have?

It is possible to avoid the complete loss of a message by properly handling unroutable messages.

### How should I use LavinMQ alternate exchange?

No matter how careful you are, mistakes can happen. For example, a client may accidentally or maliciously route messages using non-existent routing keys. To avoid complications from lost information, an easy, safe backup is to collect unroutable messages in an LavinMQ alternate exchange.

### What happens to unroutable messages in LavinMQ?

To avoid the complete loss of a message, LavinMQ handles unroutable messages in two ways based on the mandatory flag setting within the message header:

1. The server either returns the message when the flag is set to `true` or
2. Silently drops the message when set to `false`.

Applications can log returned messages, but logging does not provide a mechanism for dealing with an unreachable exchange or queue.

Whether messages return or not, unroutable messages can:

- Be returned to a broken application that constantly resends them.
- Be the result of malicious activity aimed at causing LavinMQ to become unresponsive.
- Cause the loss of mission-critical data

<div class="border border-[#414040] rounded-xl px-8 py-0">

<p class="mb-8">
<strong>Example</strong>: Think of a hospital that uses LavinMQ to help store patient information in a database. Medical devices typically push vital data to a small server for processing, which then forwards the information to the cloud for storage. In this case, LavinMQ can be used to push data to the cloud and be employed to facilitate streaming.
</p>
<p>
Should messages drop before they end up in LavinMQ, doctors could lose vital information. This loss could impact diagnosis and treatment. The situation could become even worse if the system breaks and overloads the resources handling messages related to more than one patient.
</p>
</div>

Set an alternate exchange using policies or within arguments when declaring an exchange.

<!-- img -->

The alternate exchange attaches to one or more primary exchanges. Set the exchange type to fanout to ensure that rejected messages always route to the alternate queue. Be advised that there is no way to catch invalid exchange names outside of the mandatory flag.

## Creating the Alternate Exchange

Defining an alternate exchange is no different than defining any other part of your topology. The primary exchange forwards the message to the alternate exchange, which sends it to the alternate queues.

Create a fanout exchange and attach a queue:

{% highlight python %}
conn_str = os.environ["AMQP_URI"]
params = pika.URLParameters(conn_str)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare("alt_exchange", "fanout")
channel.queue_declare("alt_queue")
channel.queue_bind("alt_queue", "alt_exchange")
{% endhighlight %}

Any message sent to the _alt_exchange_ winds up in the _alt_queue_. Since we use a fanout exchange, the message ends up in all queues attached to `alt_exchange`.

## Attaching the Alternate Exchange

Use policies, the UI, or message headers to set the alternate exchange. You must use the command line to attach an alternate exchange after declaring the primary exchange.

Use message arguments to set the exchange:

{% highlight python %}
args = {"alternate-exchange": "alt_exchange"}
channel.exchange_declare("primary_exchange", "direct", arguments=args)
{% endhighlight %}

Alternately, use the command line to specify the alternate exchange:

{% highlight shell %}
lavinmqctl set*policy AE “primary*\*” ‘{“alternate-exchange”: “alt_exchange”}’
{% endhighlight %}

LavinMQ sends all messages from the `primary_exchange` to the `alt_exchange` when the routing key is invalid. Unrouted messages end up in the `alt_queue`.

## What happens when the mandatory flag is set with an alternate exchange?

There is still a chance that messages won’t be routed if an alternate exchange is provided. For instance, the service may be unreachable, the alternate queue may not be specified correctly, or a non-existent exchange might be specified.

Setting the mandatory flag will catch these unrouted messages. Keep in mind that, if the message routes to the alternate exchange, LavinMQ marks the message as delivered.
