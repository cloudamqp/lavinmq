
LavinMQ has arguments and properties that can be used to define behaviors. Properties are defined by the AMQP protocol and included in LavinMQ. Arguments can be any key-value pair and are used for feature extensions. Some properties are mandatory while others are optional. All arguments are optional.

### What is a Property in LavinMQ?

Properties are specified in the AMQP protocol 0.9.1. An example of a property for a queue is “passive”, “durable” and “exclusive”.

### What is an Argument in LavinMQ?

An argument is an optional feature for defining behaviors, implemented by the LavinMQ server. These arguments are also known as x-arguments and can sometimes be changed after queue declaration. An example of an argument for messages and queues are “TTL”.

### What Benefits do Arguments and Properties have?

Defining properties and arguments can be beneficial, and in some cases crucial. Properties and Arguments do provide an easy and secure approach to keeping your LavinMQ instance tidy and healthy - a way to minimize unnecessary resource usage or letting a faulty client cause issues by publishing millions of messages to a single queue.

## Properties and Arguments

Properties and Arguments can be defined for [Queues](amqp-queues.md), [Exchanges](amqp-exchanges.md), and [Messages](amqp-the-protocol.md#message-and-content) in LavinMQ.

Properties are specified in the AMQP protocol 0.9.1. and implemented by LavinMQ.

Arguments are additional, optional features to the mandatory properties. Some Arguments can be dynamically changed after the creation of the queue or exchange.

### Queue Properties and Arguments

Examples of Queue properties are “passive”, which determines if the queue already exists, and “durable” which tell if the queue remains when a server restarts.

Example Queue Arguments are [“x-max-priority”](message-priority.md), which is setting a maximum number of priorities, and “x-message-ttl” which is setting managing queue TTL.

### Exchange Properties and Arguments

<!-- See Exchange Properties and Arguments can be found here. -->

Examples of Exchange properties are “durable”, which tells if the exchange remains when a server restarts, and “internal” which tells that the exchange can not be used directly by publishers.

Example Exchange Arguments are “x-dead-letter-exchange” and “x-dead-letter-routing-key” used by the dead letter exchange.

## Set arguments and properties

A property can be set while creating the queue, code-wise, via the management interface, or via policies.

### Code example

A queue’s arguments are normally set on a per queue basis when the queue is declared by the client. How the arguments are set varies from client to client.

A queue can be marked as durable, which specifies if the queue should survive an LavinMQ restart. Setting a queue property as durable only means that the queue definition will survive a restart, not the messages in it. Create a durable queue by specifying durable as `True` during the creation of the queue.

Code example of how to declare a durable queue:

{% highlight python %}
channel.queue_declare(queue='test', durable=True)
{% endhighlight %}

Dead letter exchanges are no different than other exchanges except added arguments. Code example of how to apply arguments for an exchange:

{% highlight python %}
channel.queue_declare("test_queue", arguments={
"x-dead-letter-exchange": "dlx_exchange", "x-dead-letter-routing-key": "dlx_key"})
{% endhighlight %}

### Via the LavinMQ Management Interface

A queue or exchange can be created via the Management Interface. A message can also be sent through the management interface.

When manually creating a queue through the [LavinMQ Web Management Interface](management-interface-overview.md) the arguments are set in a free text field and can be added to it with quick links. Properties can be set as true or false.

<img src="img/docs/add-queue-dm-2x.png" style="margin-bottom: 8px;" alt="Setting  arguments via Management Interface" class="border border-gray-200 rounded-xl"/> <small class="mt-2 block text-center">Illustration of a queue created with the durability property set to true, and two arguments, x-max-length and x-message-ttl</small>

<img src="img/docs/publish-message-dm-2x.png" style="margin-bottom: 8px;" alt="Setting  arguments via Management Interface" class="border border-gray-200 rounded-xl"/> <small class="mt-2 block text-center">Illustration of a message published with TTL expiration set to 36000000 milliseconds</small>

## Arguments and Policies

Arguments can also be set using [Policies](policies.md).

To set arguments, the use of policies is recommended. Policies make it possible to configure arguments for one or many queues at once, and the queues will all be updated when you’re updating the policy definition. To reduce the overhead work of configuring every single queue and exchange with arguments, the use of policies are perfect. Policies enable a way to configure multiple queues or exchanges in a consistent way, reducing the risk for sloppy mistakes in the configuration. A queue can only be applied by one policy simultaneously, but there is a priority system along with the regex recognition in order to manage several policies.

![Setting arguments through policies](img/docs/lavinmq-arguments-policy.jpg)

![Updating argument through policies](img/docs/lavinmq-policy-argument-update.jpg)
