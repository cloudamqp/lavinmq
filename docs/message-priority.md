
Certain messages that publishers send have a higher priority than others, such as an urgent alarm or a task that needs to be handled right away. LavinMQ allows for message priority, meaning that designated messages are placed at the head of the queue and get handled immediately.

### What is Message priority?

Message priorities in LavinMQ ensure that messages of different priorities are handled according to the level of importance. Both the message itself and the queue need to be configured to achieve message priority.

### What benefits does Message Priority have?

Message priority is used to ensure that messages of high priority are handled as fast as possible.

## Description

A queue's priority range is specified when the queue is created, and a message priority is specified when the message is published to this queue.

### Defining Message Priorities

Publishers specify message priority using the priority field in message properties, which becomes effective when the message is published to the queue. The priority range of messages is specified in numbers between 0 and 255. Larger numbers indicate higher priority. If no priority property is set, messages are treated as if priority is 0. If the number is set over 255, they will be handled with maximum priority.

Here follow an example of how to publish a message with priority 8, in Ruby:

<!-- prettier-ignore-start -->

{% highlight ruby %}
exchange.publish("hello",
         :routing_key => queue_name,
         :priority    => 8)
{% endhighlight %}

### Messaging with priority settings

This scenario explains the message flow when message priority is enabled: 

1. A publisher sends a message of high priority to a queue using the priority field of *basic.properties*
2. The prioritized message is added to the same queue (defined as a priority queue) as other messages
3. The prioritized message will be placed at the head of the queue immediately due to the given priority level.

NOTE: Messages with a priority sent to a queue that is not defined as a priority queue will be treated as messages without priorities. 

### Defining Queue Priorities 

To apply message priority, the queue must be defined as a *priority queue*. Setting a queue's priority range must be done when the queue is created. After declaring a queue, the number of priorities cannot be changed. A priority queue is declared via the *x-max-priority* queue argument, where this argument should, as for messages, be between 1 and 255.

Here follow an example of how to declare a queue with a max priority level of 3, in Ruby:

{% highlight ruby %}
queue = channel.queue("bunny_queue", arguments: {"x-max-priority" => 3})
{% endhighlight %}

### Consumer Interaction

Consumers do not take the message priority into account, and simply process messages in the given order. A consumer connected to an empty queue will process all messages coming into it regardless of priorities. Using [prefetch](prefetch.md) (*basic.qos*) can limit the number of messages sent out for delivery and still make the opportunity for the queue to prioritize incoming messages.

## Learning and tips

* Use priority queues with care if you have specified a Queue Length Limit that discards messages from the head of the queue. In that case; make sure that you are not reaching the limit of messages for a priority queue. Messages discarded from the head of the queue might be your most important message, and they will, in this case, be dropped when a low priority message enters the queue.

<!-- prettier-ignore-end -->
