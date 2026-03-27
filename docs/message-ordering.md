
Messages in LavinMQ are placed onto the queue in the sequential order in which they are received. The first message is placed in position 1, the second message in position 2, and so on. Messages are then consumed from the head of the queue, meaning that message 1 gets consumed first. This is called the FIFO method (first in first out). A message received by the broker is _enqueued_ (added to the queue). If there is a consumer subscribed to the queue, a message is _dequeued_ (removed from the queue) when it is sent to the consumer.

![LavinMQ FIFO - First in, First out](img/docs/lavinmq-fifo-enqueue-dequeue.jpg)

## Factors that influence message ordering

Even though LavinMQ and the AMQP protocol follow the FIFO method, message ordering might be altered by configurations or application design. Here are several scenarios that affect message ordering:

### Message redeliveries

> These redeliveries happen automatically after negative consumer acknowledgments and channel closures.

Order processing can be broken when messages are redelivered. Causes of message redelivery include: a consumer nacks or rejects a message, a consumer’s connection or channel is closed. In these cases, the message will be returned to the front of the queue and LavinMQ will then send it to the next available consumer. It is possible that consumers have already processed messages that were initially behind the redelivered messages in the queue.

The amount of resends for unhandled messages can be adjusted, to prevent the queue from filling up. `x-delivery-limit` is configurable as a policy or queue argument. If the x-delivery-limit is set, the message will be dead-lettered or deleted after X amount of redeliveries.
Every redeliver of a message to a consumer is counted. When the redelivery count exceeds the given limit, the message is `dead-lettered` or deleted.

![LavinMQ Message Redelivery](img/docs/lavinmq-dead-lettering.jpg)

### Multiple competing consumers

In a case where there are multiple receiving services, or more than one consumer for the queue, dequeued messages will be sent to consumers in FIFO order. If all of the consumers have equal priorities (see below for a discussion on consumer priority), they will be picked to receive messages on a round-robin basis.

In the illustration below, chronological message processing is not guaranteed. Messages 1, 2, and 3 are directed to different consumers, and LavinMQ cannot force the consumer that received message 1 to finish processing it before the other consumer processes message 2 (and the same for the consumer processing message 3).

![LavinMQ multiple consumers](img/docs/lavinmq-multiple-competing-consumers.jpg)

### Multiple connections or channels

Publishing applications can assume that messages published on a single channel will be enqueued in their published order. When publishing happens on multiple connections or channels, their messages will be routed concurrently and interleaved. See the illustration below.

![LavinMQ FIFO - First in, First out](img/docs/lavinmq-multiple-connections-channels.jpg)

### Message priority

LavinMQ allows special messages to be sent with a [higher priority](message-priority.md) than normal messages. This can be useful in cases such as an urgent alarm or a task that must be handled immediately. These designated messages are placed at the head of the queue and will be directed to the next available consumer. This configuration overrides the FIFO message ordering.

Note: Setting a Redelivery-limit (_x-delivery-limit_) will ensure that messages are only delivered X times according to the configuration mentioned above.

## Conclusion

LavinMQ can guarantee message order under special conditions only. Messages come from a single channel and are routed to a single consumer channel. If either channel is closed or messages are rejected, message ordering is not maintained.

Important messages can be enqueued at the front of the queue so that they are processed before ‘standard’ messages.
