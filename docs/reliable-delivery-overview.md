
In LavinMQ, reliable message delivery is vital to prevent message
loss in specific scenarios like server failures, network interruptions,
or high message traffic.

To ensure data safety, LavinMQ offers various mechanisms for reliable message
delivery. You can achieve this by employing one or a combination of the
following methods:

- Publisher Confirms
- Consumer Acknowledgements
- Durable Queues and Persistent Messages
- Transactions

## Publisher Confirms

When using publisher confirms, the producer (publisher) receives acknowledgments
from LavinMQ after successfully delivering messages to the intended queues.
This feedback mechanism assures the producer that messages have been reliably
accepted by LavinMQ.

For more information, you can read our documentation on
[publisher confirms](/documentation/publisher-confirms).

## Consumer Acknowledgements

On the other hand, when consumers consume messages from a queue,
they can also send an acknowledgement to LavinMQ confirming the
successful processing of messages. If an acknowledgment
isn't received, LavinMQ assumes something went wrong and the message is left in
the queue.

For more information, you can read our documentation on
[consumer acknowledgements](/documentation/consumer-acknowledgements).

## Durable Queues and Persistent Messages

Declaring queues as "durable" ensures that they survive server restarts,
safeguarding your messages from loss.

In LavinMQ, messages are persistent by default. LavinMQ would generally ignore the
`delivery_mode` argument, if passed.

## Transactions

Even though publisher confirms and consumer acknowledgements offer some
level of message persistence guarantee in LavinMQ, that guarante is not 100%.

Transactions in LavinMQ offer the highest level of message persistence guarantee.

To learn more about transactions in LavinMQ and how they offer the highest level
of message durability guarantee compared to pubisher confirms and consumer
acknowledgements, check out our documentation on [transactions](/documentation/transactions).

## Wrap up

By leveraging these methods in LavinMQ, you can establish a robust and
reliable message delivery system, preventing data loss and ensuring your
messaging infrastructure performs optimally even in challenging circumstances.
