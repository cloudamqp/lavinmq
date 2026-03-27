
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is the Advanced Message Queuing Protocol?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p><a href="https://www.amqp.org/">AMQP</a> is a binary protocol, designed to handle large amounts of data and is typically the protocol that is used for message-oriented middleware. It allows messages to be sent asynchronously, meaning they don't need to be processed immediately. LavinMQ supports all AMQP client libraries that are available for nearly every programming language.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        Why is AMQP used in LavinMQ?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>AMQP enables advanced messaging patterns like work queues, publish-subscribe, and routing, which make it an ideal choice for LavinMQ. It ensures reliable, asynchronous communication across different platforms, guarantees message delivery, and supports scalability, making it a trusted choice for building flexible and resilient messaging systems.</p>
    </div>
  </div>
</div>

LavinMQ extends the open standard AMQP 0.9.1 specification. AMQP is one of two protocols supported by LavinMQ.

### Main concepts in AMQP

In AMQP 0-9-1, producers send messages to an exchange. These [messages](amqp-messages.md) are routed from the exchange to a queue based on exchange types and routing keys, forming the core structure of its messaging model. Consumers then retrieve messages from the queues.

<img title="AMQP protocol" src="img/docs/amqp-protocol-illustration.png" class="border border-[#414040]"/>

### Queues

A queue provides a place to store messages, acting as a buffer before they are consumed. Before use, a queue must be declared with a specified name or assigned a random one by the broker. During creation, queues can be configured with various attributes that define their lifecycle and behaviour. For example, an auto-delete queue is automatically removed when its last connection closes. In contrast, an exclusive queue can only be used by a single connection and is deleted when that connection ends.

Learn more about how to create a [queue](amqp-queues.md), [queue properties](amqp-queues.md#queue-properties), and [arguments](amqp-queues.md#queue-arguments-optional-properties).

### Exchange types and bindings

A message is routed to a queue depending on the exchange type and bindings between the exchange and the queue. For a queue to receive messages, it must be bound to at least one exchange. LavinMQ provides four main exchange types - direct exchange, fanout exchange, topic exchange, and header exchange.

During creation, an exchange can be declared with several attributes. For instance, it can be marked as durable to survive a broker restart, or it can be marked as auto-delete, meaning that it’s automatically deleted when the last queue is unbound. A binding is a relation between a queue and an exchange consisting of rules that the exchange uses (among other things) to route messages to queues.

Learn more about [exchange types](amqp-exchanges.md) and [bindings](amqp-bindings.md) and [how to create them.](amqp-exchanges.md#the-four-exchange-types)

### Message and content

A message is a package sent from a publisher (sending client) to LavinMQ. Once LavinMQ receives it, the message is passed through an **exchange**, which decides where the message should go based on specific rules. The message is then stored in a **queue**, waiting to be picked up by a **consumer** (receiving client). Each message contains a set of headers, defining properties such as life duration, durability, and priority.

AMQP 0.9.1 includes a built-in message acknowledgment feature that confirms message delivery and processing.

Read more about [messages](amqp-messages.md).

### Connections and channels

A **connection** is like a direct link between your application and LavinMQ over TCP/IP. **Channels** are like virtual pathways within that link, through which messages are actually sent and received. One connection can support multiple channels, allowing your application to send and receive multiple messages at once, without needing separate physical connections.

The documentation provides more information about [connections and channels and their relationships](amqp-connections-and-channels.md).

### Virtual Hosts

Think of **virtual hosts (vhosts)** as separate namespaces within LavinMQ where applications can run independently. Each vhost is like its own private area where **queues** and **exchanges** are created, and these resources don’t overlap with other vhosts. This separation helps control who can access what: users are given permission to specific vhosts, so they can only interact with the resources in their assigned space. This way, you can keep different applications isolated and secure within the same broker.

Learn more about [vhosts](amqp-vhost.md).
