
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is the LavinMQ prefetch?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
            Prefetch controls how many messages a consumer can receive before acknowledging them. Once a consumer receives the specified number of unacknowledged prefetched messages, the broker pauses delivery until at least one message is acknowledged.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What benefits does LavinMQ prefetch have?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>LavinMQ prefetch controls how many messages a consumer can receive before acknowledging them, preventing messages from piling up at the client. By limiting unacknowledged messages, it ensures consumers process data efficiently without getting overwhelmed. This keeps all consumers actively engaged, balancing the workload and optimizing resource usage.</p>
      <img class="border border-[#414040]" src="img/docs/prefetch-1.png" />
      <img class="border border-[#414040]" src="img/docs/prefetch-2.png" />
    </div>
  </div>
</div>

LavinMQ offers two types of prefetch options:

- **Channel Prefetch** – This option limits the number of unacknowledged messages across all channels. It helps control the overall message flow within the connection.
- **Consumer Prefetch** – This option limits the number of unacknowledged messages per consumer.

Prefetch settings apply only when consuming messages from a queue (not when using the `get` method) and only if explicit acknowledgments are required. Pre-fetched messages are not re-queued or delivered to other consumers; they remain assigned to the current consumer and are marked as unacknowledged until processed.

By default, LavinMQ sets the prefetch limit to 65,535 (2¹⁶) messages. This value can be configured using the `default_consumer_prefetch` setting.

### Using prefetch

The `basic.qos` channel property (Quality of Service) sets the prefetch count. The **`basic_qos`** method includes a `global` flag. Setting this flag to:

- **`false`** applies the prefetch count to each consumer individually (consumer prefetch).
- **`true`** applies the prefetch count to all consumers on the channel (channel prefetch).

An unbounded prefetch can be set by `basic_qos(0)`.

Example: Limit the number of unacknowledged messages per consumer to three.

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

channel.basic_qos(prefetch_count=3, global=False)
```

![LavinMQ Prefetch example](img/docs/prefetch-3.png)

Looking for help on setting the prefetch value? Check out our blog, ["How to set the correct prefetch value in LavinMQ"](https://lavinmq.com/blog/correct-prefetch-value), for more information.
