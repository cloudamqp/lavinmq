
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is consumer priority?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          When consumers connected to LavinMQ have different priorities, messages are delivered first to those with higher priority and <a href="#consumer-capacity">available capacity</a>. Lower-priority consumers receive messages only when higher-priority ones are blocked.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        When to use consumer priority?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>A typical scenario where consumer priority proves beneficial is resource management. Assign higher priorities to consumers with greater computing power or available resources, enabling efficient handling of critical tasks.</p>
    </div>
  </div>
</div>

Assign priority to a consumer by setting the `x-priority` argument in the `basic.consume` method to an integer value. If no value is specified, the priority is 0. Higher numbers indicate higher priority, with both positive and negative numbers available.

<!-- prettier-ignore-start -->

**Example:** Set the priority on a consumer to 10

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")
		ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    "test_queue",
    callback,
    auto_ack=False,
    arguments**=**{
      'x-priority':10
    }
)
connection.close()
```

### Consumer capacity

A consumer has _capacity_ when ready to receive a message. It becomes blocked when it can't, such as when the prefetch limit of unacknowledged messages is reached.
<br />A consumer is either ready or blocked at any given moment.

### Multiple consumers with the same priority

If multiple consumers share the same priority, LavinMQ sends messages to the first consumer bound to the queue by default, rather than using a round-robin approach. To ensure fair message distribution among same-priority consumers, set the prefetch value to 1.

### Single active consumers and priority consumers

When the single active consumer feature is enabled, priority consumers cannot be used on that queue.

<!-- prettier-ignore-end -->
