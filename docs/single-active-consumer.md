
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is the Single Active Consumer?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>A Single Active Consumer permits only one consumer to process messages from a specific queue. If the active consumer becomes unavailable, the queue automatically fails to the next available consumer.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What benefits does the Single Active Consumer have?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <ul>
        <li>Guarantees that messages are processed in the exact order they arrive in the queue.</li>
        <li>Ensures a single consumer executes a task at a time.</li>
        <li>Eliminates conflicts from multiple consumers processing the same message.</li>
      </ul>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect3" id="accordion3">
        When to use a Single Active Consumer?
      </button>
    </h3>
    <div id="sect3" class="accordion-content" role="region" aria-labelledby="accordion3">
      <ul>
        <li>Maintaining event order is critical (e.g., when processing ticket requests).</li>
        <li>Preventing conflicts from parallel processing is necessary.</li>
        <li>Tasks require exclusive, sequential execution.</li>
      </ul>
    </div>
  </div>
</div>

Single Active Consumer can be enabled when declaring a queue, with the **`x-single-active-consumer`** argument set to `True`.

<!-- prettier-ignore-start -->

**Example:** Declaring a queue with a single active consumer

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

channel.queue_declare(
    queue="sac",
    durable=True,
    arguments={"x-single-active-consumer":True}
)

connection.close()
```

For a queue with single active consumer enabled, registering consumers results in the following:

- The first consumer registered with the queue becomes the *single active consumer*.
- If the active consumer becomes unavailable, the queue selects the next consumer from the pool in connection order.

### Consumer Exclusivity vs Single Active Consumer

A Single Active Consumer is not the only way to ensure that a single consumer processes messages from a queue. Consumer Exclusivity offers an alternative. It assigns one consumer as the sole processor for a specific queue, ensuring only one consumer handles messages. However, if the exclusive consumer disconnects or stops, the application must register a new consumer to continue processing—unlike the Single Active Consumer, which automatically fails over to the next available consumer.

**Example:** Consumer exclusivity

```python
import pika

connection = pika.BlockingConnection(pika.URLParameters('host-url'))
channel = connection.channel()

def callback(ch, method, properties, msg):
		print(f"[✅]  { msg }")
		ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    "ex_queue",
    callback,
    auto_ack=False,
    exclusive**=**True

)
connection.close()

```
<!-- prettier-ignore-end -->
