
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is message deduplication?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>Message deduplication prevents duplicate messages from being enqueued, reducing unnecessary processing and storage.</p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        When to enable message deduplication?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>Enable deduplication when you need to prevent redundant processing, reduce storage usage, or ensure messages are delivered only once.</p>
    </div>
  </div>
</div>

## Message deduplication

LavinMQ supports message deduplication at both exchange and queue levels:

- **Exchange-level deduplication**: When enabled, duplicate messages are not forwarded to bound queues.
- **Queue-level deduplication**: Messages are routed as usual but are not stored in the queue if already seen. This ensures correct routing while avoiding increased "Unroutable message" statistics.

## Using message deduplication

To enable deduplication, set the following arguments:

- `x-message-deduplication` (bool) – Enables deduplication.
- `x-cache-size` (int) – Defines how many messages to store for deduplication checks. A larger cache uses more memory.

Optional settings:

- `x-cache-ttl` (int) – Cache entry time-to-live in milliseconds (default: unlimited).
- `x-deduplication-header` (string) – The message header used for deduplication (default: `x-deduplication-header`).

The deduplication cache is stored in memory, meaning:

- A larger cache increases memory usage.
- The cache is lost on broker restarts or leadership transfers in a cluster.

This allows LavinMQ to filter out duplicate messages while maintaining proper routing behavior.
