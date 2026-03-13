
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a shovel?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          A shovel is a mechanism in LavinMQ used to transfer messages from one broker to another, such as between different servers. It can be configured to move messages between the following:
        </p>
        <ul>
          <li>One exchange to another exchange</li>
          <li>One exchange to a queue</li>
          <li>One queue to an exchange</li>
          <li>One queue to another queue</li>
        </ul>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        How does a shovel work?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        A shovel connects to the source and destination, authenticates, then consumes and republishes messages. If a failure occurs, it automatically retries.
      </p>
    </div>
  </div>
</div>

<!-- prettier-ignore-start -->

A shovel can be created and managed through the LavinMQ [HTTP API](https://docs.lavinmq.com/docs/#/operations/GetShovels) or the LavinMQ management console.

## API

To create a shovel using the LavinMQ API, send a `POST` request to the `/api/parameters/shovel/` endpoint with the required configuration.

**Example:** API - create a shovel

```python
POST http://<rabbitmq-server>/api/parameters/shovel/<vhost>/<shovel-name>
```

**Example:** Payload

```json
{
  "component": "shovel",
  "name": "my_shovel",
  "value": {
    "src-uri": "amqp://source-server",
    "src-type": "queue", // Source type (queue or exchange)
    "src-queue": "source-queue", // Source queue name (use src-exchange for exchanges)
    "dest-uri": "amqp://destination-server",
    "dest-type": "queue", // Destination type (queue or exchange)
    "dest-queue": "destination-queue",
    "ack-mode": "on-confirm"
  }
}
```

In this example:

- The shovel moves messages from the `source-queue` on the source server to the `destination-queue` on the destination server.
- `src-type` and `dest-type` define the type of the source and destination: either queue or exchange.
  - If `src-type` is queue, use `src-queue` to specify the source queue.
  - If `src-type` is exchange, use `src-exchange` to specify the source exchange.
  - Similarly, for the `dest-type`, if it's queue, use `dest-queue`; if it's exchange, use `dest-exchange`.
- The `ack-mode`is set to`on-confirm`, ensuring messages are acknowledged once they are confirmed by the destination.
- The `prefetch-count` is set to `100`, controlling how many messages can be prefetched at once before acknowledging them.

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-[#414040] conf-table">
      <tr>
        <td class="font-semibold">Virtual host</td>
        <td>
          Any vhost chosen
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Name (shovel-name)</td>
        <td>
          The name of the shovel
        </td>
      </tr>
    </table>
  </div>
</div>

Full API documentation can be found in the HTTP API for [parameters for components](https://docs.lavinmq.com/docs/#/operations/GetShovels) and [main](https://docs.lavinmq.com/docs/#/operations/GetShovels).

### Source

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-[#414040] conf-table">
      <tr>
        <td class="font-semibold">URI (src-uri)</td>
        <td>
          The URI for the source server (e.g., <code>amqp://source-server</code>).
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Type (src-type)</td>
        <td>
          Select whether the source is a “queue” or an “exchange”.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Endpoint queue (src-queue)</td>
        <td>
          Source queue
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Endpoint exchange (src-exchange)</td>
        <td>
          This can be used instead of <code>src-queue</code> if source type is "exchange”.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Prefetch (prefetch-count)</td>
        <td>
          How many messages to be prefetched.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Auto delete (auto-delete)</td>
        <td>
          When enabled, the shovel will transfer all messages from the queue and delete itself once done. If set to <code>false</code>, it will persist until explicitly removed.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Routing key (dest-routing-key)</td>
        <td>
          The routing key is only needed when shoveling messages to an exchange and controlling the routing.
        </td>
      </tr>
    </table>
  </div>
</div>

### Destination

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-[#414040] conf-table">
      <tr>
        <td class="font-semibold">URI (dest-uri)</td>
        <td>
          The URI for the destination server (e.g., <code>amqp://destination-server</code>).
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Type (dest-type)</td>
        <td>
          Select whether the destination is a “queue” or an “exchange”.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Endpoint queue (dest-queue)</td>
        <td>
          Destination queue
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Endpoint exchange (dest-exchange)</td>
        <td>
          This can be used instead of dest-queue if destination type is "exchange”.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Reconnect delay (reconnect-delay)</td>
        <td>
          Delay in seconds before attempting to reconnect in case of failure.
        </td>
      </tr>
      <tr>
        <td class="font-semibold">Ack mode (ack-mode)</td>
        <td>
          Set the acknowledgment mode (e.g., <code>on-confirm</code> or <code>none</code>).
        </td>
      </tr>
    </table>
  </div>
</div>

#### Webhooks as a destination

LavinMQ shovels also support setting an HTTP target as the destination (`dest-uri`). This functionality, called webhooks, allows messages to be forwarded to an HTTP endpoint instead of another AMQP broker. This is particularly useful for integrating with external systems or services that expose HTTP APIs.

For more details on webhooks, refer to the [webhooks documentation](/documentation/webhooks).

### Pausing a shovel

To pause a shovel, you can use the LavinMQ management console or the API.

To pause a shovel using the API, send a `PUT` request to the `/api/shovel/<vhost>/<shovel-name>/pause` endpoint.

**Example:** API - pause a shovel

```python
PUT http://<rabbitmq-server>/api/shovel/<vhost>/<shovel-name>/pause
```

<!-- prettier-ignore-end -->
