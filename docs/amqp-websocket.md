<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="ws-sect1" id="ws-accordion1">
        What are WebSockets?
      </button>
    </h3>
    <div id="ws-sect1" class="accordion-content" role="region" aria-labelledby="ws-accordion1">
        <p>
          WebSockets are like a phone call for your web browser. Instead of hanging up and redialing for every message (like a standard website), a WebSocket opens one continuous connection. This lets applications like live charts or chat apps instantly send and receive data without delay
        </p>
        <p>  
          WebSockets are a protocol that enables a persistent, full-duplex communication channel over a single TCP connection. Unlike HTTP, which follows a request–response model, WebSockets allow both the client and the server to send messages to each other at any time. 
        </p>
    </div>
  </div>
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        Why are WebSockets important in LavinMQ?
      </button>
    </h3>
      <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
        <p>
          WebSockets make it possible for applications to communicate with LavinMQ in real time directly from environments where AMQP libraries may not be available, such as web browsers. WebSockets are well suited for real-time applications such as chat systems, live dashboards, online games, and IoT messaging.
           This expands LavinMQ’s reach beyond traditional backend systems and makes it easier to integrate messaging into modern, interactive applications.
        </p>
    </div>
  </div>
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="ws-sect3" id="ws-accordion3">
        How does LavinMQ support WebSockets?
      </button>
    </h3>
      <div id="ws-sect3" class="accordion-content" role="region" aria-labelledby="ws-accordion3">
        <p>
          LavinMQ provides native support for WebSockets by exposing a persistent, full-duplex communication channel over a standard HTTP(S) port. Through WebSockets, applications can publish messages, consume from queues, and use exchanges in the same way as with traditional AMQP connections. Authentication, access control, and routing all work consistently, so WebSocket clients benefit from the same security and reliability features as other LavinMQ protocols.
        </p>
    </div>
  </div>
</div>
LavinMQ offers support for Websockets via: _AMQP and MQTT over Websockets_.

LavinMQ provides the WebSockets functionality through the `WebsocketProxy` module, which acts as a bridge between WebSocket clients and the standard TCP servers. Authentication, authorization, and routing work the same way as with regular AMQP or MQTT connections.

The protocol is determined by the WebSocket endpoint path:

- MQTT over WebSockets → connections to the `/mqtt` or `/ws/mqtt` are treated as MQTT.

- AMQP over WebSockets → all other WebSocket connections are treated as AMQP.

This design makes it possible for LavinMQ to serve both messaging protocols through the same WebSocket interface, enabling real-time applications, IoT devices, and browser-based clients to connect without additional libraries or plugins.

### Example (AMQP over WebSockets)

{% highlight javascript %}
const WebSocket = require('ws');
const ws = new WebSocket('ws://your-lavinmq-server/');

ws.onopen = () => {
  console.log('Connected to LavinMQ via AMQP/WebSocket');
  // Perform protocol-specific actions like declaring a queue
  // and subscribing to it.
};

ws.onmessage = (event) => {
  console.log('Received message:', event.data);
};
{% endhighlight %}

### Example (MQTT over WebSockets)

{% highlight javascript %}
const mqtt = require('mqtt');
const MQTT_URL = "wss://<host>/mqtt";
const client = mqtt.connect('MQTT_URL');

client.on('connect', function () {
  console.log('Connected to LavinMQ via MQTT/WebSocket');
  client.subscribe('mytopic', function (err) {
    if (!err) {
      console.log('Subscribed to my/topic');
    }
  });
});

client.on('message', function (topic, message) {
  console.log(`Received message on topic "${topic}": ${message.toString()}`);
});
{% endhighlight %}


### WebSocket tutorial
Dive in with our hands-on guide to get started with WebSockets in LavinMQ.
Try out the [websocket tutorial](/documentation/websocket-tutorial).
