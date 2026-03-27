
<div class="accordion">
	<div class="accordion-item">
		<h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
				What is the MQTT Protocol
			</button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
      <p>
        <a href="https://mqtt.org/">MQTT</a>, which stands for Message Queuing Telemetry Transport, is a lightweight, publish-subscribe messaging protocol. It's designed for machine-to-machine (M2M) and Internet of Things (IoT) communication. Developed with resource-constrained devices in mind, MQTT is designed for situations where network bandwidth is limited and power consumption needs to be minimized. This makes it ideal for remote sensors, embedded systems, and mobile applications with intermittent connectivity.
      </p>
    </div>
  </div>
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        Why is MQTT used in LavinMQ
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        LavinMQ is a lightweight message broker, and MQTT's small code footprint and focus on efficiency make it a perfect match. By supporting MQTT, LavinMQ can serve as a robust and scalable platform for IoT and mobile applications, such as our [Real-time temperature monitoring with LavinMQ project](/iot). It ensures asynchronous communication across different devices, making it a trusted choice for building flexible and resilient messaging systems.
      </p>
    </div>
  </div>
</div>

<!-- prettier-ignore-start -->

## MQTT Protocol Overview

MQTT operates on a client-server model and uses TCP/IP for all communication. By default, the protocol uses specific ports: 1883 for unencrypted connections and 8883 for secure, encrypted connections over TLS/SSL (MQTTS).

To establish a connection to a LavinMQ broker, the following information is needed:

* Host/broker address: The domain name or IP address of the LavinMQ server.

* Port: The port number (1883 for unencrypted or 8883 for MQTTS).

* Credentials: Username and password for authentication.
LavinMQ supports MQTT 3.1.0 and 3.1.1 clients and there are many client libraries for nearly every programming language. While they all follow the same protocol, their syntax and usage will differ a bit. The examples that follow are all in Crystal for the client library [mqtt-client.cr](https://github.com/cloudamqp/mqtt-client.cr).

## Connecting to LavinMQ with MQTT

The following table outlines the MQTT protocol and the required arguments for each packet type.

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Packet type</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Required Fields</span></th>
          <th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td>Connect</td>
					<td class="border-r border-[#414040]"><code>client_id</code>, <code>username</code>, <code>password</code></td>
          <td>Starts a session by authenticating and connecting the client to the broker.</td>
				</tr>
        <tr>
					<td>Subscribe</td>
					<td class="border-r border-[#414040]"><code>topic</code>, <code>qos</code></td>
          <td>Subscribes to a specific topic.</td>
				</tr>
        <tr>
					<td>Unsubscribe</td>
					<td class="border-r border-[#414040]"><code>topic</code></td>
          <td>Unsubscribes to a specific topic.</td>
				</tr>
        <tr>
					<td>Publish</td>
					<td class="border-r border-[#414040]"><code>topic</code>, <code>qos</code></td>
          <td>Sends a message to a specific topic.</td>
				</tr>
        <tr>
					<td>Puback</td>
					<td class="border-r border-[#414040]"><code>message_id</code></td>
          <td>Acknowledges receipt of a message (QoS 1).</td>
				</tr>
        <tr>
					<td>Disconnect</td>
					<td class="border-r border-[#414040]">N/A</td>
          <td>Ends the session gracefully.</td>
				</tr>
      </tbody>
    </table>
  </div>
</div>



### Steps to Connect:

1. Send a **CONNECT** packet to LavinMQ's MQTT listener on port `1883`.
2. Include the following fields in the CONNECT packet:
   - `client_id`: A unique identifier for the client.
   - `username` and `password`: For authentication.
   - `clean_session`: Boolean to specify whether to start a clean session.
   - `will`: An optional field for a Last Will and Testament message.
   - `qos`: Quality of Service level.

### Connecting to a Specific Virtual Host

By default, MQTT clients connect to the default virtual host configured in LavinMQ (typically "/"). To connect a user to a specific vhost, use the following username format:

```
vhost:username
```

For example, to connect user "guest" to the vhost "production", set the username field to:

```
production:guest
```

If no colon (`:`) and vhost is present in the username, the connection will use the default vhost specified by the `default_mqtt_vhost` configuration variable.

### Example Using `mqtt-client.cr`:

**Default vhost connection:**

```ruby
require "mqtt-client"
require "mqtt-protocol"

will = MQTT::Client::Message.new("connect", "body".to_slice, 1_u8, false)

client = MQTT::Client.new("localhost", 1883, user: "guest", password: "guest", client_id: "hello", clean_session: false, will: will)

# Application logic goes here.

client.disconnect
```

**Connecting to a specific vhost:**

```ruby
require "mqtt-client"
require "mqtt-protocol"

will = MQTT::Client::Message.new("connect", "body".to_slice, 1_u8, false)

# Connect to the "production" vhost as user "guest"
client = MQTT::Client.new("localhost", 1883, user: "production:guest", password: "guest", client_id: "hello", clean_session: false, will: will)

# Application logic goes here.

client.disconnect
```

### Quality of Service (QoS)

QoS defines the guarantee level for message delivery:

- **0 (At most once)**: No acknowledgment is required; the message may be lost.
- **1 (At least once)**: Requires acknowledgment (Puback); may result in duplicates of messages.

### Will

The **Will** argument specifies a message to be sent by the broker to a chosen topic if the client disconnects unexpectedly. Fields include:

- `will_topic`: Topic for the Will message.
- `will_message`: Content of the Will message.
- `will_qos`: QoS level for the Will message.
- `will_retain`: Whether the Will message should be retained.

### Clean session

The `clean_session` flag determines whether to establish a persistent or transient session:

- **True**: The broker will not retain session information, such as subscriptions or undelivered messages, after the client disconnects.
- **False**: The session is persistent, and the broker retains subscriptions and undelivered messages, enabling the client to resume where it left off.

Persistent sessions benefit clients who need to reconnect frequently while maintaining continuity. However, transient sessions are lightweight and suitable for clients that only need real-time interactions without history.

## Subscribing and Unsubscribing

### Example: Subscribe to a Topic

```ruby
require "mqtt-client"
require "mqtt-protocol"

client = MQTT::Client.new("localhost", 1883, user: "guest", password: "guest", client_id: "hello", clean_session: false)

client.on_message do |msg|
  puts "Got a message, on topic #{msg.topic}: #{String.new(msg.body)}"
end

client.subscribe("my/topic")

# Application logic goes here.

client.unsubscribe("my/topic")
```

## Publishing and Acknowledging Messages

### Example: Publish a Message

Disclaimer: It's not possible to publish messages to a topic that no session has subscribed to yet.

```ruby
require "mqtt-client"
require "mqtt-protocol"

client = MQTT::Client.new("localhost", 1883, user: "guest", password: "guest", client_id: "hello", clean_session: false)

client.publish("my/topic", "Hello World")
```

### Example: Acknowledge a Published Message (Puback)

```ruby
require "mqtt-client"
require "mqtt-protocol"

client = MQTT::Client.new("localhost", 1883, user: "guest", password: "guest", client_id: "hello", clean_session: false)

client.on_message do |msg|
  puts "Got a message, on topic #{msg.topic}: #{String.new(msg.body)}"
  client.puback("my/topic", msg.id)
end

client.subscribe("my/topic", qos: 1)
```

## Retained Messages

Retained messages are stored by the broker and delivered to new subscribers on a topic when they subscribe. To send a retained message:

### Example: Send a Retained Message

```ruby
require "mqtt-client"
require "mqtt-protocol"

client = MQTT::Client.new("localhost", 1883, user: "guest", password: "guest", client_id: "hello", clean_session: false)

client.publish("my/topic", "Hello World", retain: true)
```

---

## MQTT Client Permissions

By default, LavinMQ only checks user credentials when an MQTT client connects. To also enforce resource permissions on publish and subscribe operations, enable `permission_check_enabled` in the `[mqtt]` configuration section:

```ini
[mqtt]
permission_check_enabled = true
```

When enabled, MQTT clients use the same authorization model as AMQP clients:

- **Publish operations:** Require **write** permission on the MQTT exchange (`mqtt.default`)
- **Subscribe operations:** Require **read** permission on the MQTT exchange or **write** permission on the client's session queue (`mqtt.<client_id>`)
- **Will messages:** Require **write** permission on the MQTT exchange

If a client lacks the required permissions, the connection is closed.

LavinMQ does not currently support fine-grained, per-topic permissions for MQTT. Access is granted or denied at the exchange level.

## Clustering with MQTT

In a clustered setup, LavinMQ replicates retained messages across nodes. To ensure followers forward MQTT traffic to the leader, configure the following settings:

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Config variable</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td><code>unix_path</code></td>
					<td>UNIX path to listen for MQTT traffic, e.g., <br /> <code>/tmp/lavinmq-mqtt.sock</code>.</td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

## New Configuration Variables

The following variables control MQTT behavior in LavinMQ:

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Variable</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
				<tr>
					<td><code>bind</code></td>
					<td>IP address that AMQP, MQTT, and HTTP servers will listen on.</td>
				</tr>
        <tr>
					<td><code>port</code></td>
					<td>Port for non-TLS MQTT connections. <br />Default: <code>1883</code>.</td>
				</tr>
        <tr>
					<td><code>tls_port</code></td>
					<td>Port for TLS-enabled MQTT connections. <br />Default: <code>8883</code>.</td>
				</tr>
        <tr>
					<td><code>unix_path</code></td>
					<td>UNIX socket path for MQTT. <br />Example: <code>/tmp/lavinmq-mqtt.sock</code>.</td>
				</tr>
        <tr>
					<td><code>max_inflight_messages</code></td>
					<td>Maximum number of messages in flight simultaneously. <br />Default: <code>65535</code>.</td>
				</tr>
        <tr>
					<td><code>default_vhost</code></td>
					<td>Specifies the default virtual host (vhost) for MQTT connections. Default: "/"</td>
				</tr>
        <tr>
					<td><code>permission_check_enabled</code></td>
					<td>Enables resource permission checks on MQTT publish and subscribe operations. When disabled (the default), only authentication is checked at connection time. Default: <code>false</code>. Available since v2.5.0.</td>
				</tr>
      </tbody>
    </table>
  </div>
</div>

<!-- prettier-ignore-end -->
