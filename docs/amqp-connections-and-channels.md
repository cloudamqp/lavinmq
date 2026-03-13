
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a connection?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          A (TCP-)connection is a link between the client and the LavinMQ broker that performs
          underlying networking tasks, including initial authentication, IP resolution, and
          networking.
        </p>
				<img alt="LavinMQ connection start" class="border border-[#414040]" src="/img/docs/amqp-connections-and-channels-one.png" />
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What is a channel?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>A channel acts as a virtual connection inside a TCP connection.</p>
			<img alt="LavinMQ channel" class="border border-[#414040]" src="/img/docs/amqp-connections-and-channels-two.png" />
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect3" id="accordion3">
        What is the role of channels in the AMQP protocol?
      </button>
    </h3>
    <div id="sect3" class="accordion-content" role="region" aria-labelledby="accordion3">
      <p>
				Every AMQP protocol-related operation occurs over a channel. A channel reuses a
				connection, avoiding the need to reauthorize and open a new TCP stream. Channels
				use resources more efficiently than opening and closing connections.
			</p>
    </div>
  </div>
</div>

A connection is created by:

1. Establishing a physical TCP connection to the target server.
2. The client resolves the hostname to one or more IP addresses.
3. The receiving server authenticates the client.
4. A connection is now established.

<img alt="LavinMQ channel" class="border border-[#414040]" src="/img/docs/amqp-connections-and-channels-three.png" />

**Example:** Establishing a connection

{% highlight python %}
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
{% endhighlight %}

LavinMQ supports IPv4, IPv6, and encrypted TLS (SSL) connections. For most clients, TLS is easy to use; replace amqp:// with amqps:// in the URL.

Connections must be established before creating a channel to send messages or manage queues.

### Channel

Every AMQP protocol-related operation occurs over a channel, such as sending messages, creating an exchange, or handling queue creation and maintenance. AMQP allows one TCP/IP connection to multiplex into several “lightweight” channels. Channels are resource-efficient since they reuse existing channels and minimize the need for resource-heavy openings and closings of new TCP channels.

Closing a connection closes all associated channels.

<img alt="LavinMQ channel" class="border border-[#414040]" src="/img/docs/amqp-connections-and-channels-four.png" />

A channel can be opened right after successfully opening a connection.

**Example:** Opening a channel

{% highlight python %}
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
{% endhighlight %}
