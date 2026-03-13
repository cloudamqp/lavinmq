
<!-- prettier-ignore-start -->

In an RPC (Remote Procedure Call), a client sends a request to run a function
and any related arguments to a program located on another server. The remote
server runs the task and sends back the response. The compiled code includes a
stub that acts as a representative of the remote procedure code. The stub
receives the request while the program is running and the procedure call has
been issued. It forwards it to a client program in the local computer, while the
initiating program waits until it receives a response.

The RPC process works somewhat differently in LavinMQ. There is no stub
available. The program will not stall if you use a non-blocking consumer.
Instead, your client sends a message to the broker containing the reply-to
header. The target server processes the request and sends data back to the
server. Then, in regular pub/sub fashion, your consumer receives the
response.

### What is LavinMQ RPC?

The reply-to header in LavinMQ lets you create applications that communicate
with one another over a special form of RPC.

### How does RPC work in LavinMQ?

After a client sends the request to a server, the server gives a response
message in return. The client sends a callback queue address with its request in
order to receive a response. One queue receives the response while another
consumer handles the reply.

Another way to handle RPC in LavinMQ is by using direct reply-to, which
passes a response directly to the client, avoiding having to create a response
queue to wait on.

### When to Use RPC in LavinMQ?

RPC using the LavinMQ direct reply-to header allows you to create responsive
applications since you can perform tasks remotely from your mobile and desktop
applications.

## LavinMQ RPC

There are different RPC options in LavinMQ. One is to let the client send a
request message and let a server reply with a response message, sent into a
specified queue. This queue is called a callback or response queue. The name of
this queue must be sent as an address in the request message sent from the
client:

{% highlight python %}
channel.basic_publish(
  exchange='',
  routing_key='rpc_queue',
  properties=pika.BasicProperties(
    reply_to = response_queue,
  ),
  body=request
)
{% endhighlight %}

In the code shown above is a callback queue added for every RPC request, which
might be confusing since it's not clear to which request the response belongs.
A `correlation_id` (a unique value) can therefore be sent with every request.

![]()

{% highlight python %}
channel.basic_publish(
  exchange='',
  routing_key='rpc_queue',
  properties=pika.BasicProperties(
    reply_to = response_queue,
    correlation_id = correlation_id
  ),
  body=request
)
{% endhighlight %}

Workflow of RPC callback queues in LavinMQ:

- The client creates an exclusive callback_queue.
- The client sends a message containing the reply queue, the callback queue
  along with the correlation_id. The value of the correlation_id is set to a
  unique value at every request.
- Next, the request is sent to `rpc_queue`.
- The RPC worker/consumer, i.e. the remote server, is waiting for requests on
  that queue (rpc_queue) and handles the request once received.
- The result is sent back to the broker by the remote server using the `reply_to`
  field to know which queue it belongs in. The response goes to the response_queue.
- The callback queue is where the client/consumer waits for data.

{% highlight python %}
callback_queue = channel.queue_declare(queue='', exclusive=True)

channel.basic_publish(
  exchange='',
  routing_key='rpc_queue',
  properties=pika.BasicProperties(
    reply_to = callback_queue,
    correlation_id = correlation_id
  ),
  body=request
)
{% endhighlight %}

As mentioned earlier, you can also handle RPC in LavinMQ by using direct reply-to.

## What is Direct Reply-to?

LavinMQ allows you to pass a response directly to the client, avoiding
creating a response queue to wait on. To do so, set `reply-to`
to `amq.direct.reply-to`. Send a message in `no-ack` mode. LavinMQ
generates a special name that the target server sees as the routing key. The
remote machine then publishes the result to the default exchange. No queue is
created in the process. A direct reply allows you to avoid maintaining a
long-lived queue. It also lets you avoid creating short-lived queues that
utilize memory.

### Using Direct Reply-to in LavinMQ

To use the direct reply feature, use the reply-to header with the
`amq.direct.reply-to` queue. In this RPC example, we send a simple ping
request to a remote machine.

Start by consuming from the `amq.direct.reply-to` queue on your client to
avoid missing the response:

{% highlight python %}
def client_consumer_callback(ch, method, properties, body):
  msg = body.decode('utf-8')

  if msg in "Hello from Consumer":
    print("TARGET MACHINE IS ACTIVE")
    global RECEIVED_HELLO
    RECEIVED_HELLO = True
  else:
    print("RECEIVED UNEXPECTED MESSAGE")
    ch.close()
    channel.consume("amq.direct.reply-to", on_message_callback=client_consumer_callback)
{% endhighlight %}

The consumer checks that the response message contains a greeting from the
server.

Next, create another consumer to mimic a remote computer:

{% highlight python %}
def consumer_callback(ch, method, properties, body):
  msg = body.decode('utf-8')

  if msg in "Hello World":
    basic_props = BasicProperties()
    ch.basic_publish(exchange='', routing_key=properties.reply_to, properties=basic_props, body="Hello from Consumer")
    ch.basic_ack(delivery_tag=method.delivery_tag)
  else:
    print("RECEIVED UNEXPECTED MESSAGE")
    channel.consume("amq.direct.reply-to", on_message_callback=consumer_callback)
{% endhighlight %}

This consumer receives the request, checks that the message contains Hello World, and sends back the greeting. Notice that both consumers subscribe to amq.direct.reply-to.

Finally, send a message to the queue using the `reply-to` header:

{% highlight python %}
basic_props = BasicProperties(
    reply_to="amq.direct.reply-to"
)

channel.basic_publish(
    exchange='',
    routing_key='',
    properties=basic_props,
    body="Hello World"
)
{% endhighlight %}

You can use this method to fetch data or perform tasks such as registering users
on a mobile application. We created a simple health check.

RPC differs somewhat from the traditional publisher-subscriber model LavinMQ
is known for. No queues are created and the process sends information directly
to the client using a direct reply.

When using RPC this way:

- Try to establish a connection to the client using the generated name on a
  disposal channel to see if the client still exists
- Set the immediate flag to false when publishing
- Start consuming from the amq.lavinmq.reply-to before publishing your
  message
- Set the mandatory flag if using amq.lavinmq.reply-to to create error logs
- Do not set the mandatory flag when using a direct-reply if using
  `amq.lavinmq.reply-to.*` as your queue

These tips allow you to know when something goes wrong. They also help you
handle issues without losing messages.

---

**NOTE:**
LavinMQ supports the use of `amq.rabbitmq.reply-to` to allow for compatibility with other brokers.

---

## Wrap up

In conclusion, LavinMQ provides efficient support for Remote Procedure Calls (RPC), allowing applications to communicate with one another seamlessly. Unlike traditional RPC systems that use stubs, LavinMQ simplifies the process by eliminating the need for stubs and providing alternative mechanisms for RPC.

When using LavinMQ for RPC, clients can send requests to servers, and the servers respond with the desired data or results. The reply-to header plays a crucial role in this process, enabling clients to specify where they expect the response. The response can be received through a callback queue or, alternatively, by utilizing the direct reply-to feature. Direct reply-to streamlines the process by directly passing the response to the client, eliminating the need for maintaining long-lived or short-lived queues.

<!-- prettier-ignore-end -->
