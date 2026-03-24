
**Note:** It is hard to talk about webhooks in LavinMQ without referencing shovel.
Thus, we highly recommend that you go through our
[guide on shovel](shovel.md) first, if you are not familiar with it,
before going through this guide.

Shovels in other brokers like RabbitMQ only allow moving messages between queues
to queues, queues to exchanges(and vice-versa), and exchanges to exchanges.

While LavinMQ supports that as well, it takes this a step further
with webhooks.

## What is a webhook?

Generally speaking, a webhook allows different software services to
instantly communicate and share data with each other. With a webhook,
one application can send data in real-time to another application whenever
something happens in the first application..

Now, let's tie this to LavinMQ...

In LavinMQ, the webhook feature enables your LavinMQ server to communicate
with a HTTP server. More specifically, by configuring a webhook, you can
set up LavinMQ to forward every published message from an exchange or a queue
to a designated HTTP endpoint. It's almost like making
a `HTTP POST` request from your LavinMQ server to a HTTP server for every
new message received.

## How does a webhook work in LavinMQ?

To configure webhook in LavinMQ, simply create a "shovel" and specify the
LavinMQ server, the source exchange/queue for grabbing messages, and the
destination as the HTTP endpoint capable of receiving POST requests.

The image below demonstrates what this would look like:

<img alt="LavinMQ Webhook" src="img/docs/add-shovel-dm-2x.png" class="border border-[#414040]"/>

If you are not familiar with what each field does in the image above,
again, we encourage you to go through our [guide on shovel](shovel.md).

In summary, we are trying to create a shovel named
`webhookShovel`. The **source** section is where we specify the LavinMQ server
and the specific queue or exchange where we want our shovel to grab messages from.

In our case, we want our shovel to grab messages from the `webhook-queue`, that's
declared in the LavinMQ server whose URL we specified in the URI field.

Similarly, the **destination** section is where we specify the HTTP endpoint
where we want messages forwarded to. The
`Type` and `Endpoint` fields are automatically disabled once LavinMQ detects a valid
HTTP URL.

For your HTTP endpoint, you can grab a URL from [webhook.site](https://webhook.site/)
and use it to test things out.

## Wrap up

While LavinMQ allows moving messages between queues and exchanges with shovels,
it even takes this a step further with webhooks. With webhooks in LavinMQ, you can
effortlessly configure your server to forward every published message from an exchange or a
queue to a designated HTTP endpoint. This feature expands LavinMQ's capabilities beyond
traditional brokers, enabling you to easily integrate and exchange data with
non-amqp applications.
