
Federation is used to transfer messages between brokers, which can be useful in
a number of scenarios. This documentation will outline when to use federation,
what the difference is between queue federation and exchange federation, and how
to configure it.

### What is a federation?

Federation is used to transfer messages between brokers. It will transfer
messages from one broker called the upstream to another broker called the
downstream. There are two ways of configuring a federation, via queue federation
or exchange federation.

### What is the difference between queue federation and exchange federation?

- Queue federation is used when you want to transfer messages from a queue to
  another queue on another node or cluster, i.e. messages are moved and not
  copied. Exchange federation is used when you want to copy messages from an
  exchange to another exchange on another node or cluster.
- With queue federation, messages are only transferred where there are
  underutilized consumers, always preferring local consumers to remote ones.
  With exchange federation messages are always federated, no matter where the
  consumers are connected.
- Exchange federation will pass on bindings from the downstream to the upstreams
  when possible, whereas queue federation will not.

### What does downstream and upstream mean?

Upstream servers are where messages are originally published. Downstream servers
are where messages get forwarded to.

### When should I use federation?

Federation has several use-cases:

- collect messages in a central cluster from multiple clusters
- distribute the load from one queue to other clusters
- migrating to another cluster without stopping all producers/consumers
- a hot-standby with relatively up-to-date data

### How does the federation connect?

The federation will connect to all its upstream queues or exchanges using
AMQP 0-9-1. The URI used to connect can be in any of the formats:

```
amqp://user:password@server-name/my-vhost
```

<p style="margin-top: -24px;">
connect to server-name, with credentials and specified virtual host
</p>

```
amqps://user:password@server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/cert.pem&keyfile=/path/to/key.pem&verify=verify_peer
```

<p style="margin-top: -24px;">
connect to server-name, with credentials and SSL
</p>

```
amqps://server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/cert.pem&keyfile=/path/to/key.pem&verify=verify_peer&fail_if_no_peer_cert=true&auth_mechanism=external
```

<p style="margin-top: -24px;">
connect to server-name, with SSL and EXTERNAL authentication
</p>

## Queue Federation

Queue federation connects an upstream queue to transfer messages to a downstream
queue when there are consumers on the downstream queue. Consumers and publishers
can be moved in any order and the messages will not be duplicated. The federated
queue will only receive messages when it has run out of messages locally,
consumers are connected, and the upstream queue has messages that are not being
consumed.

## Exchange Federation

In a normal scenario, messages are published to an exchange and then routed to
the queue(s). With the use of an exchange federation, it is possible to get
LavinMQ to distribute those messages to another cluster. This means that
messages arriving in the federated exchanges will also be forwarded to the
downstream clusters. Exchange federation consumes messages from an upstream
cluster and republishes them on its own local exchange as if the messages
published on the upstream cluster were published on the local cluster.

The federation will create a queue on the upstream cluster, bind it to the
exchange being federated, and then consume from that queue to republish the
messages on the local exchange. If the connection between the two federated
clusters is broken messages will queue up on the upstream queue and when the
server reconnects again it will transfer all messages that were published during
the network outage.

## Configure a federation

The federation functionality comes as standard on LavinMQ, so there is no need to activate the functionality. Federations are always configured on the downstream servers, and you need to configure:

- One or more upstreams where the messages will come from.
- A federation link
- The policy that matches the queues/exchanges that should be federated.

![Federation add upstream](/img/docs/docs-dm/add-new-upstream-dm-2x.png)

Here is a list of all configuration with descriptions related to federations:

- Virtual host the federation will be created in
- Name of the federation
- URI of the upstream server, in any of the format:
  - `amqp://user:password@server-name/my-vhost`
    - connect to server-name, with credentials and specified virtual host
  - `amqps://user:password@server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/     cert.pem&keyfile=/path/to/key.pem&verify=verify_peer`
    - connect to server-name, with credentials and SSL
  - `amqps://server-name?cacertfile=/path/to/cacert.pem&certfile=/path/to/cert .pem&keyfile=/path/to/key.pem&verify=verify_peer &fail_if_no_peer_cert=true&auth_mechanism=external`
    - connect to server-name, with SSL and EXTERNAL authentication
- [Prefetch](/documentation/prefetch) is the maximum number of unacknowledged
  messages that will be in flight at one time.
- Reconnection delay is the time in seconds to wait after a network link goes
  down before trying to reconnection.
- Ack mode is how messages will be acknowledged:
  - `on-confirm`<br/>
    Messages are acknowledged to the upstream broker when they have been
    confirmed by the downstream server. Messages are not lost in the case of
    network errors and broker failures.
  - `on-publish`<br/>
    Messages are acknowledged to the upstream broker after they have been
    published to the downstream server. Handles network errors without losing
    messages, but may lose messages in the event of broker failures.
  - `no-ack`<br/>
    Message acknowledgements are not used. Can lose messages in the event of
    network or broker failures.
- Exchange is the name of the upstream exchange.
- Expires is the time in milliseconds when the federation link will be
  removed. Leave this blank for the link to never expire.
- Message TTL is how long the upstream should hold undelivered messages in
  the event of a network outage or backlog. Leaving this blank means forever.
- Queue is the name of the upstream queue. The default is using the same name
  as the federated queue.
- Consumers tag is used when consuming from upstream.

## Examples

In this example, we will work with two different LavinMQ clusters, located
in two different physical locations. Broker A will be the upstream server and
broker B will be the downstream server. We start by configuring the federation
on broker B (the downstream server). Configuring the federation can be done via
the LavinMQ Management Interface or the HTTP API, but in this example, we
will focus on the LavinMQ Management Interface.

![Federation example](/img/docs/lavinmq-federation-example.jpg)

When configuring the federation, we will enable messages to be moved or copied
from cluster A to cluster B. In this example, we will create a queue named
`federation.queue` on both the upstream and the downstream server. We will also
create a federated exchange called ‘federation.exchange’ and bind the exchange
on the upstream server to the federated link on the downstream server. Messages
published to the exchange on the upstream server will be routed to
the `federation.queue` and they will also be copied to the exchange on the
downstream server, as well as routed to the `federation.queue` on the
downstream server.

<ol class="flex flex-col space-y-4">
  <li>
    Start by getting the URL for the upstream server, for example <code class="language-plaintext highlighter-rouge">amqp://user:password@server-name/my-vhost</code>
  </li>
  <li>
    Configure the federation link on the downstream server (B) by going to the
    LavinMQ Management Interface and click on the tab Federation and fill in
    the section <em>Add a new upstream</em>.<br/>
    <img src="/img/docs/docs-dm/add-new-upstream-v2-dm-2x.png" alt="Federation add upstream example" class="border border-[#414040]">
  </li>
  <li>
    Create a policy on the downstream server (B) that matches the
    queues/exchanges that should be federated. Navigate to Policies and fill in the
    section Add/update policy. The pattern argument is a regular expression used to
    match exchange or queue names. In this case, we will tell the policy to federate
    all queues and exchanges whose names begin with "federation.". A policy can
    apply to an upstream set or a single upstream of exchanges and/or queues. In
    this example we apply to all upstreams, federation-upstream-set is set to all.
    <br/>
    <img src="/img/docs/docs-dm/add-policy-dm-2x.png" alt="Update federation policy" class="border border-[#414040]">
  </li>
  <li>
    We then need to create queues, exchanges, and bindings for the downstream
    server (B). The fastest and easiest way to set up this is by using any
    programming language, but it can also be done directly from the LavinMQ
    Management interface.
    <ul>
      <li>Define a queue, in this example named <code class="language-plaintext highlighter-rouge">federation.queue</code>.
      </li>
      <li>
        Create an exchange, in this example named <code class="language-plaintext highlighter-rouge">federation.exchange</code>.
      </li>
      <li>
        Bind the exchange to the queue, in this example bind
        the <code class="language-plaintext highlighter-rouge">federation.exchange</code> to the <code class="language-plaintext highlighter-rouge">federation.queue</code>.
      </li>
      <li>
        Check that the federation.exchange has a binding to the federated
        upstream exchange.
      </li>
    </ul>
  </li>
  <li>
    Try the federation by publishing one message to the exchange,
  federation.exchange, on the upstream server. You will see that the message
  moves to the <code class="language-plaintext highlighter-rouge">federation.queue</code> on the downstream server.
  </li>
</ol>

## Conclusions

- Policies are matched every time an exchange or queue is created.
- Policies with a priority greater than 0 will take precedence.
- The default exchange cannot be federated since it is not a standard exchange.

## Read more

HTTP API documentation for federation:
[https://hostname/docs/#operation/](https://hostname/docs/#operation/)
