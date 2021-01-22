[![Build Status](https://github.com/cloudamqp/avalanchemq/workflows/CI/badge.svg?branch=master)](https://github.com/cloudamqp/avalanchemq/actions)

# ![AvalancheMQ](static/img/logo-avalanche-mq-black.png)

A message queue server that implements the AMQP 0-9-1 protocol.
Written in [Crystal](https://crystal-lang.org/).

Aims to be very fast, has low RAM requirements, handles extremely long queues,
many connections, and requires minimal configuration.

## Implementation

AvalancheMQ is written in [Crystal](https://crystal-lang.org/), a modern
language built on the LLVM, that has a Ruby-like syntax, uses an event loop
library for IO, is garbage collected, adopts a CSP-like [concurrency
model](https://crystal-lang.org/docs/guides/concurrency.html) and compiles down
to a single binary. You can liken it to Go, but with a nicer syntax.

Instead of trying to cache message in RAM we write all messages as fast as we can to
disk and let the OS cache do the caching.

Each vhost is backed by a message store on disk, it's just a series of files (segments),
that can grow to 256 MB each. Each incoming message is appended to the last segment,
prefixed with a timestamp, its exchange name, routing key and message headers.
If the message is routed to a queue then the segment number and the position in
that segment is written to each queue's queue index. The queue index is
just an [in-memory array](https://crystal-lang.org/api/Deque.html)
of segment numbers and file positions. In the case of durable queues
the message index is also appended to a file.

When a message is being consumed it removes the segment-position from the queue's
in-memory array, and write the segment-position to an "ack" file. That way
we can restore the queue index on boot by reading all the segment-position stored
in the queue index file, then exclude all the segment-position read from the
"ack" file.  The queue index is rewritten when the "ack" file becomes 16 MB,
that is, every 16 \* 1024 \* 1024 / 8 = 2097152 message.
Then the current in-memory queue index is written to a new file and the
"ack" file is truncated.

Segments in the vhost's message store are being deleted when no queue index as
a reference to a position in that segment.

Declarations of queues, exchanges and bindings are written to a definitions
file (if the target is durable), encoded as the AMQP frame they came in as.
Periodically this file is garbage collected
by writing only the current in-memory state to the file (getting rid
of all delete events). This file is read on boot to restore all definitions.

All non-AMQP objects like users, vhosts, policies, etc. are stored in
JSON files. Most often these type of objects does not have a high
turnover rate, so we believe that JSON in this case makes it easy for
operators to modify things when the server is not running, if ever needed.

In the data directory we store `users.json` and `vhosts.json` as mentioned earlier,
and each vhost has a directory in which we store `definitions.amqp`
(encoded as AMQP frames), `policies.json` and the messages named such as `msgs.0000000124`.
Each vhost directory is named after the sha1 hash of its real name. The same goes
for the queue directories in the vhost directory. The queue directories only has two files,
`ack` and `enq`, also described earlier.

### Flows

Follows does an architectural description of the different flows in the server.

#### Publish

`Client#read_loop` reads from the socket, it calls `Channel#start_publish` for the Basic.Publish frame
and `Channel#add_content` for Body frames.  When all content has been received
(and appended to an `IO::Memory` object) it calls `VHost#publish` with a `Message` struct.
`VHost#publish` finds all matching queues, writes the message to the message store and then
calls `Queue#publish` with the segment position.
`Queue#publish` writes to the queue index file (if it's a durable queue).

#### Consume

When `Client#read_loop` receives a Basic.Consume frame it will create a `Consumer` class and add it to
the queue's list of consumers. The Queue got a `deliver_loop` fiber that will loop over the list of
consumers and deliver a message to each.

## Features

* AMQP 0-9-1 compatible
* AMQPS (TLS)
* HTTP API
* Publisher confirm
* Policies
* Shovels
* Queue federation
* Exchange federation
* Dead-lettering
* TTL support on queue, message, and policy level
* CC/BCC
* Alternative exchange
* Exchange to exchange bindings
* Direct-reply-to RPC
* Users and ACL rules
* VHost separation
* Consumer cancellation
* Queue max-length
* Importing/export definitions
* Priority queues
* Delayed exchanges
* Rewindable queues (all messages that are published to an exchange
  are stored and can be dumped into a queue when a certain binding is
  made, even if they have already been consumed before)

Currently missing features

* WebSockets
* Clustering
* Plugins
* Transactions (probably won't implement)

### Known differences to other AMQP servers

There are few edge-cases that are handled a bit differently in AvalancheMQ compared to other AMQP servers.

* Messages being reject and requeued with TTL 0 is delivered to consumers if there are any, not expired
* When comparing queue/exchange/binding arguments all number types (e.g. 10 and 10.0) are considered equivalent
* TTL of queues and messages are correct to the second, not to the millisecond
* Messages are not expired if there are active consumers
* Newlines are not removed from Queue or Exchange names, they are forbidden
* Impersonator tag (for overriding user_id) is not supported (yet)

### Persistent Exchange

A persistent exchange will store all messages coming into the exchange
even though there are no queue bindings on that exchange, this differs
from other exchanges where messages will be dropped if the exchange 
doesn't have any bindings. 
The exchange will also keep the message in the exchange after the 
message has been routed to all queue bindings. 

When a new binding get applied to the exchange additional arguments
can be applied which decided if these stored messages should be routed
to the new queue or not. 
For example, you can have a publisher that has been writing messages to a 
exchange for a while but you notice that no queue has been bound to 
that exchange. But since the exchange is persistent you can bind a new
queue saying that all existing message in the exchange should be routed
the to newly bounded queue. 

#### Message selection

There are currently three arguments you can give to the exchange to tell
it which messages should be published to the queue.

If the exchange has 10 messages persisted, each box represent a 
message where the first message published to the exchange is the one
far to the right, message 0.

```
[9] [8] [7] [6] [5] [4] [3] [2] [1] [0]
```

##### 1. x-head

By supplying `x-head` as argument to the binding you can select to 
get X number of message start counting from the oldest message. 
The value for `x-head` can be both positive and negative, and this
has different meaning which is illustrated below.

If you bind a queue with the argument `x-head=3` messages
0, 1 and 2 will be routed to your queue. 

If you bind a queue with the argument `x-head=-3` you will get
all the messages except the last 3 messages.
So for the example queue above you would get messages 0, 1, 2, 3, 4, 5, 6 
routed to your queue. 

##### 2. x-tail

`x-tail` is very similar to `x-head` but it counts from the other
direction. 

If you bind a queue with the argument `x-tail=3` messages
7, 8 and 9 will be routed to your queue. 

If you bind a queue with the argument `x-tail=-3` you would get
all the messages except the first 3 messages. So looking at the example
above you would get messages 3, 4, 5, 6, 7, 8 and 9 routed to your queue. 

##### 3. x-from

`x-from` allows you to be very specific on which messages to get.
Instead of saying give me the oldest or newest like `x-head` and
`x-tail`, `x-from` allows you to say exactly which message to start from 
to route to the queue. 
Each message consumed from a persistent exchange will have an additional
argument `x-offset` which you can use to request that message again. 

If you specify a `x-offset=0` or an offset that doesn't exist you will
get all messages stored in the exchange.

Example

You consume messages from a queue that is bound to a persistent
exchange, some message fails to be process but you missed to re-queue the
message. If you have been logging `x-offset` for each message you can use 
that value, bind a new queue to the exchange and supply that
value as `x-from` for that binding and the new queue would get all
messages from that offset. 

## Performance

A single c5.large EC2 instance, with a 2 TB ST1 EBS drive (XFS formatted),
can sustain about 250.000 messages/s (16 byte msg body, single queue, single producer,
single consumer). A single producer can push 550.000 msgs/s and if there's no producers
consumers can receive 730.000 msgs/s. When the message size is 1MB the
instance's network speed becomes the bottleneck at 10 Gbit/s. When the
OS disk cache is full the EBS performance becomes the bottleneck,
at about 500 MB/s.

Enqueueing 10 million messages only uses 80MB RAM. 8000
connection uses only about 400 MB RAM. Declaring 100.000 queues uses about 100
MB RAM. About 1.600 bindings per second can be made to non-durable queues,
and about 1000 bindings/second to durable queues.

## Installation

In Debian/Ubuntu:

```bash
curl -L https://packagecloud.io/cloudamqp/avalanchemq/gpgkey | sudo apt-key add -
echo "deb https://packagecloud.io/cloudamqp/avalanchemq/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/avalanchemq.list

sudo apt update
sudo apt install avalanchemq
```

In CentOS/Redhat/Amazon Linux:

```bash
sudo tee /etc/yum.repos.d/avalanchemq.repo << EOF
[avalanchemq]
name=avalanchemq
baseurl=https://dl.bintray.com/cloudamqp/rpm
gpgcheck=0
repo_gpgcheck=0
enabled=1
EOF
sudo yum install avalanchemq
sudo systemctl start avalanchemq
```

From source:

```bash
git clone git@github.com:avalanchemq/avalanchemq.git
cd avalanchemq
shards build --release --production
install bin/avalanchemq /usr/local/bin/avalanchemq
```

Refer to
[Crystal's installation documentation](https://crystal-lang.org/docs/installation/)
on how to install Crystal.

## Usage

AvalancheMQ only requires one argument, and it's a path to a data directory:

`avalanchemq -D /var/lib/avalanchemq`

More configuration options can be viewed with `-h`,
and you can specify a configuration file too, see [extras/config.ini](extras/config.ini)
for an example.

## Docker

It is possible to run AvalancheMQ using docker. To build the image run:

`docker build -t avalanchemq .`

This will create a docker image tagged as avalanchemq:latest that we then can use to launch an
instance of AvalancheMQ by executing:

`docker run --rm -p 15672:15672 -p 5672:5672 -v data:/data --name avalanchemq avalanchemq:latest`

You are now able to visit the management UI at [http://localhost:15672](http://localhost:15672) and
start publishing/consuming messages. The container can be killed
by running:

`docker kill avalanchemq`

## OS configuration

If you have a lot of clients that open connections
at the same time, e.g. after a restart, you may see
"kernel: Possible SYN flooding on port 5671" in the syslog.
Then you probably should increase `net.ipv4.tcp_max_syn_backlog`:

```bash
sysctl -w net.ipv4.tcp_max_syn_backlog=2048 # default 512
```

## Debugging

In Linux `perf` is the tool of choice when tracing and measuring performance.

To see which syscalls that are made use:

```bash
sudo perf trace -p $(pidof avalanchemq)
```

To get a live analysis of the mostly called functions, run:

```bash
sudo perf top -p $(pidof avalanchemq)
```

A more [detailed tutorial on `perf` is available here](https://perf.wiki.kernel.org/index.php/Tutorial).

In OS X the app [`Instruments` that's bundled with Xcode can be used for tracing](https://help.apple.com/instruments/mac/current/).

Memory garage collection can be diagnosed with [boehm-gc environment variables](https://github.com/ivmai/bdwgc/blob/master/doc/README.environment).

## Contributing

1. Fork, create feature branch
1. Build with `shards build --release`
1. Performance test with `bin/avalanchemqperf throughput` and compare against `master`
1. Submit pull request

### Develop

1. Run specs with `crystal spec`
1. Compile and run locally with `crystal run src/avalanchemq.cr -- -D /tmp/amqp`
1. Build with `shards build --release`

### Release

1. Update `CHANGELOG.md`
1. Bump version in `shards.yml` & `src/avalanchemq/version.cr`
1. Create and push tag
1. `build/with_vagrant && build/bintray-push`

## Contributors

* [Carl Hörberg](mailto:carl@84codes.com)
* [Anders Bälter](mailto:anders@84codes.com)
* [Magnus Landerblom](mailto:mange@cloudamqp.com)
* [Magnus Hörberg](mailto:magnus@cloudamqp.com)
* [Johan Eckerström](mailto:johan.e@cloudamqp.com)
* [Anton Dalgren](mailto:anton@cloudamqp.com)
* [Patrik Ragnarsson](mailto:patrik@84codes.com)
* [Oskar Gustafsson](mailto:oskar@84codes.com)

## License

The software is licensed under the [Apache License 2.0](LICENSE).

Copyright 2018-2021 84codes AB

AvalancheMQ is a trademark of 84codes AB
