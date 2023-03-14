[![Build Status](https://github.com/cloudamqp/lavinmq/workflows/CI/badge.svg)](https://github.com/cloudamqp/lavinmq/actions)
[![Build Status](https://api.cirrus-ci.com/github/cloudamqp/lavinmq.svg)](https://cirrus-ci.com/github/cloudamqp/lavinmq)

# ![LavinMQ](static/img/logo-lavinmq.png)

A message queue server that implements the AMQP 0-9-1 protocol.
Written in [Crystal](https://crystal-lang.org/).

Aims to be very fast, has low RAM requirements, handles very long queues,
many connections, and requires minimal configuration.

Read more at [LavinMQ.com](https://lavinmq.com)

## Installation

### From source

Begin with installing Crystal. Refer to
[Crystal's installation documentation](https://crystal-lang.org/install/)
on how to install Crystal.

Clone the git repository and build the project. 

```sh
git clone git@github.com:cloudamqp/lavinmq.git
cd lavinmq
shards build --release --production
install bin/lavinmq /usr/local/bin/lavinmq # optional
```

Now, LavinMQ is ready to be used. You can check the version with:

```sh
lavinmq -v
```

### Debian/Ubuntu

```sh
curl -L https://packagecloud.io/cloudamqp/lavinmq/gpgkey | sudo apt-key add -
. /etc/os-release
echo "deb https://packagecloud.io/cloudamqp/lavinmq/$ID $VERSION_CODENAME main" | sudo tee /etc/apt/sources.list.d/lavinmq.list
sudo apt-get update
sudo apt-get install lavinmq
```

## Usage

LavinMQ only requires one argument, and it's a path to a data directory. 

Run LavinMQ with: 
`lavinmq -D /var/lib/lavinmq`

More configuration options can be viewed with `-h`,
and you can specify a configuration file too, see [extras/config.ini](extras/config.ini)
for an example.

## Docker

Docker images are published to [Docker Hub](https://hub.docker.com/repository/docker/cloudamqp/lavinmq).
Fetch and run the latest version with:

`docker run --rm -it -P -v /var/lib/lavinmq:/tmp/amqp cloudamqp/lavinmq`

You are then able to visit the management UI at [http://localhost:15672](http://localhost:15672) and
start publishing/consuming messages to `amqp://guest:guest@localhost`.

## Debugging

In Linux, `perf` is the tool of choice when tracing and measuring performance.

To see which syscalls that are made use:

```sh
perf trace -p $(pidof lavinmq)
```

To get a live analysis of the mostly called functions, run:

```sh
perf top -p $(pidof lavinmq)
```

A more [detailed tutorial on `perf` is available here](https://perf.wiki.kernel.org/index.php/Tutorial).

In OS X the app, [`Instruments` that's bundled with Xcode can be used for tracing](https://help.apple.com/instruments/mac/current/).

Memory garbage collection can be diagnosed with [boehm-gc environment variables](https://github.com/ivmai/bdwgc/blob/master/doc/README.environment).

## Contributing

1. Fork, create feature branch
1. Build with `make -j`
1. Performance test with `bin/lavinmqperf throughput` and compare against `main`
1. Submit pull request

### Develop

1. Run specs with `crystal spec`
1. Compile and run locally with `crystal run src/lavinmq.cr -- -D /tmp/amqp`
1. Pull js dependencies with `make js`
1. Build API docs with `make docs` (requires `npx`)
1. Build with `shards build`

### Release

1. Update `CHANGELOG.md`
1. Bump version in `shards.yml`
1. Create and push an annotated tag (`git tag -a v$(shards version)`), put the changelog of the version in the tagging message

## LavinMQ with various plattforms
All AMQP client libraries work with LavinMQ and there are AMQP client libraries for almost every platform on the market. Here are  guides for a couple of common plattforms. 

1. [Ruby](https://lavinmq.com/documentation/ruby-sample-code)
2. [Node.js](https://lavinmq.com/documentation/nodejs-sample-code)
3. [Java](https://lavinmq.com/documentation/java-sample-code)
4. [Python](https://lavinmq.com/documentation/python-sample-code)
5. [PHP](https://lavinmq.com/documentation/php-sample-code)
6. [Crystal](https://lavinmq.com/documentation/crystal-sample-code)

## Performance

A single m6g.large EC2 instance, with a GP3 EBS drive (XFS formatted),
can sustain about 700.000 messages/s (16 byte msg body, single queue, single producer,
single consumer). A single producer can push 1.600.000 msgs/s and if there's no producers
auto-ack consumers can receive 1.200.000 msgs/s.

Enqueueing 10 million messages only uses 80 MB RAM. 8000
connection uses only about 400 MB RAM. Declaring 100.000 queues uses about 100
MB RAM. About 1.600 bindings per second can be made to non-durable queues,
and about 1000 bindings/second to durable queues.

## Implementation

LavinMQ is written in [Crystal](https://crystal-lang.org/), a modern
language built on the LLVM, that has a Ruby-like syntax, uses an event loop
library for IO, is garbage collected, adopts a CSP-like [concurrency
model](https://crystal-lang.org/docs/guides/concurrency.html) and compiles down
to a single binary. You can liken it to Go, but with a nicer syntax.

Instead of trying to cache messages in RAM, we write all messages as fast as we can to
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
"ack" file. The queue index is rewritten when the "ack" file becomes 16 MB,
that is, every `16 * 1024 * 1024 / 8 = 2097152` message.
Then the current in-memory queue index is written to a new file and the
"ack" file is truncated.

Segments in the vhost's message store are being deleted when no queue index has
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

Here is an architectural description of the different flows in the server.

#### Publish

`Client#read_loop` reads from the socket, it calls `Channel#start_publish` for the Basic.Publish frame
and `Channel#add_content` for Body frames. When all content has been received
(and appended to an `IO::Memory` object) it calls `VHost#publish` with a `Message` struct.
`VHost#publish` finds all matching queues, writes the message to the message store and then
calls `Queue#publish` with the segment position.
`Queue#publish` writes to the queue index file (if it's a durable queue).

#### Consume

When `Client#read_loop` receives a Basic.Consume frame it will create a `Consumer` class and add it to
the queue's list of consumers. The Queue got a `deliver_loop` fiber that will loop over the list of
consumers and deliver a message to each.

## Features

- AMQP 0-9-1 compatible
- AMQPS (TLS)
- HTTP API
- Publisher confirm
- Transactions
- Policies
- Shovels
- Queue federation
- Exchange federation
- Dead-lettering
- TTL support on queue, message, and policy level
- CC/BCC
- Alternative exchange
- Exchange to exchange bindings
- Direct-reply-to RPC
- Users and ACL rules
- VHost separation
- Consumer cancellation
- Queue max-length
- Importing/export definitions
- Priority queues
- Delayed exchanges
- Rewindable queues (all messages that are published to an exchange
  are stored and can be dumped into a queue when a certain binding is
  made, even if they have already been consumed before)
- AMQP WebSocket

Currently missing features

- Clustering
- Plugins

### Known differences to other AMQP servers

There are a few edge-cases that are handled a bit differently in LavinMQ compared to other AMQP servers.

- When comparing queue/exchange/binding arguments all number types (e.g. 10 and 10.0) are considered equivalent
- When comparing queue/exchange/binding arguments non-effective parameters are also considered, and not ignored
- TTL of queues and messages are correct to the 0.1 second, not to the millisecond
- Newlines are not removed from Queue or Exchange names, they are forbidden

### Persistent Exchange

A persistent exchange will store all messages coming into the exchange
even though there are no queue bindings on that exchange, this differs
from other exchanges where messages will be dropped if the exchange
doesn't have any bindings.
The exchange will also keep the message in the exchange after the
message has been routed to all queue bindings.

When a new binding gets applied to the exchange additional arguments
can be applied which decides if these stored messages should be routed
to the new queue or not.
For example, you can have a publisher that has been writing messages to a
exchange for a while but you notice that no queue has been bound to
that exchange. Since the exchange is persistent you can bind a new
queue saying that all existing messages in the exchange should be routed
the to newly bound queue.

#### Message selection

There are currently three arguments you can give to the exchange to tell
it which messages should be published to the queue.

If the exchange has 10 messages persisted, each box represent a
message where the first message published to the exchange is the one
to the far right, message 0.

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

## Contributors

- [Carl Hörberg](mailto:carl@84codes.com)
- [Anders Bälter](mailto:anders@84codes.com)
- [Magnus Landerblom](mailto:mange@cloudamqp.com)
- [Magnus Hörberg](mailto:magnus@cloudamqp.com)
- [Johan Eckerström](mailto:johan.e@cloudamqp.com)
- [Anton Dalgren](mailto:anton@cloudamqp.com)
- [Patrik Ragnarsson](mailto:patrik@84codes.com)
- [Oskar Gustafsson](mailto:oskar@84codes.com)
- [Tobias Brodén](mailto:tobias@84codes.com)
- [Christina Dahlén](mailto:christina@84codes.com)
- [Erica Weistrand](mailto:erica@84codes.com)
- [Viktor Erlingsson](mailto:viktor@84codes.com)

## License

The software is licensed under the [Apache License 2.0](LICENSE).

Copyright 2018-2021 84codes AB

LavinMQ is a trademark of 84codes AB
