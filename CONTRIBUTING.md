# Contributing
You are interested in contributing to LavinMQ, but you are not sure how to? 
Right now, you can contribute to LavinMQ in one of two ways:

1. [Code contributions](#code-contributions)
2. [Non-code contributions](#non-code-contributions)

## Code contributions

The first step to making a code contribution, is starting a conversation around what you'd like to work on. You can start a conversation around an existing [issue](https://github.com/cloudamqp/lavinmq/issues) or a new one that you've created - refer to [reporting an issue](#report-an-issue). Next:

1. Fork, create feature branch
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

## Non-Code contributions

Your schedule won't allow you make code contributions? Still fine, you can:

### [Report an issue](https://github.com/cloudamqp/lavinmq/issues/new)

- This could be an easily reproducible bug or even a feature request.
- If you spot an unexpected behaviour but you are not yet sure what the underlying bug is, the best place to post is [LavinMQ's community Slack](https://join.slack.com/t/lavinmq/shared_invite/zt-1v28sxova-wOyhOvDEKYVQMQpLePNUrg). This would allow us to interactively figure out what is going on. 

### [Give us some feedback](https://github.com/cloudamqp/lavinmq/discussions)

We are also curious and happy to hear about your experience with LavinMQ. You can email us via contact@cloudamqp.com or reach us on Slack. Not sure what to write to us?

- You can write to us about your first impression of LavinMQ
- You can talk to us about features that are most important to you or your organization
- You can also just tell us what we can do better


# LavinMQ Implementation Details

LavinMQ is written in [Crystal](https://crystal-lang.org/), a modern
language built on the LLVM, with a Ruby-like syntax. It uses an event loop
library for IO, is garbage collected, adopts a CSP-like [concurrency
model](https://crystal-lang.org/docs/guides/concurrency.html) and compiles down
to a single binary. You can liken it to Go, but with a nicer syntax.

### Storage Architecture

Instead of trying to cache messages in RAM, we write all messages as fast as we can to disk and let the OS cache do the caching.

Each queue is backed by a message store on disk, which is just a series of files (segments),
by default 8MB each. Message segments are memory-mapped files allocated using the mmap syscall.
Each incoming message is appended to the last segment, prefixed with a timestamp, its exchange 
name, routing key and message headers.

### Message Processing

When a message is being consumed it reads sequentially from the segments.
Each acknowledged (or rejected) message position in the segment is written to an "ack" file
(per segment). If a message is requeued its position is added to a in memory queue.

On boot all acked message positions are read from the "ack" files and then
when delivering messages skip those when reading sequentially from the message segments.
Segments are deleted when all message in them are acknowledged.

### Definitions

Declarations of queues, exchanges and bindings are written to a definitions
file (if the target is durable), encoded as the AMQP frame they came in as.
Periodically this file is compacted/garbage-collected by writing only the 
current in-memory state to the file (getting rid of all delete events). 
This file is read on boot to restore all definitions.

All non-AMQP objects like users, vhosts, policies, etc. are stored in
JSON files. Most often these types of objects does not have a high
turnover rate, so we believe that JSON in this case makes it easy for
operators to modify things when the server is not running, if ever needed.

In the data directory we store `users.json` and `vhosts.json` as mentioned earlier,
and each vhost has a directory in which we store `definitions.amqp`
(encoded as AMQP frames), `policies.json` and the messages named such as `msgs.0000000124`.
Each vhost directory is named after the sha1 hash of its real name. The same goes
for the queue directories in the vhost directory. The queue directories only has two files,
`ack` and `enq`, also described earlier.

Transient queues are stored in a separate `transient` folder in the vhost directory, and it
is removed any time LavinMQ shuts down or starts up.

### Flows

Here is an architectural description of the different flows in the server.

#### Publish

`Client#read_loop` reads from the socket, it calls `Channel#start_publish` for the Basic.Publish frame
and `Channel#add_content` for Body frames. When all content has been received
(and appended to an `IO::Memory` object) it calls `VHost#publish` with a `Message` struct.
`VHost#publish` finds all matching queues, writes the message to the message store and then
calls `Queue#publish` with the segment position. `Queue#publish` writes to the message store.

#### Consume

When `Client#read_loop` receives a Basic.Consume frame it will create a `Consumer` class and add it to
the queue's list of consumers. Each consumer has a `deliver_loop` fiber that will be notified
by an internal `Channel` when new messages are available in the queue.


## Stream Queues

Stream queues provide an append-only log structure that allows multiple consumers to read the same messages independently. Unlike standard queues, messages in stream queues aren't deleted when consumed, making them ideal for event sourcing patterns and multi-consumer scenarios.

Each consumer can start reading from anywhere in the queue using the `x-stream-offset` consumer argument and can process messages at their own pace.

### Stream Queue Filtering

Stream queues support message filtering, allowing consumers to receive only messages that match specific criteria. This is useful for consuming a subset of messages without creating multiple queues. 

#### How Filtering Works
- Consumers can filter messages based on message headers
- Two matching strategies are supported:
  - "all" (default): A message must match ALL filter conditions to be delivered
  - "any": A message matching ANY filter condition will be delivered
- Messages without filter values won't be delivered to consumers with filters unless `x-stream-match-unfiltered` is set to `true`
- Consumers without any filter conditions will receive all messages

#### Publishing Filtered Messages
When publishing messages that should be filtered, include header values that can be matched against:
- The `x-stream-filter-value` header (a comma-separated string of values)
- Any custom headers for more advanced filtering scenarios

#### Consuming Filtered Messages

To enable filtering on the consumer, set the following arguments when creating a consumer:

- `x-stream-filter`: Supports multiple formats:
  - String: A comma-separated list of values to match against `x-stream-filter-value` headers
  - Table: Key-value pairs where both the header name and value must match
  - Array: A collection of filter conditions in either string or table format
- `x-filter-match-type`: Determines the matching strategy:
  - "all" (default): All filter conditions must match
  - "any": At least one filter condition must match
- `x-stream-match-unfiltered`: Boolean (default: `false`) that determines whether to receive messages without filter values