# Implementation Details

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
- A consumer with filter values will only receive messages that contain ALL the filter values it has specified.
- A consumer with filter values will not receive messages without filter values unless they have set `x-stream-match-unfiltered` to true.
- Consumers without any filter values will receive all messages.

#### Publishing Filtered Messages
When publishing messages that should be filtered, include the `x-stream-filter-value` header. `x-stream-filter-value` should be a string of comma-separated values.

#### Consuming Filtered Messages

To enable filtering on the consumer, set the following arguments when creating a consumer:

- `x-stream-filter`: A comma-separated string of filter values the consumer is interested in
- `x-stream-match-unfiltered`: A boolean (default: `false`) that determines whether to receive messages that don't have any filter values