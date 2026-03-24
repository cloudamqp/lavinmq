
All routed messages in LavinMQ are written directly to the disk, into something called a _Message Store_. The Message Store is a series of files (segments). A routed message is located in the Message Store, with a reference from the queue’s index to the message store.

![Message Store in LavinMQ](img/docs/lavinmq-message-store.jpg)

A routed message is a message that is not dropped by the exchange, including messages that are routed via alternate exchanges. Once a message is routed to a queue the segment number, the position in that segment, and some other metadata are written to its queues index and then placed in the queue.

When LavinMQ starts up, it allocates [memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file) for message segments using the [mmap](https://en.wikipedia.org/wiki/Mmap) syscall. However, to prevent unnecessary memory usage, we unmap these files and free up the allocated memory when they are not in use. When a file needs to be accessed, we map it again with mmap and use only the memory needed for that specific segment. This keeps the memory footprint very low and allows LavinMQ to sustain unbound queue lengths. Apart from this, messages are copied directly to the OS caching layer without the need for syscalls making the writing very fast even for very small messages.

### How are messages handled by LavinMQ?

Routed messages are appended to the message store and also written to the queue index for a queue. Messages in the message store are deleted when no queue index has a reference to a position in the message store.

### How are messages stored in the Message Store?

Each incoming message is appended to the last segment and prefixed with a timestamp, that includes its exchange name, routing key, and message headers.

### How are messages stored in the Message Queue?

A message in the Message Queue is simply a reference from the queue’s index to the Message Store.

## Built in Crystal

LavinMQ is written in [Crystal](https://crystal-lang.org/), a modern language built on the LLVM, that has a Ruby-like syntax and uses an event loop library for IO. It is also garbage collected, adopts a CSP-like [concurrency model](https://crystal-lang.org/docs/guides/concurrency.html), and compiles down to a single binary. You can liken it to Go, but with a nicer syntax.

## Message store on disk

Each queue is backed by a message store on disk in a series of files (segments) that can grow to 8MB each. Each incoming message is appended to the last segment, prefixed with a timestamp, its exchange name, routing key, and message headers.

When a message is routed to a queue, the segment number and the position in that segment are written to each queue's queue index. The queue index is just an [in-memory array](https://crystal-lang.org/api/Deque.html) of segment numbers and file positions and some metadata. In the case of durable queues, the message index is also appended to a file.

### Consume a message from the message store

When a message is consumed, LavinMQ removes the segment-position from the segment. It also writes the segment-position to an "ack" file. That way the queue index will be restored on boot by reading all the segment-position information stored in the queue index file, then excluding all the segment-position read information from the "ack" file. The queue index is rewritten when the "ack" file becomes 16 MB, that is, every 16 _ 1024 _ 1024 / 8 = 2097152 message. Then the current in-memory queue index is written to a new file and the "ack" file is truncated.

Segments in the queues's message store are deleted when no queue index is a reference to a position in that segment.

## Definitions file

Declarations of queues, exchanges, and bindings are written to a definitions file (if the target is durable), and encoded with the AMQP frame they came in as. Periodically this file is garbage collected by writing only the current in-memory state to the file (getting rid of all delete events). This file is read on boot to restore all definitions.

## Storage of users, vhosts, policies

All non-AMQP objects like users, vhosts, policies, etc. are stored in JSON files. Most often these types of objects do not have a high turnover rate. JSON, in this case, makes it easy for operators to modify things when the server is not running if needed.

In the data directory, `users.json` and `vhosts.json` are stored, and each vhost has a directory in which definitions.amqp (encoded as AMQP frames), policies.json and the messages named such as msgs.0000000124 are stored as well. Each vhost directory is named after the sha1 hash of its real name. The same goes for the queue directories in the vhost directory. The queue directories only have two files, _ack_, and _enq_.

### Publish

`Client#read_loop` reads from the socket, it calls `Channel#start_publish` for the `Basic.Publish` frame and `Channel#add_content` for Body frames. When all content has been received (and appended to an _IO::Memory object_) it calls `VHost#publish` with a Message struct. `VHost#publish` finds all matching queues, writes the message to the message store, and then calls `Queue#publish` with the segment position. `Queue#publish` writes to the queue index file (if it's a durable queue).

### Consume

When `Client#read_loop` receives a `Basic.Consume` frame, it will create a Consumer class and add it to the queue's list of consumers. The Queue got a _deliver_loop_ fiber that will loop over the list of consumers and deliver a message to each.
