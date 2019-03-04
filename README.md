[![Build Status](https://travis-ci.com/84codes/avalanchemq.svg?token=rfwynuMNGnX9tuyspVud&branch=master)](https://travis-ci.com/84codes/avalanchemq)

# ![AvalancheMQ](static/img/logo-avalanche-mq.png)

A message queue server that implements the AMQP 0-9-1 protocol.
Written in [Crystal](https://crystal-lang.org/).

Aims to be very fast, have low RAM requirements, handle extremely long queues,
many connections and require minimal configuration.

## Implementation

AvalancheMQ is written in [Crystal](https://crystal-lang.org/), a modern
language built on the LLVM, that has a Ruby-like syntax, uses an event loop
library for IO, is garbage collected, adopts a CSP like [concurrency
model](https://crystal-lang.org/docs/guides/concurrency.html) and complies down
to a single binary. You can liken it to Go but with a nicer syntax.

Instead of trying to cache message in RAM we write all messages as fast as we can to
disk and let the OS cache do the cacheing.

Each vhost is backed by message store on disk, it's just a series of files (segments),
that can grow to 256 MB each. Each incoming message is appended to the last segment,
prefixed with a timestamp, its exchange name, routing key and message headers.
If the message is routed to a queue then the segment number and the position in
that segment is written to each queue's queue index. The queue index is
just an [in-memory array](https://crystal-lang.org/api/Deque.html)
of segment numbers and file positions. In the case of durable queues
the message index is also appended to a file.

When a message is being consumed it removes the segment-position from the queue's
in-memory array, and write the segment-position to an "ack" file. That way
we can restore the queue index on boot by read all the segment-position stored
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
by writing only the current in memory state to the file (getting rid
of all delete events). This file is read on boot to restore all definitions.

All non-AMQP objects like users, vhosts, policies etc. are stored in
JSON files. Most often these type of objects does not have a high
turnover rate so we believe that JSON in this case makes it easy for
operators to modify things when the server is not running if ever needed.

In the data directory we store `users.json` and `vhosts.json` as mentioned earlier,
and each vhost has a directory in which we store `definitions.amqp`
(encoded as AMQP frames), `policies.json` and the messages named such as `msgs.0000000124`.
Each vhost directory is named after the sha1 hash of its real name. Same goes
for the queue directories in the vhost directory. The queue directories only has two files,
`ack` and `enq`, also described earlier.

## Features

* AMQP 0-9-1 compatible
* AMQPS (TLS)
* Publisher confirm
* Policies
* Shovels
* HTTP API
* Queue federation
* Dead-lettering
* TTL support on queue, message and policy level
* CC/BCC
* Alternative exchange
* Exchange to exchange bindings
* Direct-reply-to RPC
* Users and ACL rules
* VHost separation
* Consumer cancellation
* Queue max-length
* Importing/export definitions

Currently missing features

* Websockets
* Exchange federation
* Clustering
* Plugins
* Priority queues
* Delayed exchanges
* Transactions (probably won't implement)

Wish list

* Rewindable queues (all messages that is published to an exchanged
  is stored and can be dumped into a queue when a certain binding is
  made, even if they already have been consumed once before)
* Horizontal scaling
* Built in stream processor engine

## Performance

A single m5.large EC2 instance, with a 500 GB GP2 EBS drive (XFS formatted),
can sustain about 150.000 messages/s (1KB each, single queue, single producer,
single consumer). When the message size is 1MB the instance's network speed
becomes the bottleneck at 10 Gbit/s. When the OS disk cache is full
the EBS performance becomes the bottleneck, at about 250 MB/s.

Enqueueing 10 million messages only uses 80MB RAM. 8000
connection uses only about 400 MB RAM. Declaring 100.000 queues uses about 100
MB RAM. About 20.000 bindings per second can be made to both durable and
non-durable queues.

## Installation

In Debian/Ubuntu:

```bash
cat > /etc/apt/sources.list.d/avalanchemq.list << EOF
deb [trusted=yes] https://apt.avalanchemq.com stable main
EOF

apt update
apt install avalanchemq
```

From source:

```bash
git clone git@github.com:avalanchemq/avalanchemq.git
cd avalanchemq
shards build --release --production
install -s bin/avalanchemq /usr/local/bin/avalanchemq
```

Refer to
[Crystal's installation documentation](https://crystal-lang.org/docs/installation/)
on how to install Crystal.

## Usage

AvalancheMQ only requires one argument, and it's a path to a data directory:

`avalanchemq -D /var/lib/avalanchemq`

More configuration options can be viewed with `-h`,
and you can specify a configration file too, see [extras/config.ini](extras/config.ini)
for an example.

## OS configuration

If you have a lot of clients that open connections
at the same time, eg. after a restart, you may see
"kernel: Possible SYN flooding on port 5671" in the syslog.
Then you probably should increase `net.ipv4.tcp_max_syn_backlog`:

```bash
sysctl -w net.ipv4.tcp_max_syn_backlog=2048 # default 512
```

## Debugging

In Linux `strace` is pretty good for understanding what the process is doing at any given time,
at least which system calls it's doing.

Run `strace` with `-c` for some time and get a table of system calls made and how long time
they took.

```bash
sudo strace -fp $(ps -C avalanchemq -o pid=) -c
```

Run `strace` with `-e trace=` and the syscalls you want to monitor
for more details of which files it's writing to and what etc.

```bash
sudo strace -fytTp $(ps -C avalanchemq -o pid=) -e trace=file,read,write
```

`perf` can be used to trace the application's method calls in Linux:

```bash
sudo perf top -p $(ps -C avalanchemq -o pid=)
```

In OS X the app Instruments that's bundled with Xcode can be used to trace
what the app is spending time at. However, it only works (or at least
is meaningful) if the debug symbols haven't been stripped, which we do in the
deb packages.

## Contributing

Fork, create feature branch, submit pull request.

## Contributors

* [Carl Hörberg](carl@84codes.com)
* [Anders Bälter](anders@84codes.com)

## License

The software is licensed under the [Apache License 2.0](LICENSE).

Copyright 2018 84codes AB

AvalancheMQ is a trademark of 84codes AB
