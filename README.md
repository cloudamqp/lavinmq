[![Build Status](https://travis-ci.com/84codes/avalanchemq.svg?token=rfwynuMNGnX9tuyspVud&branch=master)](https://travis-ci.com/84codes/avalanchemq)

<img src="avalanche.png" align="right" />

# AvalancheMQ

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

Each vhost is backed by message store on disk, it's just a series of files (segments),
that can grow to 256 MB each. Each incoming message is appended to the last segment,
prefixed with a timestamp, its exchange name, routing key and message headers.
If the message is routed to a queue then the segment number and the position in
that segment is written to each queue's queue index. The queue index is
just an [in-memory array](https://crystal-lang.org/api/Deque.html)
and a file to which the segment number and position is appended (the file is
only used if it's a durable queue and the message is marked as persistent).

When a message is being consumed it removes the segment-position from the queue's
in-memory array, and write the segment-position to an "ack" file. That way
we can restore the queue index on boot by read all the segment-position stored
in the queue index file, then exclude all the segment-position read from the
"ack" file.  The queue index is rewritten when the "ack" file becomes 16 MB,
that is, every 16*1024*1024/8=2097152 message. Then the current in-memory queue
index is written to a new file and the "ack" file is truncated.

Segments in the vhost's message store are being deleted when no queue index as
a reference to a position in that segment.

## Installation

In Debian/Ubuntu:

```
cat > /etc/apt/sources.list.d/avalanchemq.list << EOF
deb [trusted=yes] https://apt.avalanchemq.com stable main
EOF

apt update
apt install avalanchemq
```

From source:

```
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

## OS configuration

There are a couple of OS kernel parameters you need to modify for better performance:

```
sysctl -w net.core.somaxconn=2048
...
```

## Performance

A single m5.large EC2 instance, with a 500 GB GP2 EBS drive (XFS formatted),
can sustain about 150.000 messages/s (1KB each, single queue, single producer,
single consumer). Enqueueing 10 million messages only uses 80MB RAM. 8000
connection uses only about 400 MB RAM. Declaring 100.000 queues uses about 100
MB RAM.

## Contributing

Fork, create feature branch, submit pull request.

## Contributors

- [Carl Hörberg](carl@84codes.com)
- [Anders Bälter](anders@84codes.com)

## License

The software is licensed under the [Apache License 2.0](LICENSE).

Copyright 2018 84codes AB

AvalancheMQ is a trademark of 84codes AB
