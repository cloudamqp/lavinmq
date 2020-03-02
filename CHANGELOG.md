# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.6] - 2020-03-02

### Added
- More reporting when on signal USR1
- Clear ShortString string pool on signal USR2

## [0.9.5] - 2020-02-27

### Fixed
- Message expiration loop didn't reset read position

## [0.9.4] - 2020-02-27

### Fixed
- Message expiration could end up in an infinite loop
- Auto-delete queues are not automatically deleted at shutdown/compaction
- Messages dropped due to queue max-length are now decreasing segment refcount
- Bsearch is used for reinserting messages into ready queue
- PUT /api/user now returns 204 for updating users (earlier 200)
- More info logging around queue index compaction

## [0.9.3] - 2020-02-25

### Fixed
- Channel#timeout is used for queue/msg expiration

### Added
- Signal USR1 prints various stats
- Signal USR2 forces a GC collect
- Signal HUP only reloads the config file

### Changed
- Only ignore publish frames on closed channels
- Different log message if user is missing or password is wrong

## [0.9.2] - 2020-02-17

### Fixed
- Changes to users (password, tags) are store to disk

### Added
- avalanchemqperf: --persistent flag for throughput tests
- avalanchemqperf: queues are now bound to the exchange

### Changed
- Crystal 0.33.0
- avalanchemqperf: use fibers, don't fork
- Only ack:ing persistent messages are now written to the queue index
- Mutexes around the unacked dequeue in Consumer, for thread safety

## [0.9.1] - 2020-02-13

### Changed
- GC message segments periodically instead of on rotation
- Compacting queue indexes in a more efficient way
- New implementation of Shovel, based on amqp-client.cr

### Added
- gc_segments_interval configration, how often to GC message segments
- queue_max_acks configration, compact/GC queue indexes after this many acks
- Reloading configuration on SIGHUP

## [0.9.0] - 2020-01-30

### Fixed
- Crystal 0.32.x compatiblity (avoid reentrant mutexes)
- Reference couting of messages in segments for GC purposes could go negative
- Multiple dead lettering bugs
- Faster restoring of indexes by using another data type
- Make amq.default exchange clickable in the web UI
- Make it possible to unbind bindings with empty routing key in web UI
- Dead letter publishes won't cause a publish loop due to cached look up

### Added
- Queue bind with empty queue name default to last declared tmp queue
- Don't allow binding/unbinding from the default exchange
- Nicer formatting of numbers in the web UI
- Now possible to unbind binding from the exchange view in the web UI

### Changed
- More efficent amq.default exchange

## [0.8.6] - 2019-10-03

### Fixed
- Crystal 0.31.0 and multi threading compability
- Make stat counters UInt64 to avoid overflows
- Send ConfirmOk before adding consumer to queue
- Correctly report FD limit in Linux

### Changed
- Round rates in the UI to 1 decimal
- avalanchemqperf throughput now forks for each connection instead of spawn
- Speed up GC of segments by using reference counting

## [0.8.5] - 2019-09-19

### Fixed
- Sorting of numbers is done correctly

### Changed
- Dont wakeup the heartbeat loop on connection cleanup, might be the cause of a Invalid memory access
- Don't fallocate/preallocate segment or index files, minimal performance gain on XFS

### Added
- Applications array with AvalancheMQ version added to /api/nodes response

## [0.8.4] - 2019-09-18

### Fixed
- Overview rate stats doesn't overflow anymore
- Segment size in config is respected
- Queues now closes all FDs to segments it doesn't reference

## [0.8.3] - 2019-09-16

### Fixed
- Basic::GetEmpty responses are correctly encoded
- Timestamps are correctly parsed (as seconds, not ms)

### Changed
- Logging of clients disconnected is debug level now

## [0.8.2] - 2019-07-21

### Changed
- Faster queue matching on publishing by reusing a Set
- Faster GC of unused message store segments
- Do not enable TCP_NODELAY
- Faster avalanchemqproxy

## [0.8.1] - 2019-07-19

### Changed
- Restored fairness between publishers
- Use a SparseArray as channel store, for faster access times

## [0.8.0] - 2019-07-16

### Added
- Fsyncing message store and queue index if publish confirm, when idle or every 200ms

### Changed
- AMQ::Protocol::Table is now backed by an IO::Memory and only parsed on-demand

## [0.7.13] - 2019-07-07

### Changed
- Separate buffer_size configuration for files and sockets
- Fsync when writing defintions, vhosts and users

## [0.7.12] - 2019-06-28

### Added
- Make buffer_size configurable via config and increase default from 8KB to 128KB
- Pre-allocate the max size for new segments, might improve write performance (on Linux only)

## [0.7.11] - 2019-06-20

### Changed
- Revert to write lock behavior, but yield only when queue length > 10000

## [0.7.10] - 2019-06-19

### Added
ShortStrings are now added to StringPool for reduced GC pressure, the pool size is printed on HUP

### Changed
- Make setting the timestamp property optional through a config setting (set_timestamp), default to false
- Optimized topic exchange and fanout routing, giving ~5% and ~30% throughput boost respectively
- Using a Channel to communicate between Client and Vhost when publishing, removed the need for a lock

### Fixed
- AMQPLAIN support in shovels is implemented correctly

## [0.7.9] - 2019-06-10

### Fixed
- Restore ability to login with AMQPLAIN mechanism
- Making a binding via the HTTP API without a routing key threw an exception

## [0.7.8] - 2019-06-08

### Fixed
- Fix account of position in delivery loop so that we don't have to seek on disk

### Changed
- Optimize disconnection of consumers with many unacked messages

## [0.7.7] - 2019-06-07

### Fixed
- Always copy msg body to memory before writing to disk to avoid corruption if client disconnects mid-stream
- Reset position of segment when an exception is caught in delivery loop

### Changed
- chmod the unix domain socket to 777, authentication is done on the protocol level anyway

### Added
- CHANGELOG file
