# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Memory usage monitoring is now showing current RSS, not max RSS
- Consumer cancellation doesn't requeue unacked messages
- UI: No more JSON parsing errors when the server is offline
- UI: Fix name filter pagination bug
- Support delayed exchange via policy and x-delayed-message exchange type
- Clear all user permissions to a vhost when it's deleted

### Added
- UNIX socket support for HTTP server
- UI: Show data rates and heartbeats in connections listing
- Documented how persistent exchange works in the readme

### Changed
- Only listen on localhost by default
- DEB packages are distributed via packagecloud.io

## [0.11.0] - 2020-07-01

### Changed
- Using mmap to read/write to the segment files, almost doubling the performance
- UI: format uptime, format numbers according to browser locale
- Automatically add permissions for the user creating a vhost to that vhost

### Added
- Persistent exchange support, an exchange that can republish messages to a queue
- Unroutable message count on exchanges
- perf throughput now accepts --time, --pmessages and --cmessages arguments
- perf throughput rate limiter is vastly improved and much more accurate
- robots.txt to disallow crawling of the mgmt UI

### Fixed
- The --persistent flag is actually being respected in avalanchemqperf throughput
- On shutdown, force close client connections
- Can now successfully delete shovels that has connection problems

## [0.10.7] - 2020-06-19

### Fixed
- Prevent overflow exception in shovel, don't look for short queues

## [0.10.6] - 2020-06-19

### Fixed
- Fixed overflow exception in /api/nodes if uptime was more than Int32::MAX milliseconds

## [0.10.5] - 2020-06-18

### Changed
- fsyncing on publish confirm as soon as possible (previously only every 200ms)

### Fixed
- Crystal 0.35 compatibility
- Decrease segment position counter on queue delete

### Added
- robots.txt file to disallow crawling
- Possibility to cancel consumers from the UI

## [0.10.4] - 2020-06-09

### Added
- Display message rates in queues list in mgmt UI
- Config for changing endianess of on-disk data

### Fixed
- Only use `copy_file_range` when the glibc version supports it

## [0.10.3] - 2020-05-26

### Changed
- Faster segment GC
- Using zero-copy syscall `copy_file_range` between temp file and segment

### Fixed
- XSS in shovel UI
- Echo incoming heartbeat if we didn't send one recently

## [0.10.2] - 2020-05-01

### Changed
- The way to find holes in segment files to punch is much improved

## [0.10.1] - 2020-04-26

### Fixed
- Updated amq-protocol.cr, fixes skipping headers in partial GC

## [0.10.0] - 2020-04-26

### Changed
- Partial GC of segments by hole punching, disk usage is much lower now
- Lower default file read buffer to 16KB
- Removed some HTTP protection measures that aren't applicable
- If writing to the definitions.amqp file fails abort the whole application
- Periodically write to the lock file to detect lost lock

### Added
- Does now notify systemd when properly started

### Fixed
- All settings in the file config is respected now, not just a few

## [0.9.16] - 2020-04-13

### Fixed
- Keep reference to the opened .lock file until shutdown so it's not GCed
- Publish stats correct

## [0.9.15] - 2020-04-09

### Fixed
- Crystal 0.34.x compability
- Rescue error while trying to parse the PROXY protocol
- Header exchange binding without arguments matches empty message headers
- avalanchemqperf throughput now declares queues smarter

### Changed
- Decreased default file buffer size to 128kb
- Increased default socket buffer size to 16kb
- Log a warning if not built in release mode
- Print out data dir on start up
- Fiber.yield only in IO.copy, it balances clients better
- Don't log warnings if msg body can't be fully read from disconnected client

### Added
- .lock file in the data dir that's locked on start so that only one instance can use it at a time
- Protection against common HTTP attacks
- Nodes page with details on node usage
- Snapcraft configuration file
- Dockerfile

## [0.9.14] - 2020-03-25

### Fixed
- 'Move messages' in the UI now uses current user's credentials for the shovel
- Don't retry shovel if it's stopped
- /api/nodes fd_limit is now correct again
- Flush segment before resetting position if message write fails

## [0.9.13] - 2020-03-24

### Fixed
- Message bodies larger than frame_max size weren't correctly written to disk
- If a segment is missing when reading metadata we stopped there, now we loop until we find a message we can read
- Ignore all errors when closing client socket
- If a body can't be read from socket, reset position on segment and continue (don't rotate segment)

### Changed
- File descriptor limit is automatically maximized on start
- Only keep one segment per queue open at any one time
- 'Lost connection' is now only reported on debug level

### Added
- In each vhost dir and each queue dir are now a file outputted with the plain text name

## [0.9.12] - 2020-03-22

### Fixed
- FD leak in queues. Segments weren't closed until all queue missed reference to it.

## [0.9.11] - 2020-03-20

### Added
- More stats in the /api/nodes endpoint, including CPU, diskspace, IOPS etc
- Structure the output of USR1 signal better

### Fixed
- Bug when publishing to a queue that has reach its max-length
- Memory leak of queue lookup caches, the caches are now moved to the Channel

### Changed
- No logging of GC of segments, except in debug mode
- Message bodies smaller than a frame is written directly to disk
- Message bodies spread out over several frames to temporarily written to disk

## [0.9.10] - 2020-03-12

### Changed
- Timestamps are only accurate to the second, giving a 10% publish speed boost
- Default value for queue_max_ack is doubled to 2M messages, decreasing number of index compactions
- Don't maintaine perfect round robin to consumers in high throughput scenarios, increasing delivery rate dramatically

## [0.9.9] - 2020-03-11

### Fixed
- Move messages between queues is now working again
- Connection view is working again
- Memory leak in queue's segment position cache fixed

### Changed
- Shovel now try to declare the source queue as passive, non-passive if that fails
- Using (faster) unchecked mutexes whenever possible

## [0.9.8] - 2020-03-09

### Fixed
- Applying TTL policy to existing queues works as expected
- Removing a policy removes its effect on existing queues
- Auto-delete queues are deleted in vhost as well when closed

### Added
- Report accumulated message stats in the /api/overview endpoint

### Changed
- Default to 0 heartbeat
- Use socket timeout for heartbeat sending, instead of a looping fiber
- Round message stat metrics to 1 decimal

## [0.9.7] - 2020-03-06

### Fixed
- Exchange to exchange bindings are restored at boot correctly

### Changed
- Rewritten garbage collection of message segments
- Rewritten mechanism for message index compaction
- Faster restore of queue message indexes
- Both transient and persistent messages are persisted between restarts

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
