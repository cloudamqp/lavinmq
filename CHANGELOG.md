# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Memory leak in StreamConsumer [#1266](https://github.com/cloudamqp/lavinmq/pull/1266)

## [2.4.3] - 2025-09-11

### Fixed
- Broken javascript dependency [#1247](https://github.com/cloudamqp/lavinmq/pull/1247)
- Queue `unacked_bytesize` return `unacked_count` [#1250](https://github.com/cloudamqp/lavinmq/pull/1250)
- Fix bug where only one consumer got notified about new messages in a stream [#1253](https://github.com/cloudamqp/lavinmq/pull/1253
- Fix bug where a stream consumer's deliver loop could loop for infinity [#1254](https://github.com/cloudamqp/lavinmq/pull/1254)

## [2.4.2] - 2025-09-10

### Fixed
- Memory leak in `MQTT::Consumer` [#1242](https://github.com/cloudamqp/lavinmq/pull/1242)

## [2.4.1] - 2025-07-21

### Fixed
- MQTT 3.1 client support
- Allow publishing of MQTT messages larger than 64KiB

## [2.4.0] - 2025-06-11

### Added
- Parts of LavinMQ is now multi threaded, many structures are now thread safe
- Streams - Fltering on any header [#1053](https://github.com/cloudamqp/lavinmq/pull/1053)
- Purgeing a queue without unacked messages is now instant [#1083](https://github.com/cloudamqp/lavinmq/pull/1083)
- Show active arguments on queues & exchanges [#1072](https://github.com/cloudamqp/lavinmq/pull/1072)
- Install instructions for Archlinux [#1001](https://github.com/cloudamqp/lavinmq/pull/1001)

### Changed
- Requires Crystal 1.16 and `-Dpreview_mt -Dexecution_context` to run
- Run etcd lease keepalive in a separate thread
- `lavinmqperf throughput` is multithreaded
- Drop global ACL caches and keep one write cache only per Client
- Remove ws.html and ws-mqtt.html

### Fixed
- Allow requeuing of MQTT messages when delivery fails [#1081](https://github.com/cloudamqp/lavinmq/pull/1081)
- Fixed routing_key or exchange with length 255 causing an ArithmeticOverflow [#1094](https://github.com/cloudamqp/lavinmq/pull/1094)
- Append to x-received-from instead of replacing [#1084](https://github.com/cloudamqp/lavinmq/pull/1084)
- Don't allow invalid binding arguments to ConsitentHashExchange [#1089](https://github.com/cloudamqp/lavinmq/pull/1089)
- Drop msg on requeue if above delivery_limit [#996](https://github.com/cloudamqp/lavinmq/pull/996)
- Fixed clustering leader election timing issue [#1138](https://github.com/cloudamqp/lavinmq/pull/1138)

## [2.3.0] - 2025-04-17

### Added
- MQTT websocket support [#1007](https://github.com/cloudamqp/lavinmq/pull/1007)
- Ability to change channel prefetch in UI/API [#1033](https://github.com/cloudamqp/lavinmq/pull/1033)
- Add Prometheus metrics for `global_message_*` counters [#1010](https://github.com/cloudamqp/lavinmq/pull/1010)
- Add filtering for bindings on exchange [#1032](https://github.com/cloudamqp/lavinmq/pull/1032)
- Make it possible to filter on connection_name and user from /connections [#1031](https://github.com/cloudamqp/lavinmq/pull/1031)
- Log total startup time [#1056](https://github.com/cloudamqp/lavinmq/pull/1056)

### Fixed
- Multiple nodes could generate and set clustering secret, causing the leader to use another secret than the followers. [#998](https://github.com/cloudamqp/lavinmq/pull/998)
- A policy with delivery-limit is now properly applied to a queue if the value is lower than the existing argument. [#1000](https://github.com/cloudamqp/lavinmq/pull/1000)
- Fix cluster ID and advertised URI collision handling, preventing confusing behavior when multiple nodes have the same identity [#1023](https://github.com/cloudamqp/lavinmq/pull/1023)
- Close MQTT client socket and exit deliver loop on errors [#1043](https://github.com/cloudamqp/lavinmq/pull/1043)
- Don't federate internal exchanges [#1058](https://github.com/cloudamqp/lavinmq/pull/1058)
- Stop existing federation links when applying a new policy [#1059](https://github.com/cloudamqp/lavinmq/pull/1059)

### Changed
- Cleaner CLI output with separators [#1018](https://github.com/cloudamqp/lavinmq/pull/1018)
- Default limit of 128 items in deduplication cache [#1019](https://github.com/cloudamqp/lavinmq/pull/1019)
- Messages in stream queues now support multiple filter values [#1022](https://github.com/cloudamqp/lavinmq/pull/1022)
- Filtering on stream queues now requires all filters on a consumer to match [#1022](https://github.com/cloudamqp/lavinmq/pull/1022)
- Make it possible to filter on connection_name and user from /connections [#1031](https://github.com/cloudamqp/lavinmq/pull/1031)
- Add BasicAuth section to HTTP API docs [#1030](https://github.com/cloudamqp/lavinmq/pull/1030)
- Command line arguments for mqtt_bind [#992](https://github.com/cloudamqp/lavinmq/pull/992)
- Show MQTT client keepalive in management UI [#989](https://github.com/cloudamqp/lavinmq/pull/989)
- Count MQTT messages in vhosts message stats [#988](https://github.com/cloudamqp/lavinmq/pull/988)
- Build RPM packages for Fedora 42 [391f79be](https://github.com/cloudamqp/lavinmq/commit/391f79be)
- README overhaul with improved structure and visual elements [#1052](https://github.com/cloudamqp/lavinmq/pull/1052)
- Don't build lavinmqperf with MT

## [2.2.0] - 2025-03-13

### Added
- Implemented support for MQTT 3.1.1. [#766](https://github.com/cloudamqp/lavinmq/pull/766)
- Introduced message deduplication on exchanges and queues. [#854](https://github.com/cloudamqp/lavinmq/pull/854)
- Added filtering capabilities for streams. [#893](https://github.com/cloudamqp/lavinmq/pull/893)
- Added garbage collection (GC) metrics to Prometheus endpoints. [#954](https://github.com/cloudamqp/lavinmq/pull/954)
- Incorporated message rates into the exchanges view. [#929](https://github.com/cloudamqp/lavinmq/pull/929)
- Displayed disk space usage in the `/queues` endpoint. [#930](https://github.com/cloudamqp/lavinmq/pull/930)
- Introduced configurability for `default_user` and `default_password`. These can now be set in the configuration file, as command-line arguments, or as environment variables. [#919](https://github.com/cloudamqp/lavinmq/pull/919)
- Added the `/api/auth/hash_password` endpoint and the `lavinmqctl hash_password` command to facilitate password hash generation. [#919](https://github.com/cloudamqp/lavinmq/pull/919)
- Enhanced `lavinmqperf` with a new queue-count test scenario. [#955](https://github.com/cloudamqp/lavinmq/pull/955)
- Added tooltips for the details table to improve user experience. [#925](https://github.com/cloudamqp/lavinmq/pull/925)

### Changed
- Ensured that `channel_max` negotiation is respected, treating a value of 0 as unlimited.
- Implemented logging and re-raising of errors if loading unexpectedly fails. [#933](https://github.com/cloudamqp/lavinmq/pull/933)
- Included priority settings in the journald log format. [#950](https://github.com/cloudamqp/lavinmq/pull/950)
- Deprecated `guest_only_loopback` in favor of `default_user_only_loopback`; `guest_only_loopback` will be removed in the next major release. [#919](https://github.com/cloudamqp/lavinmq/pull/919)

### Fixed
- Queues now expire `TTL` milliseconds after the last consumer disconnects. [#924](https://github.com/cloudamqp/lavinmq/pull/924)
- Prevented LavinMQ from freezing when closing consumers and channels. [#947](https://github.com/cloudamqp/lavinmq/pull/947)
- `x-delivery-count` is now excluded from the initial delivery of a message; the count accurately reflects the number of delivery attempts prior to the current delivery. [#977](https://github.com/cloudamqp/lavinmq/pull/977)
- Resolved an issue where follower nodes unnecessarily resynchronized identical files during full syncs due to mismatched hash calculations. [#963](https://github.com/cloudamqp/lavinmq/pull/963)
- Validated that the `expiration` parameter is a non-negative number when publishing via the HTTP API. [#969](https://github.com/cloudamqp/lavinmq/pull/969)
- Implemented handling for missing files during follower synchronization. [#968](https://github.com/cloudamqp/lavinmq/pull/968)

## [2.2.0-rc.1] - 2025-02-21

See [v2.2.0-rc.1 Release Notes](https://github.com/cloudamqp/lavinmq/releases/tag/v2.2.0-rc.1) for changes in this pre-release

## [2.1.0] - 2025-01-16

### Changed

- New UI for management interface [#821](https://github.com/cloudamqp/lavinmq/pull/821)
- Use sent/received bytes instead of messages to trigger when other tasks can run [#863](https://github.com/cloudamqp/lavinmq/pull/863)
- Spread out stream queues GC-loop over time [#876](https://github.com/cloudamqp/lavinmq/pull/876)
- Don't unmap files on USR2 or when last consumer disconnects [#876](https://github.com/cloudamqp/lavinmq/pull/876)
- Unmap files when they are no longer in use [#876](https://github.com/cloudamqp/lavinmq/pull/876)
- Store non-durable queues in a separate dir that is removed on startup and shutdown [#900](https://github.com/cloudamqp/lavinmq/pull/900)

### Fixed

- Queues will no longer be closed if file size is incorrect. Fixes [#669](https://github.com/cloudamqp/lavinmq/issues/669)
- Dont redeclare exchange in java client test [#860](https://github.com/cloudamqp/lavinmq/pull/860)
- Removed duplicate metric rss_bytes [#881](https://github.com/cloudamqp/lavinmq/pull/881)
- Release leadership on graceful shutdown [#871](https://github.com/cloudamqp/lavinmq/pull/871)
- Rescue more exceptions while reading msg store on startup [#865](https://github.com/cloudamqp/lavinmq/pull/865)
- Crystal 1.15 support [#905](https://github.com/cloudamqp/lavinmq/pull/905)
- lavinmqctl now handles pagination of large result sets [#904](https://github.com/cloudamqp/lavinmq/pull/904)
- Make clustering more reliable [#879](https://github.com/cloudamqp/lavinmq/pull/879), [#909](https://github.com/cloudamqp/lavinmq/pull/909), [#906](https://github.com/cloudamqp/lavinmq/pull/906)
- Strip newlines from logs [#896](https://github.com/cloudamqp/lavinmq/pull/896)

### Added

- Added some logging for followers [#885](https://github.com/cloudamqp/lavinmq/pull/885)

## [2.0.2] - 2024-11-25

### Fixed

- Don't raise FileNotFound when deleting already deleted file [#849](https://github.com/cloudamqp/lavinmq/pull/849)
- Increase default etcd lease TTL to 10s and run keepalive earlier [#847](https://github.com/cloudamqp/lavinmq/pull/847)
- Don't remove consumers from queue before all data is sent to client. It may cause a segmentation fault because a unmapped message segment is accessed. [#850](https://github.com/cloudamqp/lavinmq/pull/850)

### Changed

- Exit with status 1 if lost leadership so that LavinMQ is restarted by systemd [#846](https://github.com/cloudamqp/lavinmq/pull/846)

## [2.0.1] - 2024-11-13

### Fixed

- etcd lease will no longer expire during slow startup [#834](https://github.com/cloudamqp/lavinmq/pull/834)
- Leader node is no longer unresponsive while sending file_list/files to new followers [#838](https://github.com/cloudamqp/lavinmq/pull/838)
- Leader node now waits for all data to be acked to all followers before shutting down on graceful shutdown [#835](https://github.com/cloudamqp/lavinmq/pull/835)
- Improve broker initiated client disconnects [#816](https://github.com/cloudamqp/lavinmq/pull/816)
- Fixed sorting on unacked messages [#836](https://github.com/cloudamqp/lavinmq/pull/836)

### Changed

- Remove the 'reset vhost' feature [#822](https://github.com/cloudamqp/lavinmq/pull/822)
- Build with latest crystal version [#841](https://github.com/cloudamqp/lavinmq/pull/841)
- Added metadata when logging connection handshake [#826](https://github.com/cloudamqp/lavinmq/pull/826)

### Added

- Added some indexing for streams, greatly increasing performance when looking up by offset or timestamp [#817](https://github.com/cloudamqp/lavinmq/pull/817)
- Added buildstep for 'make rpm' [#840](https://github.com/cloudamqp/lavinmq/pull/840)

## [2.0.0] - 2024-10-31

With the release of 2.0.0 we introduce High Availablility for LavinMQ in the form of clustering. With clustering, LavinMQ replicates data between nodes with our own replication protocol, and uses etcd for leader election. See [this post](https://lavinmq.com/blog/lavinmq-high-availability) in the LavinMQ blog or the [readme](https://github.com/cloudamqp/lavinmq?tab=readme-ov-file#clustering) for more information about clustering.

### Added

- Full HA clustering support, uses etcd for leader election and metadata, and a replication protocol between nodes.
- Added cluster_status to lavinmqctl [#787](https://github.com/cloudamqp/lavinmq/pull/787)
- Added deliver_get to message_stats [#793](https://github.com/cloudamqp/lavinmq/pull/793)
- Added a configurable default consumer prefetch value for all consumers. Defaults to 65535. [#813](https://github.com/cloudamqp/lavinmq/pull/813)
- Output format (text/json) can now be selected when running lavinmqctl cmds [#790](https://github.com/cloudamqp/lavinmq/pull/790)
- Clustering clients will now listen on the UNIX sockets if configured
- Clustering server will accept PROXY protocol V2 headers from followers

### Fixed

- Shovels' batching of acks caused a lot of unacked messages in source broker [#777](https://github.com/cloudamqp/lavinmq/pull/777)
- Shovel AMQP source didn't reconnect on network failures (partially fixed in prev release) [#758](https://github.com/cloudamqp/lavinmq/pull/758)
- Running lavinmqctl commands on a follower node now displays an error and exits with code 2 [#785](https://github.com/cloudamqp/lavinmq/pull/785)
- Federation queue links now reconnects if the upstream disconnects [#788](https://github.com/cloudamqp/lavinmq/pull/788)
- Proxying from followers is now more resilient [#812](https://github.com/cloudamqp/lavinmq/pull/812)
- Sorting on consumer count at channels page
- Log if user tries to declare a queue with an unknown queue type [#792](https://github.com/cloudamqp/lavinmq/pull/792)
- Force close AMQP connections if a protocol error occurs
- Remove min_isr setting [#789](https://github.com/cloudamqp/lavinmq/pull/789)
- Wait for followers to synchronize on shutdown of leader
- Make proxied UNIX sockets in followers RW for all
- SystemD notify ready in cluster mode when lader is found
- Etcd actions are retried, etcd can now be restarted without issues
- Clustering secret is monitored if not available yet when becoming a replication follower

### Changed

- Updated RabbitMQ HTTP API Go client test to use a patch-file for LavinMQ compatibility [#778](https://github.com/cloudamqp/lavinmq/pull/778)
- Renames clustering_max_lag -> clustering_max_unsynced_actions to clarify that it is measured in number of actions [#810](https://github.com/cloudamqp/lavinmq/pull/810)
- Renames lag -> lag_in_bytes to clarify that lag is measured in bytes [#810](https://github.com/cloudamqp/lavinmq/pull/810)
- Suggests etcd 3.4.0 as min version [#815](https://github.com/cloudamqp/lavinmq/pull/815)
- Changed logging to provide more information about which queue is being handled [#809](https://github.com/cloudamqp/lavinmq/pull/809)
- Publishing messages are now handled fully by exchanges [#786](https://github.com/cloudamqp/lavinmq/pull/786)
- Logging is now handled more uniformly throghout LavinMQ [#800](https://github.com/cloudamqp/lavinmq/pull/800)
- Don't log handled exceptions as error with backtrace [#776](https://github.com/cloudamqp/lavinmq/pull/776)
- Don't dynamically fetch active etcd endpoints

## [2.0.0-rc.5] - 2024-10-28

See <https://github.com/cloudamqp/lavinmq/releases/tag/v2.0.0-rc.5> for changes in this pre-release

## [1.3.1] - 2024-08-29

### Fixed

- Don't shovel messages after initial queue length [#734](https://github.com/cloudamqp/lavinmq/pull/734)
  - Shovels that were set up to stop after the initial queue length will now stop. For high ingress queues a race condition could trigger these shovels to send more messages than the initial queue length.
- Memory leak in Crystal's Hash implementation fixed [#14862](https://github.com/crystal-lang/crystal/pull/14862)
  - This release is using Crystal `1.13.2`
- Bindings are now sorted properly in the web interface [#726](https://github.com/cloudamqp/lavinmq/pull/726)
- Shovel AMQP source didn't reconnect on network failures [#758](https://github.com/cloudamqp/lavinmq/pull/758)
  - Update `amqp-client.cr` to get a necessary fix for reconnecting shovel sources
- Dead-lettering loop when publishing to a delayed exchange's internal queue was fixed [#748](https://github.com/cloudamqp/lavinmq/pull/748)
- Exchange federation tried to bind to the upstream's default exchange [#749](https://github.com/cloudamqp/lavinmq/pull/749)
- Shovel ack all unacked messages on stop [#755](https://github.com/cloudamqp/lavinmq/pull/755)
- Prevent a queue that is overflowing from consuming too many resources [#725](https://github.com/cloudamqp/lavinmq/pull/725)

## [2.0.0-rc.4] - 2024-08-21

See <https://github.com/cloudamqp/lavinmq/releases/tag/v2.0.0-rc.4> for changes in this pre-release

## [1.3.0] - 2024-07-17

### Removed

- Removed old replication in anticipation of coming clustering.

### Changed

- Build with Crystal 1.13.1
- Deb packages now includes all debug symbols, for useful stacktraces
- Replaced HTTP router with an internal one. Now LavinMQ uses 0 external libraries.
- Specs are more reliable when a new server is started for each spec
- LavinMQ never sets TLS ciphers by it self, custom ciphers have to be configured via the config

### Fixed

- HTTP API: Regression where an (empty) body was required to PUT a new vhost
- lavinmqctl didn't recognize 201/204 response codes from set_permissions, set_user_tags and add_vhost
- Queues will no longer be closed if file size is incorrect. Fixes [#669](https://github.com/cloudamqp/lavinmq/issues/669)
- Improved logging
- Won't choke on empty message files on boot [#685](https://github.com/cloudamqp/lavinmq/issues/685)
- Won't choke on somewhat too large message files on boot [#671](https://github.com/cloudamqp/lavinmq/issues/671)

### Added

- Can view which messages are unacked in each queue (HTTP and UI) [#712](https://github.com/cloudamqp/lavinmq/pull/712)
- Tags and descriptions on VHosts
- Can pass an array of URLs to Shovel

## [2.0.0-rc.3] - 2024-07-12

See <https://github.com/cloudamqp/lavinmq/releases/tag/v2.0.0-rc.3> for changes in this pre-release

## [2.0.0-rc.2] - 2024-07-05

See <https://github.com/cloudamqp/lavinmq/releases/tag/v2.0.0-rc.2> for changes in this pre-release

## [2.0.0-rc.1] - 2024-06-25

See <https://github.com/cloudamqp/lavinmq/releases/tag/v2.0.0-rc.1> for changes in this pre-release

## [1.2.14] - 2024-06-15

### Fixed

- Exporting definitions broke delayed exchanges [#699](https://github.com/cloudamqp/lavinmq/pull/699)
- lavinmqctl: escape symbols in parameters (eg. can now create vhosts with / in the name) [#699](https://github.com/cloudamqp/lavinmq/pull/696)

## [1.2.13] - 2024-06-14

### Fixed

- Some prometheus metrics typing was mixed up, counter where it should be gauge
- Unmap memory mapped files on finalize, could cause segfaults in replication when files already deleted tried to be replicated
- Bug fix where followers who were synchronizing could miss some updates

### Changed

- Auto reconnect `lavinmqperf throughput` on disconnect
- Render HTTP API docs using Stoplight Elements

### Added

- HTTP API: /api/connections/:user, lists connections by a specific user

## [1.2.12] - 2024-05-24

### Changed

- Follower lag is now based on bytes written to action queue instead of socket [#652](https://github.com/cloudamqp/lavinmq/pull/652)
- Change how delayed exchanges are exported with definitions [#663](https://github.com/cloudamqp/lavinmq/pull/663)
- Force 4096 bytes frame_max for WebSocket connections [#681](https://github.com/cloudamqp/lavinmq/pull/681)

### Fixed

- Federated queues now only transfers messages if there is a consumer on the downstream queue [#637](https://github.com/cloudamqp/lavinmq/pull/637)
- Handle proxied WebSocket connections [#680](https://github.com/cloudamqp/lavinmq/pull/680)

## [1.2.11] - 2024-04-26

### Fixed

- Empty ack files created for all segments [#658](https://github.com/cloudamqp/lavinmq/pull/658)
- UI: Set proper width (colspan) for pagination cell  [#662](https://github.com/cloudamqp/lavinmq/pull/662)
- Provide better information about connections LavinMQ initiates [#613](https://github.com/cloudamqp/lavinmq/pull/613)
- Bugfix: Make sure shovels reconnect after destination disconnects [#667](https://github.com/cloudamqp/lavinmq/pull/667)

### Changed

- LavinMQ now waits for any followers to be in sync before shutting down [#645](https://github.com/cloudamqp/lavinmq/pull/645)
- UI: Creating queues and exchanges in the UI now defaults to durable [#656](https://github.com/cloudamqp/lavinmq/pull/656)
- Rename config.ini -> lavinmq.ini [#664](https://github.com/cloudamqp/lavinmq/pull/664)

### Added

- Replication lag is now exported in metrics [#646](https://github.com/cloudamqp/lavinmq/pull/646)

## [1.2.10] - 2024-03-25

### Fixed

- Don't use shovel name as consumer tag [#634](https://github.com/cloudamqp/lavinmq/pull/634)
- Keep the same vhost selected when creating queues [#629](https://github.com/cloudamqp/lavinmq/pull/629)
- Make sure name of Queues, Exchanges & Vhosts is not longer than 255 characters [#631](https://github.com/cloudamqp/lavinmq/pull/631)
- Add nilguard for matching x-delayed-type argument [#639](https://github.com/cloudamqp/lavinmq/pull/639)

### Changed

- Use system assigned ports in specs [#640](https://github.com/cloudamqp/lavinmq/pull/640)
- Let MFile raise ClosedError instead of IO::Error [#642](https://github.com/cloudamqp/lavinmq/pull/642)

### Added

- Improved logging during definitions loading [#621](https://github.com/cloudamqp/lavinmq/pull/621)

## [1.2.9] - 2024-02-05

### Fixed

- LavinMQ now calls Systemd.notify_ready when started as follower [#626](https://github.com/cloudamqp/lavinmq/pull/626)

## [1.2.8] - 2024-01-10

### Fixed

- A bug causing faulty definition frames for delayed exchanges, preventing LavinMQ from starting [#620](https://github.com/cloudamqp/lavinmq/pull/620)
- Better table views for Queues, where the name isn't cut of
- Persist table sorting between page loads
- Don't include permissions in users array in definitions json export, as that was incompatible with other brokers
- Don't mmap a segment file to just verify the schema version (uses pread instead)

### Changed

- Build binaries and container images using Crystal 1.11.0
- Don't allow clients open an already open channel
- Use DirectDispatcher for logging and add more logging to startup procedure [#619](https://github.com/cloudamqp/lavinmq/pull/619)

### Added

- Support for Consumer timeouts, default none.

## [1.2.7] - 2023-12-12

- Version 1.2.6 may not include the bumped version of lavinmq, so instead use version 1.2.7

## [1.2.6] - 2023-12-12

### Fixed

- Don't update user's password hash if given password is the same as current [#586](https://github.com/cloudamqp/lavinmq/pull/586)
- Remove old segments in the background for stream queues [#608](https://github.com/cloudamqp/lavinmq/pull/608)

### Changed

- New amq-protocol.cr version 1.1.12
- Add package build for Debian 12 (bookworm) [#597](https://github.com/cloudamqp/lavinmq/pull/597)
- Do not update permissions if they are the same [#609](https://github.com/cloudamqp/lavinmq/pull/609)
- Do not update password hash if given current password [#586](https://github.com/cloudamqp/lavinmq/pull/586)

### Added

- Add playwright for frontend specs [#560](https://github.com/cloudamqp/lavinmq/pull/560)

## [1.2.5] - 2023-11-06

### Added

- Consumer arguments support for shovels [#578](https://github.com/cloudamqp/lavinmq/pull/578)

### Fixed

- A bug in delay exchanges caused messages to be routed to x-dead-letter-exchange instead of bound queues. It also ruined any dead lettering headers.
- A bug that prevented headers exchange to match on table in message headers.
- Purging a queue with a lot of messages blocked LavinMQ from other operations.
- Message timestamps not being updated when dead lettered breaking TTL.

### Changed

- Definitions uploads can now be JSON body [#580](https://github.com/cloudamqp/lavinmq/pull/580)

## [1.2.4] - 2023-09-26

### Fixed

- A bug in amq-protocol caused corrupt headers when a message was dead-lettered multiple times [amq-protocol/#14](https://github.com/cloudamqp/amq-protocol.cr/pull/14)

## [1.2.3] - 2023-09-12

### Fixed

- A bug in amq-protocol caused lost headers when dead-lettering [amq-protocol/#12](https://github.com/cloudamqp/amq-protocol.cr/pull/12)
- Block creation of queues and users when disk is close to full to prevent disk from becoming full [#567](https://github.com/cloudamqp/lavinmq/pull/567)
- Compacting definitions during runtime to avoid the definitions file to grow endlessly when churning bindings/queues (#571)
- Only store bind/unbind defintions if not already added, decreases definitions file growth
- Unmap segments in stream queues regularly
- Unmap segments when all consumers from a queue has disconnected

## [1.2.2] - 2023-08-22

### Added

- Make it possible to declare temp queues in lavinmqperf connection-count

### Fixed

- Delete fully ACK'd segments when opening new segments [#565](https://github.com/cloudamqp/lavinmq/pull/565)
- Don't send internal error messages to clients
- Remove queues from ACL cache on deletion
- Use federated queue's name if queue name is empty [#562](https://github.com/cloudamqp/lavinmq/pull/562)

### Changed

- Refactoring
- Use amq-protocol v1.1.8, refactor header handling
- Optimize SegmentPosition creation

## [1.2.1] - 2023-08-09

### Fixed

- Aggressively unmapping message store segments could result in seg faults when consumers are slow.

## [1.2.0] - 2023-08-08

### Added

- Stream queues, queues that can be consumed multiple times, messages (segments) are only deleted by a retention policy

### Fixed

- Queue message segments where opened with the wrong permissions to be able to be truncated if needed

## [1.1.7] - 2023-08-05

### Fixed

- Acked messages reappeared after server reboot, ack files were accidently truncated
- Deliver msgs to consumer even if queue ttl is 0
- Allow multiple formats (array, string etc) of tags in definitions.json
- Acking delivery tag 0 (with multiple=true) when channel as no unacked messages will not result in an error

### Added

- Signal USR2 will unmap all mmap:ed files (in addition to force a memory garbace collection)
- Building packages for enterprise linux 9 (EL9, redhat 9, centos stream 9, rocky linux 9 etc)

### Changed

- Removed "Message size snapshot", UI and API, which has been non functioning for a long time
- Crystal 1.9.2

## [1.1.6] - 2023-07-20

### Added

- Replication support, followers can connect and stream data from a leader in real time, enabling hot-standby use-cases
- Config variables for free_disk_min and free_disk_warn
- Support for adding custom properties in `lavinmqperf throughput --properties '{"headers": {"a": 1}}'`
- Support for random bodies in `lavinmqperf --random-bodies`

### Changed

- Temporary message files (per channel) are not truncated on publish, so performance for transactions are up 10x, and publishing messages larger than frame_max too
- Modifying defintions (declaring/deleting queues/exchanges etc) is now synchrous and thread safe.
- Message segments aren't ftruncated until server is cloded, as it's a slow operation
- SEGV signal always generate coredumps now (no custom signal handler for SEGV)
- Boehm GC upgraded to version 8.2.4 (from 8.2.2)

### Fixed

- Messages larger than frame_max, routed to multiple queues, were only written to the first queue, and corrupted the others
- Message segments are aggressively unmapped to decrease memory usage
- Exclusive queues are no longer included in definitions export
- UI error handling simplified

## [1.1.5] - 2023-06-23

### Fixed

- Prevent memory leaks from consumer fibers which are waiting for empty queues etc.

## [1.1.4] - 2023-06-22

### Changed

- fsync on transaction commit, but not on publish confirm

### Fixed

- Don't allow creating policies with wrongly typed defintions
- Javascript bugs in the UI
- Allow importing definitions where entities lacks the vhost property
- Export vhost parameters when exporting a single vhost
- Redirect to login page when cookie expires in UI

### Added

- Single Active Consumer queue support
- Exponential backoff for shovel reconnects
- Consumer churn rate metrics
- Quicker boot time after crash
- UI view for consumers

## [1.1.3] - 2023-05-22

### Added

- Added churn_rates to /api/overview call [#517](https://github.com/cloudamqp/lavinmq/pull/517)

### Changed

- Improve RAM usage by only spawning queue_expiration loop when it's needed [#512](https://github.com/cloudamqp/lavinmq/pull/512)
- Better handling of JSON arguments in UI [#513](https://github.com/cloudamqp/lavinmq/pull/513)
- Load debug symbols by default

### Fixed

- Fixed some specs

## [1.1.2] - 2023-04-27

### Fixed

- Seg fault issues with Crystal 1.8.0 and LLVM 12, so this version just rebuild of 1.1.1 using crystal 1.8.1 and LLVM 15

## [1.1.1] - 2023-04-26

### Fixed

- Segmentation fault bug with Crystal 1.8 for exchange to exchange binding listing in API
- Consistent login handling in UI (use cookie, not basic auth)
- Shovel/federation clients now support negotiating frame_max with upstream server

### Changed

- Remove signal handler for SEGV (segmentation fault) in release mode so that a coredump is generated
- Don't include stat history when listing exchanges
- UI table refactoring so that multiple tables can have pagination for instance
- UI keeps trying to load data while server is offline
- Paused queues will no longer resume on restart, instead they will stay paused untill manually resumed [#511](https://github.com/cloudamqp/lavinmq/pull/511)

### Added

- `lavinmqctl definitions <datadir>` to generate definitions json from an existing data dir for emergency recovery

## [1.1.0] - 2023-04-15

### Added

- Only mmap message store files on-demand, decreasing memory usage for arbitrary long queues to 0
- CSP rules in the UI, forbids inline js etc.
- Support `?columns=` query for all API endpoints
- The HTTP API now supports cookie authentication, in addition to basic auth

### Changed

- Precompress static assets (inflate on-demand if client don't accept deflate)
- Don't include historic message stats when listing queues via the API
- New favicon (that works both in dark and light mode)
- Return 403 rather than 401 for access refused in /api (401 only if unauthenticated)
- Cleaning up JS in UI

### Fixed

- Error message in UI when lacking access to Logs/Vhosts/Users/shovels/federation
- Autocomplete queues/vhosts is now correct in the UI (selecting only from the vhost in question)
- Deliveries of messages larger than socket_buffer_size (16KB) on x86_64 fixed in Crystal 1.8.0 due to faulty LibC bindings
- Increased responsiveness in cases with very fast consumers
- UI: Consumers listing on Queue page
- UI: Users with vhost access can also list all connections/channels/consumers in that vhost

## [1.0.1] - 2023-04-06

### Fixed

- Require `administrator` tag to show logs in UI/API
- VHost dropdown menu is now sorted by name

### Changed

- Optimized images in UI (replace jpg/png with webp, svg and css gradients)
- Calculate etags for static resources at compile time
- Use weak etags so that responses can be compressed without dropping the etag
- no-cache for views and static resources, the browser will still cache them, but query the browser if there are newer versions
- Use the hash part of the URL as arguments to /queue and other views, so that the browser reuse the same cached HTML

## [1.0.0] - 2023-03-31

### Added

- Log exchange [#473](https://github.com/cloudamqp/lavinmq/pull/473)

### Fixed

- A lot of management UI fixes
- Disable proxy buffering of /api/livelog

## [1.0.0-beta.13] - 2023-03-30

### Changed

- Use /tmp/lavinmqctl.sock as unix socket path for lavinmqctl on all platforms
- Make HTTP unix sockets world writeable, authentication is done on the protocol level
- Keep debug symbols in debian packages, no separate dbgsym package required

### Fixed

- JS error when checking authentication
- Optimized basic auth check

## [1.0.0-beta.12] - 2023-03-22

### Fixed

- Add lavinmq user when installing Debian/Ubuntu deb packages

## [1.0.0-beta.11] - 2023-03-18

### Changed

- Each queue now has its own message store, simplifying a lot of code (net -1500 LOC), but sacrificing fanout performance and persistent exchange support (to be replaced with stream queues). Single queue performance is better (as only one write is needed per message) and no message GC is needed, disk space usage is better.
- Backup data dir before migration (remember to delete when successful)
- pcre2 as regex engine

### Fixed

- No flash of layout when not logged in to the UI
- Delete temporary messages on transaction rollback

## [1.0.0-beta.10] - 2023-03-10

### Added

- Log view in the UI, uses SSE to live stream any logs from the server

### Changed

- Use relative links in the UI, so that the mgmt interface can be mapped at any path
- HTML views are now templated using ECR
- Own implementation of embedding UI assets in executable

### Fixed

- Fixed potential message store corruption bug, if client disconnected while publishing (and body frame split over multiple TCP packets)
- Include libsystemd-pthread in container image so that lavinmqperf works again
- Builds are reproducible

## [1.0.0-beta.9] - 2023-02-22

### Changed

- Side menu in manager is now more responsive to smaller browser windows [#437](https://github.com/cloudamqp/lavinmq/pull/437)
- Updated Chart.js version form v2.9.4 to v4.0.1 [#426](https://github.com/cloudamqp/lavinmq/pull/426)
- Build lavinmqperf with multi threading
- `lavinmqperf connection-count` now uses multiple fibers to take use of multi threading
- Charts are updated faster with recent information
- UI updates for a better responsive experience [#463](https://github.com/cloudamqp/lavinmq/pull/463)

### Fixed

- Property tag shortcuts for JSON Payload fixed [#441](https://github.com/cloudamqp/lavinmq/pull/441)
- Warn if vm.max_map_count is low on boot
- Restore priority queues correctly [#436](https://github.com/cloudamqp/lavinmq/pull/436)
- Increased multi threading compability
- Can re-decalre a delayed-message exchange without failing [#450](https://github.com/cloudamqp/lavinmq/pull/450)

## [1.0.0-beta.8] - 2022-11-29

### Added

- Upload definitions to a specific vhost only
- Building RPM packages (for Fedora 37 currently, please request others if needed)
- Build DEB debuginfo/debugsource packages
- Include debug symbols in the container image binaries
- `install`/`uninstall` targets in `Makefile`

### Changed

- Allow administrators to change user's tags without also updating password
- Doesn't try to send publish confirms before publish (so messaged being delivered might still be unconfirmed)
- Understandable error message from lavinmqctl if lavinmq isn't running
- Lint openapi spec before building API docs

### Fixed

- Messages published in a transaction now gets their timestamp at the time of commit
- Impersonator tag now works as it should (allow messages to have another `user_id` property than the user publishing it)
- Unacked messages are only stored on the Channel level now, no double booking in the Queue, ~17% performance increase
- Close client socket as soon as Connection#CloseOk has been read
- Close channel if a new publish starts before another publish has finished
- Prevent double ack in a transaction
- Precondition error if trying to TxCommit/Rollback when TxSelect hasn't been issued
- Don't requeue uncommited ack:ed messages in Tx on BasicRecover

## [1.0.0-beta.7] - 2022-11-21

### Added

- Include the message timestamp and the TTL in the queue index for faster handling of expired message when the Queue has a message TTL
- Migrating index files to the new queue index format
- New RoughTime methods for faster time lookups
- Transaction support in lavinmqperf
- Spec stage/layer in `Dockerfile`: `docker build --target spec .`

### Fixed

- Queue/exchange arguments has priority over policies, but lowest value wins in case of numeric property
- Removed the use of StringPool for short strings, no performance impact and lower memory usage
- Proper error message if listing in UI includes error items
- Correct handling of zero TTL messages (expire on requeue)
- 6% publish rate increase due to caching of segment id (1.6M msgs/s locally)
- Include ca-certificates in container image so that lavinmqperf can connect to AMQPS servers with valid certs
- Unacked basic_get messages werent accounted for, which in rare cases could lead to message loss (unacked for a long time and then requeued)

### Changed

- Always fsync on transaction commit (if the transaction included persistent messages)
- Build container image and debian packages with crystal 1.6.2
- lavinmqctl list_connections connection_properties emulates the rabbitmqctl output format
- Command line arguments overrides config file settings
- lavinmqperf queue-churn uses random queue names for parallel runs

## [1.0.0-beta.6] - 2022-11-10

### Fixed

- Improved user handling ([#400](https://github.com/cloudamqp/lavinmq/pull/400))
- BasicRecover w/ requeue should return all unacked messages to the queues ([#398](https://github.com/cloudamqp/lavinmq/pull/398))
- Don't include basic_get messages when limiting global prefetch

### Added

- Support for AMQP transactions ([#403](https://github.com/cloudamqp/lavinmq/pull/403))
- support for set_permissions to lavinmqctl ([#397](https://github.com/cloudamqp/lavinmq/pull/397))
- Let consumers pull messages from queues ([#391](https://github.com/cloudamqp/lavinmq/pull/391)

### Changed

- Keep same Logger for server restarts ([#399](https://github.com/cloudamqp/lavinmq/pull/399))

## [1.0.0-beta.5] - 2022-11-02

### Changed

- Removed first/last timestamp from Queue view, as it as poorly implemented ([#392](https://github.com/cloudamqp/lavinmq/pull/392))

### Fixed

- Selects the selected vhost as default when creating queue ([#394](https://github.com/cloudamqp/lavinmq/pull/394))
- Add number of persistent messages in queue response ([#390](https://github.com/cloudamqp/lavinmq/pull/390))

## [1.0.0-beta.4] - 2022-10-18

### Changed

- Allow "policy maker"-user to export/import vhost definitions ([#379](https://github.com/cloudamqp/lavinmq/pull/379))

### Fixed

- Ensure Http tag does not give management access ([#381](https://github.com/cloudamqp/lavinmq/pull/381))
- If the server crashed, the index files could in many cases be corrupted and result in message loss ([#384](https://github.com/cloudamqp/lavinmq/pull/384))

## [1.0.0-beta.3] - 2022-09-06

Measure and log time it takes to collect metrics in stats_loop ([#371](https://github.com/cloudamqp/lavinmq/pull/371))

### Added

- Queue size metric details to present expensive queue metrics calculation ([#362](https://github.com/cloudamqp/lavinmq/pull/362))
- Support for vhost limits (max-queues and max-connections)
- Support for configuring max_message_size (default 128MB)
- The amount of ready and unacknowledged messages are shown in the nodes details ([#368](https://github.com/cloudamqp/lavinmq/pull/368))

## [1.0.0-beta.2] - 2022-06-30

### Fixed

- Fix endless loop over `expire_queue` on expired queues that still had consumers ([#363](https://github.com/cloudamqp/lavinmq/pull/363) and [#364](https://github.com/cloudamqp/lavinmq/pull/364))

## [1.0.0-beta.1] - 2022-05-17

### Added

- Queue multi action UI, purge or delete multiple queues ([#330](https://github.com/cloudamqp/lavinmq/pull/330))
- Shovel error feedback, ability to see why a shovel failed without consulting the logs ([#328](https://github.com/cloudamqp/lavinmq/pull/328))
- Can pass queue and consumer arguments in lavinmqperf
- Shovel configuration error feedback ([#328](https://github.com/cloudamqp/lavinmq/pull/328))
- Can reset vhosts ([#321](https://github.com/cloudamqp/lavinmq/pull/321))
- `lavinmqperf connection-count` for benchmarking many connections
- Read cgroup max memory, both for cgroup v1 and v2
- Show message rates per vhost (#355)
- Kubernetes example file (./extras/kubernetes.yaml)

### Fixed

- Fix potential corruption by race condition between multiple consumers
- Stop Queue if unrecoverable read error (i.e. corruption) ([#318](https://github.com/cloudamqp/lavinmq/pull/318))
- Handle both ackmode and ack_mode as param to "Get messages" ([#347](https://github.com/cloudamqp/lavinmq/pull/347))
- Number of messages was never used when puring from UI ([#337](https://github.com/cloudamqp/lavinmq/pull/337))
- Use the statically linked gc libary, which gives fewer GC pauses ([#337](https://github.com/cloudamqp/lavinmq/pull/337))
- Report which protocol each listener uses in /api/overview (#348)
- Limit API result sets, truncate instead of corrupt output (#343)
- Truncate spare index files on queue index restore for decreased memory usage on recovery

### Changed

- Renamed to LavinMQ
- Limit number of consumers listed on Queue page, improve load time if there's lots of consumers ([#327](https://github.com/cloudamqp/lavinmq/pull/327))
- Count consumers more efficiently ([#346](https://github.com/cloudamqp/lavinmq/pull/346))
- Stop building deb packages for Ubuntu 18.04
- New logging framework (#332)
- Improved websocket example page (./static/ws.html)
- Base container on crystal 1.4.1 and ubuntu 22.04 (openssl 3.0)

## [1.0.0-alpha.34] - 2022-01-19

### Added

- Option do configure tcp keepalive via config file
- Allow or block guest user using loopback address. ([#305](https://github.com/cloudamqp/lavinmq/pull/305))
- Reject and requeue is now default option when getting messages in UI. ([#307](https://github.com/cloudamqp/lavinmq/pull/307))
- Show hostname in header ([#308](https://github.com/cloudamqp/lavinmq/pull/308))
- Prometheus metrics
- Make it possible to reque Get messages last with the HTTP API
- Show server hostname in UI
- Displaying timestamp of first and last message in each Queue (HTTP API/UI)
- Option to set max count of how many messages to purge
- lavinmqperf throughput have a max uncofirmed messages option
- lavinmqperf throughput can multi ack X number of messages
- lavinmqperf consumer-churn command
- lavinmqctl status command

### Removed

- Remove SystemD socket activation/seamless restart ([#303](https://github.com/cloudamqp/lavinmq/pull/303))

### Fixed

- GC collect every 10s and unmap GC memory as soon as possible
- No 2GB limit of MFiles (segments/queue indicies)
- Messages are requeued correctly if delivery fails
- Can log to file using log_file config option
- Fix print build info on startup, log prefix on each line

### Changed

- Faster JSON generation for queues
- Build with Crystal 1.3.1
- Allow Get/Reject messages from paused queues in the HTTP API
- Use /var/lib/lavinmq as default path for data in containers
- libsystemd is no longer a dependency
- Faster ready messages bytesize counting

## [1.0.0-alpha.33] - 2021-10-28

### Fixed

- Fix stuck deliver_loop by yielding once in a while

## [1.0.0-alpha.32] - 2021-10-27

### Added

- On `USR1` print segments referenced by each queue
- Log to file, via config option `log_file`
- New option `--build-info` to print build information (Crystal, LLVM version etc)
- New option `--no-data-dir-lock` to stop lock file being created
- Added `lavinmqctl` and `lavinmqperf` binaries to Docker images
- Packages and images built with Crystal 1.2.1 and LLVM 10
- Keep debug symbols in Docker builds

### Fixed

- MFile (mmap) unmap pointer bug
- VHost definition import failed if default user was renamed
- Definitions export bug if compiled with LLVM 11
- Memory reporting on BSD/Mac, value was kbyte but handled as byte
- Assertions against acking wrong messages
- Handle IO::Error exceptions in read_loop

### Changed

- Unix domain socket default location changed to `/dev/shm/lavinmq-http.sock` on Linux
- Don't log if client EOF on connect, such as healthchecks

## [1.0.0-alpha.31] - 2021-06-28

### Fixed

- When starting, make sure to only load queues once even if the defintions.amqp includes creating/deleting the same queue multiple times
- Speed up logging by only looking at ENV once
- StartLimitIntervalSec was in the wrong SystemD service file section
- Catch and only log IO::Errors in accept loop (could happen if client disconnected very soon after TCP was established)

### Changed

- Log backtrace if unhandled exception occurs
- Only log if a publish confirm can't be delivered to client, don't raise

## [1.0.0-alpha.30] - 2021-04-25

### Fixed

- Restore PROXY protocol v2 functionality
- Correct header names for Received/Sent bytes in Connections listing

## [1.0.0-alpha.29] - 2021-04-23

### Fixed

- Build script fixes
- Charts rendered correctly

## [1.0.0-alpha.28] - 2021-04-23

### Added

- FreeBSD compatibility
- Crystal 1.0.0 compatibility
- Publisher confirm count to Overview chart
- Support for PROXY protocol v2, including SSL information

### Changed

- Delayed message exchange bug fix
- Trigger explicit GC of message segments on purge and delete queue

## [1.0.0-alpha.27] - 2021-03-09

### Added

- WebSocket support, for use with <https://github.com/cloudamqp/amqp-client.js>

### Changed

- Compress HTTP responses
- Faster restoring of queue indexes on boot

### Fixed

- Reply with FlowOk when getting Flow from channel
- Consitent clean up of dropped connections
- Mark vhost as dirty on boot to run at least one GC
- x-frame-options now applied correctly

## [1.0.0-alpha.26] - 2021-02-19

### Fixes

- Confirms are sent before messages are delivered to consumers
- Federation links connection recovery improved
- Null padded index files are truncated on restore
- File descriptors are always closed when files are deleted

### Changes

- Lower default segment size to 8MB
- Include client provided connection name (if any) in log statements

## [1.0.0-alpha.25] - 2021-01-30

### Fixed

- Deleted queues could reappear after a crash
- Never send frames to closed channels
- Checking RSS on OS X could fail when FD limit was reached
- Escaping connection name in UI to prevent XSS

### Changed

- Crystal 0.36.0 compability
- Don't open a temp write for every channel, only when needed

## [1.0.0-alpha.24] - 2021-01-26

### Added

- Support for consumer priority
- Display the timestamp of the first & last message in a Queue, reveals consumer lag

### Changed

- Index files are memory-mapped which makes them much faster
- Default git branch is renamed to main
- Default to 8MB segment size on non-Linux machines (that doesn't support hole punching)
- Default to 300s heartbeat timeout

### Fixed

- Consumer accounting wasn't right when client disconnected
- Some stats could overflow, fixed by making the counter UInt64

## [1.0.0-alpha.23] - 2021-01-22

### Fixed

- Federation: improvements to how messages are acked
- Segment positions must be in order for GC to work
- Show queue arguments in queue list

### Changed

- Removed ability to ack with requeue from HTTP API

## [1.0.0-alpha.22] - 2021-01-20

### Fixed

- Bug where we tried to write beyond segment capacity
- Missing return when bind/unbind preconditions failed
- Restore Queue@max_length_bytes when policy changes
- Start federation links when exchange policy is applied
- Don't expire messages when the queue has been closed
- Mark vhost as dirty when deleting or purging queues

### Changed

- Overwrite user on import definitions if used already exist
- Passwords are exported in RabbitMQ compatible format
- Message properties are included in shoveled and federated messages

## [1.0.0-alpha.21] - 2021-01-18

### Fixed

- Make sure client connections are cleaned up correctly
- Bug when creating and listing users in lavinmqctl
- Kill process if segment GC loop fails unexpectedly
- SegmentPosition is requeued if not expired
- Writes to `ack` file is always flushed to disk

### Added

- Autocomplete queues and exchanges in UI
- Create and delete queues and exchanges from lavinmqctl

## [1.0.0-alpha.20] - 2021-01-11

### Fixed

- x-max-length is always respected
- Validate x-max-priority
- Better handling in UI on server errors
- Remember table sorting option until session ends + correct sorting on page refresh
- Refresh last used time on queue when policy with expire applies to it
- Improved rendering of charts

### Changed

- Allow all printable ASCII characters in entity (queue and exchnage) names.

### Added

- Webhook shovel
- Autofill of update policy form when click on policy name in the queue, exchange or policies views.

## [1.0.0-alpha.19] - 2020-12-01

### Fixed

- Improved rendering of charts

### Changed

- Allow all printable ASCII characters in entity (queue and exchange) names.

## [1.0.0-alpha.18] - 2020-12-01

### Fixed

- Keep unacked messages sorted, as the merge sort in segment GC expects

## [1.0.0-alpha.17] - 2020-11-20

### Fixed

- Requeued messages wasn't always inserted back in correct order into the Ready queue
- Report correct disk usage for mmap:ed files
- Force close AMQP connections on shutdown after 10s timeout

### Changed

- Arguments given on command line overrides config file settings

## [1.0.0-alpha.16] - 2020-11-17

### Fixed

- Bug where messages larger than frame_max didn't get published properly

## [1.0.0-alpha.15] - 2020-11-17

### Fixed

- Message rate stats count messages in and out, not messages routed
- Charts now shows all the data it got, not limited

### Changed

- Debug symbols for ARM builds
- Only perform segment GC if messages has been consumed/deleted
- Copy message bodies (that fit in a single frame) to RAM before writing to msg store so that slow clients don't block
- frame_max lowered to 128kb

## [1.0.0-alpha.14] - 2020-11-11

### Fixed

- Write default definitions to disk immediately on start, don't wait for compaction
- Decreased memory usage when queues are long (~25%) and ~10% higher throughput performance
- Don't do unnecessary async msync syscalls

### Changed

- UI improvements
- Prefetch global now accounts for all consumers on the channel
- Improved HTTP API compatibility

### Added

- A lot more commands added to lavinmqctl

## [1.0.0-alpha.13] - 2020-10-30

### Fixed

- Messages that can't be delivered to client is properly requeued now
- Support for uploading definitions via the HTTP API (earlier only UI)
- Fixed a counter could overflow when there was more than 2GB of messages in the queues
- Many API calls are more robust when it's fed erroneous input data

### Changed

- Consistent HTTP status responses
- Smarter segment GC where we iterate the existing ready/unack queues instead of creating a new array
- Increase systemd max start time to 5min
- Optimize two cases in topic exchanges

### Added

- Building and distributing ARM64 debian and AMD64 RPM packages (via packagecloud.io)
- HTTP API documentation
- Log level is updated on SIGUP if changed in config

## [1.0.0-alpha.12] - 2020-10-25

### Added

- tcp_proxy_protocol config for if PROXY protocol is used on TCP connections

### Fixed

- Optimize publish of persisted messages when when publish confirm isn't enabled

## [1.0.0-alpha.11] - 2020-10-24

### Fixed

- Don't read message from disk on expiration unless required

## [1.0.0-alpha.10] - 2020-10-23

### Fixed

- Lower IOPS usage when messages are expired and all data doesn't fit in disk cache
- Churn metrics correctly reported from closed connections/channels

### Changed

- UI: Display more connection properties such as max_frame, TLS version and cipher
- UI: Nicer charts
- UI: Default sort order is now descending

### Added

- Releases are published on Docker hub: <https://hub.docker.com/repository/docker/cloudamqp/lavinmq>

## [1.0.0-alpha.9] - 2020-10-20

### Fixed

- Bug where segment files could be truncated at boot

### Changed

- Default segment size is now 1 GB (up from 32 MB)
- Report size of deleted segments without holes
- Truncate segments after last message (not hole punching)
- Looks for a config file at /etc/lavinmq
- Uses ENV["StateDirectory"] as data dir if set

### Added

- Support for systemd socket activation, both of the HTTP and AMQP sockets

## [1.0.0-alpha.8] - 2020-10-14

### Fixed

- All frames are counted as heartbeats, but we also make sure to send heartbeats if we only receive traffic
- Regression where only the first exchange to exchange binding was respected

### Added

- UI: Show exchange arguments

## [1.0.0-alpha.7] - 2020-10-13

### Fixed

- Send heartbeats at heartbeat timeout / 2
- UI: Always base64 bodies if not valid utf8
- HTTP API: Message stats on vhost
- HTTP API: More stat properites at /api/nodes
- HTTP API: Allow publishing msgs without body

## [1.0.0-alpha.6] - 2020-10-13

### Fixed

- GC collection doesn't close mmap:ings

## [1.0.0-alpha.5] - 2020-10-12

### Fixed

- Don't delete segments that might have references to it
- Write lock around segment rotation
- Don't double base64 encode message bodies in UI
- Allow PUT to queues without body

## [1.0.0-alpha.4] - 2020-10-12

### Fixed

- When allowing TLS 1.0 and 1.1, add SECLEVEL=1 to ciphers list (required in ubuntu/debian)
- Corrent page aligment when hole punching segment files

### Changed

- Don't strip debug symbols in debian/ubuntu package

## [1.0.0-alpha.3] - 2020-10-11

### Fixed

- Considerably faster garbage collection of segments and hole punching

### Changed

### Added

- tls_min_version configuration option, default is 1.2, but 1.0, 1.1 and 1.3 is are also allowed options
- tls_ciphers configuration option that sets enabled TLS ciphers (config and cmd line)
- Log IP when TLS connection fails
- Limit syscalls server is allowed to call, in systemd service file

- Don't log HTTP error when client disconnects

## [1.0.0-alpha.2] - 2020-10-08

### Fixed

- Routing of binding keys with multiple wildcards fixed

## [1.0.0-alpha.1] - 2020-10-07

### Fixed

- Memory usage monitoring is now showing current RSS, not max RSS
- Consumer cancellation doesn't requeue unacked messages
- UI: No more JSON parsing errors when the server is offline
- UI: Fix name filter pagination bug
- Support delayed exchange via policy and x-delayed-message exchange type
- Clear all user permissions to a vhost when it's deleted
- Allow exchange to exchange binding for internal exchanges
- Redeclare queue updatex expiration time correctly
- Stricter validation of queue argments
- Stricter validation of frame sizes
- Closing connections on missed heartbeat (even if the TCP connection is alive)
- Correctly requeue cancelled consumers messages on close
- Respect the not_in_use flag when deleting exchanges
- UI: Fixing the Unbind button on Exchange in the UI
- Support for Nack with delivery tag 0
- Stricter exchange and queue name validation
- Dead-letters can't loop
- Detect header/body frames that are out of order
- Don't allow binding/unbinding exclusive queues
- Use the shortest TTL of expiration header and queue message-ttl
- Validates the expiration field on publish
- Semantic comparison of headers when declaring queues/exchanges

### Changed

- On-disk file formats has changed so previous data directories are incompatible with this version
- Decreased memory usage and increased performance by reimplemented segment GC
- Only listen on localhost by default
- DEB packages are distributed via packagecloud.io
- Log less on shutdown
- Higher throughput due to revamped segment GC algorithm
- Validate x-match headers
- Don't allow declaring or deleting the default exchange
- lavinmqctl now uses a private unix socket for communication
- Make queue/exchange delete and unbind idempotent

### Added

- Support for consistent hash exchange
- Support for exchange federation
- Support for priority queues
- Ability to "pause" queues, stopping messages to be delivered to consumers
- max-length-bytes supported as queue argument and policy
- UI: Shows bytes of messages a queue hold
- UI: Show data rates and heartbeats in connections listing
- Reload TLS certificates on HUP signal
- systemctl reload lavinmq now supported in systemd (by sending HUP to main pid)
- UNIX socket support for HTTP server
- Documented how persistent exchange works in the readme
- Promethous exporter at /metrics
- Respect the CC header when dead-lettering
- Support for Decimal values in headers
- UI: Show Unroutable messages rates in Exchange graph
- UI: Churn stats
- Queue bind now substitutes empty queue and routing key values with last declared queue
- UserID header is now validate
- Better server properties sent on connection establishment

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

- The --persistent flag is actually being respected in lavinmqperf throughput
- On shutdown, force close client connections
- Can now successfully delete shovels that has connection problems

## [0.10.7] - 2020-06-19

### Fixed

- Prevent overflow exception in shovel, don't look for short queues

## [0.10.6] - 2020-06-19

### Fixed

- Fixed overflow exception in /api/nodes if uptime was more than Int32::MAX milliseconds

## [0.10.5] - 2020-06-18

### Fixed

- Crystal 0.35 compatibility
- Decrease segment position counter on queue delete

### Changed

- fsyncing on publish confirm as soon as possible (previously only every 200ms)

### Added

- robots.txt file to disallow crawling
- Possibility to cancel consumers from the UI

## [0.10.4] - 2020-06-09

### Fixed

- Only use `copy_file_range` when the glibc version supports it

### Added

- Display message rates in queues list in mgmt UI
- Config for changing endianess of on-disk data

## [0.10.3] - 2020-05-26

### Fixed

- XSS in shovel UI
- Echo incoming heartbeat if we didn't send one recently

### Changed

- Faster segment GC
- Using zero-copy syscall `copy_file_range` between temp file and segment

## [0.10.2] - 2020-05-01

### Changed

- The way to find holes in segment files to punch is much improved

## [0.10.1] - 2020-04-26

### Fixed

- Updated amq-protocol.cr, fixes skipping headers in partial GC

## [0.10.0] - 2020-04-26

### Fixed

- All settings in the file config is respected now, not just a few

### Changed

- Partial GC of segments by hole punching, disk usage is much lower now
- Lower default file read buffer to 16KB
- Removed some HTTP protection measures that aren't applicable
- If writing to the definitions.amqp file fails abort the whole application
- Periodically write to the lock file to detect lost lock

### Added

- Does now notify systemd when properly started

## [0.9.16] - 2020-04-13

### Fixed

- Keep reference to the opened .lock file until shutdown so it's not GCed
- Publish stats correct

## [0.9.15] - 2020-04-09

### Fixed

- Crystal 0.34.x compability
- Rescue error while trying to parse the PROXY protocol
- Header exchange binding without arguments matches empty message headers
- lavinmqperf throughput now declares queues smarter

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

### Fixed

- Bug when publishing to a queue that has reach its max-length
- Memory leak of queue lookup caches, the caches are now moved to the Channel

### Changed

- No logging of GC of segments, except in debug mode
- Message bodies smaller than a frame is written directly to disk
- Message bodies spread out over several frames to temporarily written to disk

### Added

- More stats in the /api/nodes endpoint, including CPU, diskspace, IOPS etc
- Structure the output of USR1 signal better

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

### Changed

- Default to 0 heartbeat
- Use socket timeout for heartbeat sending, instead of a looping fiber
- Round message stat metrics to 1 decimal

### Added

- Report accumulated message stats in the /api/overview endpoint

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

### Changed

- Only ignore publish frames on closed channels
- Different log message if user is missing or password is wrong

### Added

- Signal USR1 prints various stats
- Signal USR2 forces a GC collect
- Signal HUP only reloads the config file

## [0.9.2] - 2020-02-17

### Fixed

- Changes to users (password, tags) are store to disk

### Changed

- Crystal 0.33.0
- lavinmqperf: use fibers, don't fork
- Only ack:ing persistent messages are now written to the queue index
- Mutexes around the unacked dequeue in Consumer, for thread safety

### Added

- lavinmqperf: --persistent flag for throughput tests
- lavinmqperf: queues are now bound to the exchange

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
- lavinmqperf throughput now forks for each connection instead of spawn
- Speed up GC of segments by using reference counting

## [0.8.5] - 2019-09-19

### Fixed

- Sorting of numbers is done correctly

### Changed

- Dont wakeup the heartbeat loop on connection cleanup, might be the cause of a Invalid memory access
- Don't fallocate/preallocate segment or index files, minimal performance gain on XFS

### Added

- Applications array with LavinMQ version added to /api/nodes response

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
- Faster lavinmqproxy

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

### Fixed

- AMQPLAIN support in shovels is implemented correctly

### Added

ShortStrings are now added to StringPool for reduced GC pressure, the pool size is printed on HUP

### Changed

- Make setting the timestamp property optional through a config setting (set_timestamp), default to false
- Optimized topic exchange and fanout routing, giving ~5% and ~30% throughput boost respectively
- Using a Channel to communicate between Client and Vhost when publishing, removed the need for a lock

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
