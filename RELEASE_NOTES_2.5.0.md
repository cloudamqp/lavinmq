# LavinMQ 2.5.0 Release Notes

LavinMQ 2.5.0 includes performance improvements, UI updates, and better clustering support. This release focuses on faster boot times, a refactored priority queue system, and an improved management interface.

## Breaking Changes

### Deprecated Metrics

Queue metrics have been renamed for consistency [#1121](https://github.com/cloudamqp/lavinmq/pull/1121):

- `ready` → use `messages_ready`
- `ready_bytes` → use `message_bytes_ready`
- `unacked` → use `messages_unacknowledged`
- `unacked_bytes` → use `message_bytes_unacknowledged`

The old names still work in 2.5.0 but will be removed in 3.0.0.

### Crystal Version Requirement

LavinMQ 2.5.0 requires Crystal 1.18.0 or later [#1356](https://github.com/cloudamqp/lavinmq/pull/1356), [#1360](https://github.com/cloudamqp/lavinmq/pull/1360).

### Metrics Server Port Configuration

The Prometheus metrics endpoint can now be served on a separate port [#1217](https://github.com/cloudamqp/lavinmq/pull/1217). In clustered setups, follower nodes no longer forward `/metrics` requests to the leader—each node serves its own metrics.

## Performance Improvements

### Faster Boot Times

Boot time has been optimized by storing message counts in metadata files [#1163](https://github.com/cloudamqp/lavinmq/pull/1163). Previously, LavinMQ scanned all message segments on startup to count messages. With metadata files, servers with large message backlogs can now start up much faster.

### Priority Queue Refactor

Priority queues now use one message store per priority level instead of a single interleaved store [#1156](https://github.com/cloudamqp/lavinmq/pull/1156). This reduces memory usage since the message index doesn't need to reside entirely in memory, improves I/O with sequential reads, and fixes an issue where messages were incorrectly marked as redelivered. The migration happens automatically on first startup.

### Network I/O Optimization

Socket write buffers are now only flushed when the delivery loop would block [29ab3436](https://github.com/cloudamqp/lavinmq/commit/29ab3436), reducing system calls and improving throughput for high-message-rate scenarios.

### Clustering Improvements

Follower synchronization is faster thanks to file checksum caching [#1088](https://github.com/cloudamqp/lavinmq/pull/1088). The leader maintains a cache of checksums instead of recalculating them for every file. The clustering codebase has been refactored [#1164](https://github.com/cloudamqp/lavinmq/pull/1164) to improve reliability. Consumer fairness for AMQP has also been improved [#1173](https://github.com/cloudamqp/lavinmq/pull/1173).

## User Interface

### Light Mode

The management interface now includes a light mode theme [46b00e4c](https://github.com/cloudamqp/lavinmq/commit/46b00e4c). Your preference is saved in browser storage.

### Navigation Updates

The sidebar has been updated with icons and better grouping [#1268](https://github.com/cloudamqp/lavinmq/pull/1268).

### Data Exploration

Table filters now respond as you type [#1274](https://github.com/cloudamqp/lavinmq/pull/1274). You can read messages directly from stream queues in the management interface [#1236](https://github.com/cloudamqp/lavinmq/pull/1236). The logs page keeps column headers visible while scrolling [#1333](https://github.com/cloudamqp/lavinmq/pull/1333). Exchange types now show readable names [#1238](https://github.com/cloudamqp/lavinmq/pull/1238).

### Shovel Management

Shovels can be paused and resumed via the API and management interface [#1103](https://github.com/cloudamqp/lavinmq/pull/1103), [#1151](https://github.com/cloudamqp/lavinmq/pull/1151). Pause state is persisted across restarts. The shovel interface now handles stream queues correctly [#1337](https://github.com/cloudamqp/lavinmq/pull/1337).

## MQTT

### Permissions

MQTT clients now respect LavinMQ's permission system [#1275](https://github.com/cloudamqp/lavinmq/pull/1275). Publish operations check write permissions, and subscribe operations check read/write permissions on topics.

### Protocol Support

Added support for MQTT 3.1.0 clients in addition to 3.1.1.

## Tooling and Operations

### lavinmqperf

The benchmarking tool now supports MQTT [#983](https://github.com/cloudamqp/lavinmq/pull/983). It also waits for all clients to connect before starting [#1120](https://github.com/cloudamqp/lavinmq/pull/1120).

### Channel Consumer Limits

You can limit the number of consumers per channel with the `x-max-consumers` argument [#1106](https://github.com/cloudamqp/lavinmq/pull/1106).

### etcd Authentication

Clustering deployments can now use Basic Authentication and HTTPS for etcd connections [#1212](https://github.com/cloudamqp/lavinmq/pull/1212).

### Alpine Linux

Alpine Linux is now supported [#1115](https://github.com/cloudamqp/lavinmq/pull/1115). Container images include SBOM and provenance information [54a3cc21](https://github.com/cloudamqp/lavinmq/commit/54a3cc21).

## API

The `/api/queues` endpoint now exposes `messages_ready` and `messages_unacknowledged` [#1121](https://github.com/cloudamqp/lavinmq/pull/1121). The web interface includes checksum verification for JavaScript dependencies [#1257](https://github.com/cloudamqp/lavinmq/pull/1257).

## Bug Fixes

### Topic Exchange Routing

Fixed a bug where topic exchanges wouldn't route messages correctly to all matching bindings [#1300](https://github.com/cloudamqp/lavinmq/pull/1300).

### Stream Consumers

Fixed stream consumers sometimes failing to flush messages to the socket [#1385](https://github.com/cloudamqp/lavinmq/pull/1385).

### Dead Letter Loops

Improved dead letter loop detection to prevent infinite loops [#1206](https://github.com/cloudamqp/lavinmq/pull/1206).

### Federation and Shovels

Federation now respects the `max-hops` setting [#1130](https://github.com/cloudamqp/lavinmq/pull/1130). Shovels with `DeleteAfter::QueueLength` are no longer deleted when paused [#1376](https://github.com/cloudamqp/lavinmq/pull/1376). Federation links handle variable references correctly [#1378](https://github.com/cloudamqp/lavinmq/pull/1378).

### MQTT

Fixed reconnecting MQTT clients with unacked messages being blocked [#1102](https://github.com/cloudamqp/lavinmq/pull/1102). MQTT clients can now publish messages larger than 64 KiB [cd77b3f0](https://github.com/cloudamqp/lavinmq/commit/cd77b3f0).

### Clustering

Fixed proxying requests from followers during full sync [#1283](https://github.com/cloudamqp/lavinmq/pull/1283). Corrected the condition for writing followers to the ISR list [#1341](https://github.com/cloudamqp/lavinmq/pull/1341). Meta files are now replicated [#1365](https://github.com/cloudamqp/lavinmq/pull/1365).

### Authentication

AMQPLAIN authentication now handles missing keys gracefully [035b8fdf](https://github.com/cloudamqp/lavinmq/commit/035b8fdf). Permission denial logs now include the username [#1392](https://github.com/cloudamqp/lavinmq/pull/1392).

## Upgrade Notes

### Before Upgrading

Back up your data directory. If you use the deprecated queue metrics, plan to update your dashboards. Ensure Crystal 1.18.0 or later is available if building from source.

### Upgrade Considerations

The marker file for paused queues changed from `.paused` to `paused` [#1209](https://github.com/cloudamqp/lavinmq/pull/1209). This happens automatically, but external tools that look for `.paused` files will need updating.

### During Upgrade

Priority queue storage will be migrated automatically. This may take a moment for large queues. Monitor startup logs for large installations.

### After Upgrading

If running in clustered mode, verify followers synchronize correctly. If using topic exchanges heavily, verify routing behaves as expected. Start transitioning to the new metric names.
