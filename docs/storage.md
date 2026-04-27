# Storage

LavinMQ uses a disk-first persistence model. Messages are written directly to disk and served from the OS page cache, avoiding the overhead of maintaining an in-memory copy.

## Segment-Based Storage

Messages are stored in segment files. Each segment is a fixed-size file (default 8MB, configurable via `segment_size`).

- New messages are appended to the current write segment
- When a segment is full, a new segment is created
- Segments are named sequentially (`msgs.0000000000`, `msgs.0000000001`, etc.)

## Memory-Mapped Files

Segment files are memory-mapped (mmap), allowing the OS to manage caching. This means:

- Read performance depends on available system memory for page cache
- No explicit message cache management needed
- Memory usage is bounded by the OS, not LavinMQ

## Acknowledgment Tracking

Each segment has a corresponding ack file (`acks.{segment_id}`) that tracks which messages have been acknowledged (deleted):

- When a message is acked, its position is recorded in the ack file
- The ack file is a compact list of deleted message positions within the segment

## Segment Garbage Collection

When all messages in a segment have been acknowledged, the segment and its ack file are deleted. This happens automatically as consumers process messages.

## Data Directory Layout

```
<data_dir>/
  .lock                      # Data directory lock file
  users.json                 # User definitions
  vhosts.json                # Vhost list
  <vhost_sha1>/              # Per-vhost directory (SHA1 of vhost name)
    definitions.amqp         # Exchange, queue, binding, policy definitions
    <queue_sha1>/            # Per-queue directory (SHA1 of queue name)
      msgs.0000000000        # Message segment files
      acks.0000000000        # Ack tracking files
      meta.0000000000        # Metadata files (used by some queue types)
```

## Data Directory Locking

LavinMQ acquires an exclusive file lock on the data directory to prevent multiple instances from corrupting data. This is enabled by default and can be disabled with `--no-data-dir-lock` (not recommended).

## Disk Space Monitoring

| Config Key | Default | Description |
|-----------|---------|-------------|
| `free_disk_min` | `0` | Minimum free disk space (bytes). Publishing is blocked when free space drops below this value. |
| `free_disk_warn` | `0` | Warning threshold (bytes). Emits warnings when free space drops below this value. |

Publishing is blocked when free disk space drops below `3 * segment_size` or below `free_disk_min` (whichever is higher). With the default `free_disk_min = 0`, the `3 * segment_size` threshold (default ~24 MB) is the active trigger. When publishing is blocked, the server returns `precondition_failed` channel errors on `basic.publish`.

## Definitions Compaction

Definitions (exchanges, queues, bindings, etc.) are stored in an append-only file. Deletions are also appended. When the number of delete operations exceeds `max_deleted_definitions` (default 8192), the file is compacted by rewriting only the current state.
