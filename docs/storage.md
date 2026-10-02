# Storage

LavinMQ uses a disk-first persistence model. Messages are written directly to disk and served from the OS page cache, avoiding the overhead of maintaining an in-memory copy.

## Segment-Based Storage

Messages are stored in fixed-size segment files. New messages are appended to the current write segment; when it fills, a new segment is created. Segments are named sequentially (`msgs.0000000000`, `msgs.0000000001`, etc.).

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `segment_size` | `[main]` | `8388608` | Segment file size in bytes (8MB) |

## Memory-Mapped Files

Segment files are accessed via `mmap(MAP_SHARED)` rather than `read`/`write` syscalls. When a segment is created, the file is pre-grown to `segment_size` with `ftruncate` and the entire range is mapped into the process address space. Reads and writes then go through normal pointer access, and the kernel's page cache transparently handles which pages are resident in RAM.

This avoids the syscall and copy overhead of buffered I/O — hot data is served straight from the page cache as a memory access — and lets LavinMQ keep its memory footprint small while still benefiting from all the RAM the OS chooses to use as cache.

Streams release a segment's resident pages as soon as its last reader leaves, for example when a consumer advances to another segment or is cancelled. The active write segment is excluded from this cleanup. Releasing pages does not delete the segment or its messages; a later reader can load them again from disk. This keeps memory usage during a stream replay from accumulating across all the segments already read.

## Acknowledgment Tracking

For standard and priority queues, each segment has a corresponding ack file (`acks.{segment_id}`) that tracks which messages have been acknowledged (deleted):

- When a message is acked, its position is recorded in the ack file
- The ack file is a compact list of deleted message positions within the segment

Streams do not record individual message deletions in ack files. Acknowledgments can persist consumer offsets, while retention determines when messages are removed. Any leftover stream ack files are discarded when the stream loads. See [Streams](streams.md) for offset tracking and retention.

## Segment Garbage Collection

For standard and priority queues, when all messages in a segment have been acknowledged, the segment and its ack file are deleted. This happens automatically as consumers process messages. Streams instead delete whole segments according to their retention settings.

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

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `free_disk_min` | `[main]` | `0` | Minimum free disk space (bytes). Publishing is blocked when free space drops below this value. |
| `free_disk_warn` | `[main]` | `0` | Warning threshold (bytes). Emits warnings when free space drops below this value. |

Publishing is blocked when free disk space drops below `3 * segment_size` or below `free_disk_min` (whichever is higher). With the default `free_disk_min = 0`, the `3 * segment_size` threshold (default ~24 MB) is the active trigger. When publishing is blocked, the server returns `precondition_failed` channel errors on `basic.publish`.

## Definitions Compaction

Definitions (exchanges, queues, bindings, etc.) are stored in an append-only file. Deletions are also appended. When the number of delete operations exceeds the configured threshold, the file is compacted by rewriting only the current state.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `max_deleted_definitions` | `[main]` | `8192` | Delete operations before the definitions file is compacted |
