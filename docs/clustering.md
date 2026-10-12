# Clustering

LavinMQ supports multi-node clustering with leader-based replication, using etcd for leader election and coordination.

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **etcd** — external coordination service for leader election, ISR tracking, and shared state.

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

```ini
[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
etcd_endpoints = etcd1:2379,etcd2:2379,etcd3:2379
etcd_prefix = lavinmq
```

See [Configuration](configuration.md) for all clustering options.

## Replication

### The Journal

Every replicated change on the leader (an append to a segment or to the definitions log, a rewritten JSON file, a deleted file, a request to sync files to disk) is appended to an in-memory journal and gets a sequence number. Each synced follower has its own sender that reads the journal from the position the follower joined at, compresses the entries with LZ4 and writes them to the follower. A publish therefore only appends to the journal; compression and network writes never run in the publishing connection.

The journal keeps a bounded number of entries (262,144, about 18 MiB of memory; message bodies are not copied into it). A follower that falls further behind than that disconnects and rejoins with a bulk sync.

### Resuming

A follower records its position in the leader's journal in `.replication_position` in its data directory: the journal's id and the last entry it has applied, written after syncing the data directory (at every full-filesystem sync, at least every 30 seconds while streaming, and on shutdown or promotion). When it reconnects it offers that position, and if the leader still holds every later entry it streams from there: no file lists, no checksums, no file transfers. Entries applied after the position was written are simply applied again, which is harmless since every append carries its offset. A restarted or briefly disconnected follower therefore rejoins in seconds. A new leader has a new journal, so after a failover followers do a bulk sync.

### Bulk Sync

When a follower first connects, follows a new leader, or has fallen behind the journal, it performs a bulk sync:

1. The leader sends a file index with checksums of all data files
2. The follower requests files that are missing or have mismatching checksums
3. The leader takes a journal position as the cut, sends a second, smaller index of what changed during step 2 (each file capped at its size at the cut), and the follower requests those files too
4. The follower's sender starts streaming the journal from the cut

Writers are never paused by a join: changes made while a follower syncs are journaled and reach it through its sender.

### Incremental Replication

After bulk sync, the leader streams the journal in real-time:

- **Appends** — bytes to append to data files (message segments, definitions)
- **Deletes** — files that have been removed
- **Rewrites** — files that have been completely rewritten (e.g., compacted definitions)

Data is compressed with LZ4 during replication.

### Replication Durability

For AMQP publisher confirms and MQTT QoS 1 PUBACKs, the leader requests synchronization of the affected files before waiting for acknowledgments from every in-sync follower. A follower synchronizes the requested files before acknowledging past that point in the replication stream. If a follower leaves the ISR during the wait, the leader commits that membership change before confirming to the publisher, so a node that missed the confirmed data cannot be promoted.

Replication protocol version 2 carries these synchronization requests, allowing followers to sync only the requested files instead of flushing the whole filesystem before every acknowledgment. A whole-filesystem sync is requested for transaction commits, and a follower also falls back to it when the number of requested files exceeds its own `syncfs_threshold` setting in `[main]` (default `64`).

Replication protocol version 3 names every record by its journal sequence number and gives every append its offset in the file. A follower writes appends at that offset, so a record it already has (for example the tail of the file it received when joining) is rewritten with the same bytes instead of appended twice, and a repeated or missing sequence number is detected. Followers acknowledge the sequence number of the last record they applied, together with the number of bytes received so far, so the leader waits for exactly the entries a confirm depends on and still sees progress while a large message is being transferred.

Version 1 and 2 peers remain compatible, so a cluster can be upgraded one node at a time. Version 1 connections use the previous behavior: the follower syncs the whole filesystem before each acknowledgment. The protocol version is negotiated automatically; a follower that connects with a newer version than the leader speaks reconnects with the leader's.

Synchronization is enabled by default. Setting `sync=false` in `[main]` (or using `--no-sync`) on a node disables its disk synchronization; replication acknowledgments from that node then do not guarantee disk durability. The leader and each follower use their own configuration. See [Publisher Confirms](publisher-confirms.md#durability-and-synchronization) for the leader's synchronization behavior.

### What Gets Replicated

- Definitions (exchanges, queues, bindings, users, permissions, policies, parameters)
- Message data (segments, acknowledgment files)
- All persistent vhost data

### ISR (In-Sync Replicas)

The ISR set tracks which followers are fully synchronized. A follower joins the ISR after completing bulk sync and staying current.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `max_unsynced_actions` | `[clustering]` | `8192` | **Deprecated:** still accepted but has no effect. A follower is removed from the ISR when it stops acking replicated data within the leader's ack deadline |

## Failover

If the leader fails, etcd coordinates leader election among ISR members. The first ISR member to successfully campaign becomes the new leader. A node that wins the election while no longer in the ISR (its candidacy was queued before it fell out of sync) steps down immediately — it releases its lease and exits so an in-sync candidate can win, and rejoins as a follower after re-syncing.

### Leader Election Hooks

Shell commands can be executed on leadership transitions:

```ini
[clustering]
on_leader_elected = /usr/local/bin/update-dns.sh
on_leader_lost = /usr/local/bin/drain-connections.sh
```

## Clustering Proxy

When a node is a follower, it automatically proxies client traffic to the current leader. Clients can connect to any node in the cluster on the normal protocol ports and reach the leader without needing to know which node is the leader.

The proxy is transparent and runs on every follower for:

- AMQP and AMQPS (TCP and Unix socket)
- MQTT and MQTTS (TCP and Unix socket)
- HTTP/management (TCP and Unix socket)

TCP listeners always proxy; Unix-socket proxying activates per protocol when the matching `unix_path` is configured in `[amqp]`, `[mqtt]`, or `[mgmt]`. The same setting controls both the listener on the leader and the proxy socket on a follower, so configuring `unix_path` once gives clients a consistent Unix socket on every node.

For AMQP and MQTT TCP traffic, the proxy prepends a PROXY protocol v1 header so the leader sees the original client address. No further configuration is needed; the proxy starts and stops automatically as leadership changes.

## Security

Followers authenticate to the leader using a shared secret stored in etcd. The secret is randomly generated on first cluster initialization and stored under `{etcd_prefix}/clustering_secret`.
