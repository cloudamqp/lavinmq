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

### Bulk Sync

When a follower first connects (or has fallen too far behind), it performs a bulk sync:

1. The leader sends a file index with checksums of all data files
2. The follower requests files that are missing or have mismatching checksums
3. While syncing, the leader queues changes

### Incremental Replication

After bulk sync, the leader streams changes in real-time:

- **Appends** — bytes to append to data files (message segments, definitions)
- **Deletes** — files that have been removed
- **Rewrites** — files that have been completely rewritten (e.g., compacted definitions)

Data is compressed with LZ4 during replication.

### What Gets Replicated

- Definitions (exchanges, queues, bindings, users, permissions, policies, parameters)
- Message data (segments, acknowledgment files)
- All persistent vhost data

### ISR (In-Sync Replicas)

The ISR set tracks which followers are fully synchronized. A follower joins the ISR after completing bulk sync and staying current. The `max_unsynced_actions` setting (default 8192) controls how many unacknowledged actions a follower can lag before being removed from ISR.

## Failover

If the leader fails, etcd coordinates leader election among ISR members. The follower with the most up-to-date data is elected.

### Leader Election Hooks

Shell commands can be executed on leadership transitions:

```ini
[clustering]
on_leader_elected = /usr/local/bin/update-dns.sh
on_leader_lost = /usr/local/bin/drain-connections.sh
```

## Clustering Proxy

Followers can proxy client connections to the current leader, allowing clients to connect to any node. The proxy transparently forwards traffic to the leader.

## Security

Followers authenticate to the leader using a shared password derived from the etcd cluster state.
