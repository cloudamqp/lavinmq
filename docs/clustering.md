# Clustering

LavinMQ supports multi-node clustering with leader-based replication. Leader election, ISR tracking and shared state are handled by a coordination **backend**: either an external **etcd** cluster (default) or an **embedded Raft** group (no external dependency).

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **Coordination backend** — `etcd` (external) or `raft` (embedded); handles leader election, ISR tracking, and advertising the leader's URI.

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

### etcd backend (default)

```ini
[clustering]
enabled = true
backend = etcd
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
etcd_endpoints = etcd1:2379,etcd2:2379,etcd3:2379
etcd_prefix = lavinmq
```

### Raft backend (embedded, no etcd)

The Raft backend runs consensus inside LavinMQ itself, so no etcd is needed. It requires a few extra settings because, unlike etcd, it can't auto-discover the cluster:

- **Static membership** — every node must be listed in `raft_peers` as `id@host:port`, where `id` is a small integer that is unique across the cluster and is used as both the Raft node id and the LavinMQ clustering id. Set `raft_node_id` to this node's own id.
- **Replication secret** — the shared secret used to authenticate followers is **not** stored in the Raft log. Each node reads it from `<data_dir>/.replication_secret`. Generate a secret once and copy the same file to every node (like an Erlang cookie). LavinMQ refuses to start in Raft mode if the file is missing.

```ini
[clustering]
enabled = true
backend = raft
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
; this node's id; must match its entry in raft_peers
raft_node_id = 1
; all nodes (including this one): id@host:port for the Raft transport
raft_peers = 1@node1.example.com:5680,2@node2.example.com:5680,3@node3.example.com:5680
raft_bind = 0.0.0.0
raft_port = 5680
```

Create the shared secret on every node:

```sh
head -c 32 /dev/urandom | base64 > /var/lib/lavinmq/.replication_secret
# copy the identical file to every node's data dir
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

For AMQP TCP traffic, the proxy prepends a PROXY protocol v1 header so the leader sees the original client address. No further configuration is needed; the proxy starts and stops automatically as leadership changes.

## Security

Followers authenticate to the leader using a shared secret stored in etcd. The secret is randomly generated on first cluster initialization and stored under `{etcd_prefix}/clustering_secret`.
