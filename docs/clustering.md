# Clustering

LavinMQ supports multi-node clustering with leader-based replication. Two coordination backends are available:

- **etcd** (default) — uses an external etcd cluster for leader election and shared state.
- **raft** — self-contained consensus built into LavinMQ itself; no external service required.

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **Coordination backend** — tracks leader election, ISR membership, and cluster state (etcd or raft).

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

### etcd backend (default)

```ini
[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
etcd_endpoints = etcd1:2379,etcd2:2379,etcd3:2379
etcd_prefix = lavinmq
```

### Raft backend

The raft backend does not require etcd. Set `backend = raft` and open the raft port (default 5680) between nodes:

```ini
[clustering]
enabled = true
backend = raft
bind = 0.0.0.0
port = 5679
raft_port = 5680
advertised_uri = tcp://node1.example.com:5679
```

See [Configuration](configuration.md) for all clustering options.

#### Declarative cluster formation with `seed_uris`

Instead of joining nodes one by one with `lavinmqctl raft_join`, you can let nodes form the cluster automatically by giving every node the same seed list:

```ini
[clustering]
enabled = true
backend = raft
advertised_uri = tcp://node1.example.com:5679
seed_uris = http://node1.example.com:15672,http://node2.example.com:15672,http://node3.example.com:15672
```

The same list goes on every node, including the node itself. Equivalent forms:

| Method | Syntax |
|--------|--------|
| INI (`[clustering]`) | `seed_uris = http://node1:15672,http://node2:15672,http://node3:15672` |
| CLI flag | `--clustering-seed-uris=http://node1:15672,http://node2:15672,http://node3:15672` |

There is no environment variable for `seed_uris`.

**How formation works:** when a node boots with a seed list and no existing cluster state, it compares its advertised host against the seed hosts. The node whose advertised host sorts lexicographically lowest bootstraps a single-node cluster; the rest join it. Boot all nodes simultaneously and the cluster forms with no manual commands.

**`clustering_advertised_uri` must match a seed entry.** Each node identifies itself in the seed list by its advertised host. If a node's advertised host does not match any seed host, it will always attempt to join (never bootstrap) — a safe failure, but the cluster won't form if the lowest-host node never identifies itself.

**Initial formation requires the lowest-host node.** If the node with the lexicographically lowest host in the seed list is down at boot time, no node will bootstrap; the others retry joining until it appears. Once the cluster exists, that node is an ordinary member with no special role.

#### Recovery runbook

To rebuild a node that has lost or diverged its raft state:

1. Run `lavinmqctl raft_reset` on the affected node. This wipes raft state from the data directory and signals the running node to exit. If the node is currently running and belongs to a multi-peer cluster (or its status cannot be verified), the command requires `--force` to override the safety guard; a stopped node is wiped unconditionally without `--force`.
2. Restart lavinmq. If `seed_uris` is configured, the node rejoins automatically.

**Exception — recovering the lowest-host node:** after `raft_reset`, the lowest-host node sees no peers and would bootstrap a new cluster, splitting the existing one. Use `lavinmqctl raft_join <other-member-uri>` instead. This writes a `.join_target` marker that forces the node to join on restart rather than bootstrap:

```
lavinmqctl raft_join http://node2.example.com:15672
```

The imperative `raft_join` path is retained specifically for this recovery scenario.

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
