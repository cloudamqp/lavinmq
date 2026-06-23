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

**How formation works:** when a node boots with a seed list and no existing cluster state, it compares its advertised host against the seed hosts. The node whose advertised host sorts lexicographically lowest bootstraps a single-node cluster; the rest join it. With the shared secret in place (see the prerequisite below), boot all nodes simultaneously and the cluster forms without per-node `raft_join` commands.

**Prerequisite — distribute the replication secret first.** Followers authenticate to the leader with a shared secret. On the raft backend this secret lives in `<data_dir>/.clustering_password` and is **not** replicated between nodes. Generate one secret and place the *identical* `.clustering_password` (mode `0600`) in every node's data directory **before** booting:

```sh
openssl rand -base64 32 > /var/lib/lavinmq/.clustering_password
chmod 0600 /var/lib/lavinmq/.clustering_password
# copy the same file to every node (config management, a mounted secret, etc.)
```

If you skip this, only the node that bootstraps will have a secret (it auto-generates one), and every other node logs a fatal "Replication secret file missing" error and exits when it tries to follow the leader — until you copy that file to them. A node that already has the file reads it as-is and never generates a new one, so pre-placing the same secret everywhere is safe.

**`clustering_advertised_uri` must match a seed entry.** Each node identifies itself in the seed list by its advertised host. If a node's advertised host does not match any seed host, it will always attempt to join (never bootstrap) — a safe failure, but the cluster won't form if the lowest-host node never identifies itself.

**Initial formation requires the lowest-host node.** If the node with the lexicographically lowest host in the seed list is down at boot time, no node will bootstrap; the others retry joining until it appears. Once the cluster exists, that node is an ordinary member with no special role.

#### Recovery runbook

To rebuild a node that has lost or diverged its raft state:

1. Run `lavinmqctl raft_reset` on the affected node. This wipes raft state from the data directory and, if a node is running, signals it to exit. The command **fails closed**: it proceeds without `--force` only when it can prove the node is safe to discard — a running node whose `/raft/status` reports a single-node cluster (≤1 peer). A node that is **stopped** (no control socket / unreachable), a follower, in a multi-peer cluster, or whose status omits the peer list cannot be verified, so it requires `--force` to override the guard. (Most recovery scenarios involve a stopped or unhealthy node, so expect to pass `--force`.)
2. Restart lavinmq. If `seed_uris` is configured, the node rejoins automatically.

**Exception — recovering the lowest-host node:** after `raft_reset`, the lowest-host node sees no peers and would bootstrap a new cluster, splitting the existing one. Use `lavinmqctl raft_join <other-member-uri>` instead. This writes a `.join_target` marker that forces the node to join on restart rather than bootstrap:

```
lavinmqctl raft_join http://node2.example.com:15672
```

The imperative `raft_join` path is retained specifically for this recovery scenario.

#### Known limitation — ISR commits under quorum loss

On the raft backend, the in-sync replica set (ISR) is committed through raft consensus while the leader holds its replication-dispatch lock. If the raft cluster loses quorum (e.g. two of three nodes down), an ISR commit blocks until leadership is lost (an election timeout), which can stall the data plane for that window — even when leader→follower data replication is otherwise healthy. This is not a regression from the etcd backend, which commits the ISR under the same lock (with a bounded single PUT); it is a worse latency profile specific to raft's blocking consensus. Moving the ISR commit out from under that lock is tracked as a follow-up.

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

Followers authenticate to the leader using a shared replication secret. How it is stored depends on the clustering backend:

- **`etcd`** — stored in etcd under `{etcd_prefix}/clustering_secret`, randomly generated on first cluster initialization. Every node reads it from etcd, so no manual distribution is needed.
- **`raft`** — stored in each node's `<data_dir>/.clustering_password` and **not** replicated between nodes. The bootstrapping node auto-generates one (mode `0600`) if absent; every other node must already have the *same* file or it exits when it tries to follow the leader. Distribute the identical secret to all nodes before forming the cluster — see [Declarative cluster formation with `seed_uris`](#declarative-cluster-formation-with-seed_uris) for the prerequisite and the generation command.
