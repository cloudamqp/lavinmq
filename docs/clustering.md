# Clustering

LavinMQ supports multi-node clustering with leader-based replication. Leader election and the in-sync replica set (ISR) are kept by one of two backends, chosen with `backend` in `[clustering]`:

- **`raft`** — the nodes elect the leader themselves with a built-in [Raft](https://raft.github.io/) implementation, no external coordination service is needed. Recommended for new clusters.
- **`etcd`** (default) — an external [etcd](https://etcd.io/) cluster does leader election and stores the ISR. Kept so that existing clusters keep working unchanged when upgraded; they can [migrate to raft](#migrating-from-etcd-to-raft) when convenient.

A node never switches backend on its own, `backend` has to be changed by the operator.

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **Raft backend** — every node takes part in leader election over the raft port (`5680` by default). A majority of the configured peers must be reachable to elect a leader and to change the ISR.
- **etcd backend** — external coordination service for leader election, ISR tracking, and the shared replication secret.

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

### Raft backend

```ini
[clustering]
enabled = true
backend = raft
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
peers = node1.example.com:5680,node2.example.com:5680,node3.example.com:5680
raft_advertised_address = node1.example.com:5680
password_file = /etc/lavinmq/clustering_password
```

- `peers` lists the raft address of every node, including this one, and must be identical on all nodes. Three or five nodes are recommended: a cluster of `N` nodes keeps working with `(N - 1) / 2` nodes down. Without `peers` the node forms a cluster of one.
- `raft_advertised_address` is this node's entry in `peers`, `hostname:raft_port` by default.
- A password shared by all nodes is required. It authenticates both election traffic and followers replicating from the leader. Put it in a file owned by the lavinmq user with mode `0600` and point `password_file` at it; startup fails if the file is readable by group or others. It can also be given inline as `password` (or `LAVINMQ_CLUSTERING_PASSWORD`), but a config file is often world readable, so LavinMQ warns when it is.

  ```sh
  openssl rand -base64 32 > /etc/lavinmq/clustering_password  # copy the same file to every node
  chown lavinmq: /etc/lavinmq/clustering_password && chmod 600 /etc/lavinmq/clustering_password
  ```
- The raft listener binds to the same address as `bind`.
- When starting a new cluster of more than one node, start one of them with `--clustering-bootstrap` (or `LAVINMQ_CLUSTERING_BOOTSTRAP=true`) the first time. No node becomes leader without it, see [Migrating from etcd to raft](#migrating-from-etcd-to-raft) for why.

### etcd backend

```ini
[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
etcd_endpoints = etcd1:2379,etcd2:2379,etcd3:2379
etcd_prefix = lavinmq
```

The raft options (`peers`, `password_file`, `election_timeout`, ...) are ignored with the etcd backend.

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

### Raft backend

If the leader stops sending heartbeats for `election_timeout` (1500 ms by default), the other nodes elect a new one. A node only votes for a candidate that is in the ISR and whose election log is at least as recent as its own, so a node lacking confirmed data can never become leader. If no ISR member is reachable, no leader is elected until one comes back.

A new leader starts with an ISR of only itself; followers are added back as they finish syncing from it.

A leader that can't reach a majority of the peers for `election_timeout` steps down and exits (code 3), like when it loses leadership in any other way. A leader shutting down gracefully hands leadership over to a caught up ISR member right away instead.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `election_timeout` | `[clustering]` | `1500` | Milliseconds without a leader heartbeat before an election starts |
| `heartbeat_interval` | `[clustering]` | `250` | Milliseconds between leader heartbeats, at most half the election timeout |
| `bootstrap` | `[clustering]` | `false` | Let this node become leader before any node has election state, see below |

### etcd backend

If the leader fails, etcd coordinates leader election among ISR members. The first ISR member to successfully campaign becomes the new leader. A node that wins the election while no longer in the ISR (its candidacy was queued before it fell out of sync) steps down immediately — it releases its lease and exits so an in-sync candidate can win, and rejoins as a follower after re-syncing.

### Migrating from etcd to raft

The migration needs a short full cluster downtime. All nodes have to switch backend at the same time, a cluster can't run with both.

1. Stop all nodes, the followers first and the leader last, so the node with the most recent data is known.
2. Add `backend = raft`, `peers`, `raft_advertised_address` and `password_file` to every node's config and open the raft port between the nodes. `etcd_endpoints` and `etcd_prefix` can be removed, raft ignores them.
3. Start the former leader with `--clustering-bootstrap` (or `LAVINMQ_CLUSTERING_BOOTSTRAP=true`), and the other nodes normally.

A node without election state (`.raft_state` in the data dir) doesn't know whether its data is current, so it won't try to become leader until an elected leader has it in the in-sync replica set, i.e. once it has synced from that leader. That includes nodes with an empty data dir: if they could, two replaced nodes could outvote the one that still has the data, and it would then sync their empty state. `bootstrap` overrides that and lets the node become the cluster's first leader. Until the other nodes have synced, the bootstrapped node is the only one that can lead, so if it goes down the cluster waits for it to come back. It only has an effect while the node has no election state, but remove it once the cluster is up: if that node loses its data dir along with a majority of the others, it could otherwise start a new, empty cluster. A cluster of a single node needs no bootstrap.

To roll back to etcd, stop all nodes the same way, followers first and the leader last. Delete `{etcd_prefix}/isr` in etcd (`etcdctl del lavinmq/isr`), since it's from before the migration and may list nodes that are no longer in sync. Set `backend = etcd` again on every node and delete `.raft_state` from the data dirs. Then start the former leader first, and the other nodes once it has been elected.

### Leader Election Hooks

Shell commands can be executed on leadership transitions:

```ini
[clustering]
on_leader_elected = /usr/local/bin/update-dns.sh
on_leader_lost = /usr/local/bin/drain-connections.sh
```

### Leader Status Socket

For an agent on the same host that tracks which node is the leader, e.g. to
point a router at it, LavinMQ can stream its leader status over a unix socket:

```ini
[clustering]
status_unix_path = /run/lavinmq/clustering-status.sock
```

On connect the current state is written as one line, then one line on every
change. The connection stays open, clients don't send anything:

```
ready=0 leader=0 term=3 leader_uri=tcp://node1:5679 seq=4
ready=1 leader=1 term=4 leader_uri=tcp://node2:5679 seq=7
```

- `ready=1`: this node is the leader and accepts client connections, route
  traffic here. It turns `0` before connections are closed on shutdown or
  lost leadership.
- `leader`: this node's raft role, which turns `1` slightly before `ready`.
- `term`: the raft term. If two nodes report `ready=1` (a paused or
  partitioned old leader), trust the one with the highest term.
- `leader_uri`: the current leader's clustering URI, empty when unknown.
- `seq`: increases on every line from this process.

EOF (e.g. LavinMQ stopped or crashed) means the node isn't the leader.
Reconnect with a backoff. Values never contain whitespace, and new keys may
be added, so ignore keys you don't know.

```sh
socat -u UNIX-CONNECT:/run/lavinmq/clustering-status.sock - |
  while read -r line; do echo "$line"; done
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

With the raft backend, nodes authenticate each other with the shared password (`password_file` or `password`): raft connections with an HMAC-SHA256 challenge-response, and followers by sending it to the leader's replication port.

With the etcd backend, followers authenticate to the leader using a shared secret stored in etcd. The secret is randomly generated on first cluster initialization and stored under `{etcd_prefix}/clustering_secret`.

Clustering connections aren't encrypted, so keep clustering traffic on a trusted network.
