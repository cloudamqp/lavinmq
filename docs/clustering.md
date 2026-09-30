# Clustering

LavinMQ supports multi-node clustering with leader-based replication. Leader election and the in-sync replica set are handled by the nodes themselves, with a built-in [Raft](https://raft.github.io/) implementation; no external coordination service is needed.

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **Raft** — every node takes part in leader election over the raft port (`5680` by default). A majority of the configured peers must be reachable to elect a leader and to change the ISR.

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

```ini
[clustering]
enabled = true
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

### Replication Durability

For AMQP publisher confirms and MQTT QoS 1 PUBACKs, the leader requests synchronization of the affected files before waiting for acknowledgments from every in-sync follower. A follower synchronizes the requested files before acknowledging past that point in the replication stream. If a follower leaves the ISR during the wait, the leader commits that membership change before confirming to the publisher, so a node that missed the confirmed data cannot be promoted.

Replication protocol version 2 carries these synchronization requests, allowing followers to sync only the requested files instead of flushing the whole filesystem before every acknowledgment. A whole-filesystem sync is requested for transaction commits, and a follower also falls back to it when the number of requested files exceeds its own `syncfs_threshold` setting in `[main]` (default `64`).

Version 1 peers remain compatible. Version 1 connections use the previous behavior: the follower syncs the whole filesystem before each acknowledgment. The protocol version is negotiated automatically.

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

If the leader stops sending heartbeats for `election_timeout` (1500 ms by default), the other nodes elect a new one. A node only votes for a candidate that is in the ISR and whose election log is at least as recent as its own, so a node lacking confirmed data can never become leader. If no ISR member is reachable, no leader is elected until one comes back.

A new leader starts with an ISR of only itself; followers are added back as they finish syncing from it.

A leader that can't reach a majority of the peers for `election_timeout` steps down and exits (code 3), like when it loses leadership in any other way. A leader shutting down gracefully hands leadership over to a caught up ISR member right away instead.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `election_timeout` | `[clustering]` | `1500` | Milliseconds without a leader heartbeat before an election starts |
| `heartbeat_interval` | `[clustering]` | `250` | Milliseconds between leader heartbeats, at most half the election timeout |
| `bootstrap` | `[clustering]` | `false` | Let this node become leader before any node has election state, see below |

### Migrating from etcd

Earlier versions used etcd for leader election. `etcd_endpoints` and `etcd_prefix` are still accepted but ignored. To migrate:

1. Stop all nodes, the followers first and the leader last, so the node with the most recent data is known.
2. Add `peers`, `raft_advertised_address` and `password_file` to every node's config and open the raft port between the nodes.
3. Start the former leader with `--clustering-bootstrap` (or `LAVINMQ_CLUSTERING_BOOTSTRAP=true`), and the other nodes normally.

A node that has data but no election state (`.raft_state` in the data dir) doesn't know whether its data is current, so it won't try to become leader until it has heard from an elected one. `bootstrap` overrides that and lets it become the cluster's first leader. It only has an effect while the node has no election state, so leaving it set afterwards is harmless. Nodes with an empty data dir, and a cluster of a single node, need no bootstrap.

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

Nodes authenticate each other with the shared password (`password_file` or `password`): raft connections with an HMAC-SHA256 challenge-response, and followers by sending it to the leader's replication port. Neither connection is encrypted, so keep clustering traffic on a trusted network.
