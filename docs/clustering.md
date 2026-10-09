# Clustering

LavinMQ supports multi-node clustering with leader-based replication. Leader election and the in-sync replica set (ISR) are kept by one of two backends:

- **`raft`** (when `seeds` are set) — the nodes elect the leader themselves with a built-in [Raft](https://raft.github.io/) implementation, no external coordination service is needed. Recommended for new clusters.
- **`etcd`** (otherwise) — an external [etcd](https://etcd.io/) cluster does leader election and stores the ISR. Kept so that existing clusters keep working unchanged when upgraded; they can [migrate to raft](#migrating-from-etcd-to-raft) when convenient.

The backend follows from the config: setting `seeds`, which raft requires and etcd ignores, selects raft. `backend = etcd` or `backend = raft` in `[clustering]` overrides that, e.g. to keep etcd while `seeds` are already in place for a migration. A node never switches backend while it runs.

## Architecture

- **Leader** — accepts all client connections and writes. Replicates data to followers.
- **Followers** — receive replicated data from the leader. Can be promoted to leader on failover.
- **Raft backend** — every node takes part in leader election over the raft port (`5680` by default). A majority of the voting members must be reachable to elect a leader and to change the ISR.
- **etcd backend** — external coordination service for leader election, ISR tracking, and the shared replication secret.

Only the leader handles client traffic. Followers maintain a synchronized copy of the data.

## Enabling Clustering

### Raft backend

```ini
[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://node1.example.com:5679
seeds = node1.example.com,node2.example.com,node3.example.com
password = replace-with-your-shared-secret
```

- `seeds` are the raft addresses to form or join a cluster with. The port defaults to `5680`; set it explicitly for a member using another port, e.g. `node1.example.com:5690`. IPv6 addresses use brackets, e.g. `[::1]` or `[::1]:5690`. The default seed port is independent of this node's `raft_port`. When a new cluster is started, list every node: the first leader makes them its voters, so the list sets the quorum. A node joining an existing cluster only needs one member. Once a node has the membership from the raft log, that is used, and `seeds` are only read again when starting from scratch. Three or five voters are recommended: a cluster of `N` voters keeps working with `(N - 1) / 2` of them down. `seeds` is required, so that a node left out of the config doesn't silently form a cluster of its own; a single node cluster lists only its own address.
- `advertised_uri` is the URI followers replicate from: `tcp://hostname:port` when `bind` is all interfaces (`::` or `0.0.0.0`), otherwise `tcp://bind:port`.
- `raft_advertised_address` is the address other nodes reach this node's raft port at, by default the host of `advertised_uri` with `raft_port`, so it rarely needs to be set. Use a stable name in `advertised_uri`: if the address changes, e.g. with the hostname of a recreated container, and that happens to most nodes at once, the cluster is without a leader for a few election timeouts (see [Changing a node's address](#changing-a-nodes-address)).
- A password shared by all nodes is required. It authenticates both election traffic and followers replicating from the leader. Set `password` in `[clustering]`, `LAVINMQ_CLUSTERING_PASSWORD`, or `--clustering-password`. CLI overrides environment, which overrides the config file. The password must be nonempty and at most 255 bytes. Protect any config file or environment containing the password; command-line passwords may be visible in process listings.
- Alternatively, put the password in a file and point `password_file` at it. If both `password` and `password_file` are set, the file takes precedence. Mode `0600` is recommended; startup warns if the file is accessible by group or others, but still loads it. An unreadable or missing file still prevents startup.

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

The raft options (`seeds`, `password`, `password_file`, `election_timeout`, ...) are ignored with the etcd backend.

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

### Raft backend

If the leader stops sending heartbeats for `election_timeout` (1500 ms by default), the other nodes elect a new one. A node only votes for a candidate that is in the ISR and whose election log is at least as recent as its own, so a node lacking confirmed data can never become leader. If no ISR member is reachable, no leader is elected until one comes back.

A new leader starts with an ISR of only itself; followers are added back as they finish syncing from it.

A leader that can't reach a majority of the voters for `election_timeout` steps down, like when it loses leadership in any other way. It disconnects its followers first, so nothing more is confirmed, then stops serving clients and continues as a follower in the same process: its raft node keeps running, so it can vote and be elected again without a restart. If stopping to serve fails or takes longer than 60 seconds, the node exits (code 3) instead. A leader shutting down gracefully hands leadership over to a caught up ISR member right away.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `election_timeout` | `[clustering]` | `1500` | Milliseconds without a leader heartbeat before an election starts |
| `heartbeat_interval` | `[clustering]` | `250` | Milliseconds between leader heartbeats, at most half the election timeout |
| `bootstrap` | `[clustering]` | `false` | Let this node become leader before any node has election state, see below |

A node that has lost its election state (`.raft_state`) but kept its clustering id only votes for a candidate that has no election state either, as when a cluster is first bootstrapped. It can't tell whether it already voted in the current term, and voting twice could elect two leaders. It votes normally again once it has the log from a leader.

`lavinmqctl cluster_status` (or `GET /api/cluster`, followers proxy it to the leader) shows the leader, the ISR and how far each member has caught up. When there's no leader, `lavinmqctl cluster_status` run on a node without `--uri` answers over that node's control socket with its own view: its role and term, the leader it last knew of, and the committed ISR and membership it has.

Every node exposes its raft state as Prometheus metrics, also while there's no leader, see [Raft metrics](monitoring.md#raft).

#### Recovering when no ISR member can come back

Only an ISR member can become leader, so if all of them are lost for good, no leader is elected. To recover from the remaining nodes, accepting that messages only the lost nodes had confirmed are gone:

1. Check each remaining node's view with `lavinmqctl cluster_status` and pick the node with the most recent data, e.g. the one that was in the ISR most recently.
2. Stop all remaining nodes.
3. Set `seeds` on each of them to only the remaining nodes. A bootstrapping node counts every seed towards the quorum.
4. Delete `.raft_state` from the data dir of every remaining node, not only the chosen one: nodes with election state wouldn't vote for a node that isn't in their ISR.
5. Start the chosen node with `--clustering-bootstrap`, then the others. They sync the broker data from it, discarding what they have that it doesn't.
6. Remove `bootstrap` again, and add replacement nodes as described in [Relocating a replica](#relocating-a-replica).

### etcd backend

If the leader fails, etcd coordinates leader election among ISR members. The first ISR member to successfully campaign becomes the new leader. A node that wins the election while no longer in the ISR (its candidacy was queued before it fell out of sync) steps down immediately — it releases its lease and exits so an in-sync candidate can win, and rejoins as a follower after re-syncing.

### Migrating from etcd to raft

The migration needs a short full cluster downtime. All nodes have to switch backend at the same time, a cluster can't run with both.

1. Stop all nodes, the followers first and the leader last, so the node with the most recent data is known.
2. Add `seeds` (every node, which selects raft) and `password` (or `password_file`) to every node's config and open the raft port between the nodes. `etcd_endpoints` and `etcd_prefix` can be removed, raft ignores them.
3. Start the former leader with `--clustering-bootstrap` (or `LAVINMQ_CLUSTERING_BOOTSTRAP=true`), and the other nodes normally.

A node without election state (`.raft_state` in the data dir) doesn't know whether its data is current, so it won't try to become leader until an elected leader has it in the in-sync replica set, i.e. once it has synced from that leader. That includes nodes with an empty data dir: if they could, two replaced nodes could outvote the one that still has the data, and it would then sync their empty state. `bootstrap` overrides that and lets the node become the cluster's first leader. Until the other nodes have synced, the bootstrapped node is the only one that can lead, so if it goes down the cluster waits for it to come back. It only has an effect while the node has no election state, but remove it once the cluster is up: if that node loses its data dir along with a majority of the others, it could otherwise start a new, empty cluster. A cluster of a single node needs no bootstrap.

To roll back to etcd, stop all nodes the same way, followers first and the leader last. Delete `{etcd_prefix}/isr` in etcd (`etcdctl del lavinmq/isr`), since it's from before the migration and may list nodes that are no longer in sync. Remove `seeds`, or set `backend = etcd`, on every node and delete `.raft_state` from the data dirs. Then start the former leader first, and the other nodes once it has been elected.

### Changing the cluster membership

With the raft backend the membership is kept in the Raft log, so nodes are added, promoted and removed at runtime with the operations below. Editing `seeds` doesn't change the membership: they're only used to start a new cluster and to let a joining node find it. If they differ from the committed membership, the node logs that it uses the membership.

Changes are made one server at a time and only on the leader, and the leader only has one uncommitted change at a time (a second one gets `409`). A node can be:

- a **learner**: it gets the Raft log and replicates broker data like a follower, so it can end up in the ISR, but it doesn't vote, count towards commit or quorum, or campaign. Adding one doesn't change the quorum.
- a **voter**: a promoted learner. It can only be promoted once it is in the ISR (has the broker data) and has caught up in the Raft log.

Nodes are identified by the `.clustering_id` in their data dir, the membership records where to reach each of them. `add_cluster_member` takes the new node's raft address and asks the node for its id, so the node must be running. The other operations take either the address or the id (as shown by `cluster_status`).

Removing a node also removes it from the ISR in the same entry, so a removed node that missed it can still never win an election. The leader can't be removed, transfer leadership first. Non-members are refused when they try to replicate broker data, and a removed node's replication connection is closed.

The operations are available in `lavinmqctl` (`cluster_status`, `add_cluster_member`, `promote_cluster_member`, `remove_cluster_member`, `transfer_leadership`) and in the HTTP API under `/api/cluster` (administrator only, see the OpenAPI docs). With the etcd backend they return `400`.

### Transferring leadership

`lavinmqctl transfer_leadership --target <address>` hands leadership to a chosen voter that is in the ISR (without `--target`, any caught up one). The leader stops serving clients, tells the target to take over, and continues as a follower of the target in the same process. Its raft node keeps running throughout, so the new leader keeps its quorum also when only a majority of the voters is up. If the target doesn't take over within two election timeouts, the leader serves again. The HTTP request returns `202` before that, so it only means the transfer was accepted. Followers proxy HTTP to the leader and a leader starts HTTP once it is serving, so `GET /api/cluster` answering with `leader` set to the target means the target leads and serves. `--wait` polls for that.

### Relocating a replica

To move a replica from node A to a new node D:

1. Start D with `seeds` listing at least one existing member, the same shared password, and no `bootstrap`. It doesn't campaign with an empty log. **Don't** list only D in `seeds`, a node that is its only seed bootstraps a new cluster of its own.
2. `lavinmqctl add_cluster_member <D>`. D gets the Raft log and syncs the broker data. `cluster_status` shows `in_isr` for D when it is done.
3. `lavinmqctl promote_cluster_member <D>`.
4. `lavinmqctl transfer_leadership --target <D> --wait` if A is the leader. A continues as a follower.
5. `lavinmqctl remove_cluster_member <A>`, then shut A down and wipe its data dir before reusing it. A removed node logs that it isn't a member and keeps retrying until it is stopped.

### Changing a node's address

To give a node a new address but keep its data dir, restart it with the new `advertised_uri` (or `raft_advertised_address`, if set). When it connects from there, the leader makes it a learner at the new address and promotes it back once it has synced and caught up again. Meanwhile the cluster has one voter less: in a three node cluster the other two must both be up. A node at a new address doesn't campaign until the leader has moved it.

Moving a voter takes a majority of the voters at their old addresses, so if most of them come back at new addresses together, e.g. after changing `raft_port` on every node or when the hostnames change and neither `advertised_uri` nor `raft_advertised_address` is set, no leader can be elected the normal way. After three election timeouts without a leader the nodes count each other's votes wherever they are and log a warning that they do. The leader elected then records the new addresses in the membership, without demoting anyone, and the normal rules apply again. Until then the cluster is unavailable, about four election timeouts (6 s by default) plus the election itself, so prefer moving one node at a time.

That shortcut trusts that a node connecting from a new address with a known clustering id is that node, moved, and not a copy of its data dir running next to the original. Never run a node and a copy of it at the same time; with a leader around the copy is refused, but without one for long enough both could be counted. If the cluster still doesn't elect a leader, for example because the nodes in the ISR are gone, see [Recovering when no ISR member can come back](#recovering-when-no-isr-member-can-come-back).

Copying a data dir to start another node also copies its clustering id. While the original is connected, the copy is refused with a log about a clustering id conflict. Delete `.clustering_id` on the copy to give it an id of its own. If the original is down, the copy is taken for the node having moved, and the original is refused when it comes back.

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

With the raft backend, nodes authenticate each other with the shared password from `password` or `password_file`: both raft connections and followers connecting to the leader's replication port answer an HMAC-SHA256 challenge, so the password is never sent over the network. A raft follower refuses to authenticate to a leader that only speaks the older replication protocol, which sent the password in clear text.

With the etcd backend, followers authenticate to the leader the same way, using a shared secret stored in etcd. The secret is randomly generated on first cluster initialization and stored under `{etcd_prefix}/clustering_secret`.

Clustering connections aren't encrypted, so keep clustering traffic on a trusted network.
