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

Give every node the same seed list to let them form the cluster automatically, with no manual per-node join step:

```ini
[clustering]
enabled = true
backend = raft
advertised_uri = tcp://node1.example.com:5679
seed_uris = http://node1.example.com:15672,http://node2.example.com:15672,http://node3.example.com:15672
```

The same list goes on every node, including the node itself. Joining is a `POST /raft/admin/add_server` call authenticated with the shared clustering secret: each node sends its `.clustering_password` automatically, so the seed URIs carry no credentials and formation never depends on the management user database. Equivalent forms:

| Method | Syntax |
|--------|--------|
| INI (`[clustering]`) | `seed_uris = http://node1:15672,http://node2:15672,http://node3:15672` |
| CLI flag | `--clustering-seed-uris=http://node1:15672,http://node2:15672,http://node3:15672` |

There is no environment variable for `seed_uris`.

**How formation works:** when a node boots with a seed list and no existing cluster state, it compares its advertised host against the seed hosts. The node whose advertised host sorts lexicographically lowest bootstraps a single-node cluster — but only after a short bounded check that no seed is already serving one (see the recovery runbook below); the rest join it. With the shared secret in place (see the prerequisite below), boot all nodes simultaneously and the cluster forms with no manual steps.

**Prerequisite — distribute the replication secret first.** Followers authenticate to the leader with a shared secret. On the raft backend this secret lives in `<data_dir>/.clustering_password` and is **not** replicated between nodes. Generate one secret and place the *identical* `.clustering_password` (mode `0600`) in every node's data directory **before** booting:

```sh
openssl rand -base64 32 > /var/lib/lavinmq/.clustering_password
chmod 0600 /var/lib/lavinmq/.clustering_password
# copy the same file to every node (config management, a mounted secret, etc.)
```

If you skip this, only the node that bootstraps will have a secret (it auto-generates one), and every other node logs a fatal "Replication secret file missing" error and exits before it even attempts to join — until you copy that file to them. A node whose file holds a *different* secret gets its join rejected and exits with a fatal error rather than retrying. A node that already has the correct file reads it as-is and never generates a new one, so pre-placing the same secret everywhere is safe.

**`clustering_advertised_uri` must match a seed entry.** Each node identifies itself in the seed list by its advertised host. If a node's advertised host does not match any seed host, it will always attempt to join (never bootstrap) — a safe failure, but the cluster won't form if the lowest-host node never identifies itself.

**Initial formation requires the lowest-host node.** If the node with the lexicographically lowest host in the seed list is down at boot time, no node will bootstrap; the others retry joining until it appears. Once the cluster exists, that node is an ordinary member with no special role.

#### Recovery runbook

To rebuild a node that has lost or diverged its raft state:

1. Run `lavinmqctl raft_reset` on the affected node. This wipes raft state from the data directory and, if a node is running, signals it to exit. The command **fails closed**: it proceeds without `--force` only when it can prove the node is safe to discard — a running node whose `/raft/status` reports a single-node cluster (≤1 peer). A node that is **stopped** (no control socket / unreachable), a follower, in a multi-peer cluster, or whose status omits the peer list cannot be verified, so it requires `--force` to override the guard. (Most recovery scenarios involve a stopped or unhealthy node, so expect to pass `--force`.)
2. Restart lavinmq. If `seed_uris` is configured, the node rejoins automatically — including the lexicographically-lowest-host node: before bootstrapping a new cluster, it briefly probes the seed list, and if any seed is already serving (the normal case when only this one node was reset), it joins that cluster instead. No special-cased command or manual marker is needed for this node; the runbook above is the same for every node in the cluster.

#### Known limitation — recovering the lowest-host node during a full outage

If the lexicographically-lowest-host node is reset (step 1 above) **at the same time** the rest of the cluster has also lost quorum (no leader reachable anywhere), the boot-time probe will get no answer from any seed — an unreachable-everywhere cluster can't be probed by HTTP, the same inherent blind spot described in the ISR limitation below — and the node will bootstrap a fresh single-node cluster instead of waiting to rejoin the original one. This only affects the coincidence of losing this specific node's state *and* the whole cluster's quorum simultaneously; recovering the lowest-host node while the rest of the cluster is healthy (the common case) is fully automatic.

#### Known limitation — ISR commits under quorum loss

On the raft backend, the in-sync replica set (ISR) is committed through raft consensus while the leader holds its replication-dispatch lock. If the raft cluster loses quorum (e.g. two of three nodes down), an ISR commit blocks until leadership is lost (an election timeout), which can stall the data plane for that window — even when leader→follower data replication is otherwise healthy. This is not a regression from the etcd backend, which commits the ISR under the same lock (with a bounded single PUT); it is a worse latency profile specific to raft's blocking consensus. Moving the ISR commit out from under that lock is tracked as a follow-up.

### Migrating from the etcd backend to the raft backend

An existing etcd-coordinated cluster can be switched to the raft backend in place: the data directories, node identities (`.clustering_id`) and message data all carry over — only the coordination backend changes. The migration reads etcd but never writes to it, so it can be rolled back.

The cutover requires a **full-cluster stop**. The order in which nodes are stopped and started carries the safety guarantees, so follow the steps exactly.

> **Warning — never mix backends.** A node started with `backend = raft` while other nodes still run the etcd backend forms a second, independent cluster and serves stale data alongside the etcd leader (split-brain). There is no built-in fence against this; the full stop in step 3 is what prevents it.

**Prerequisites**

- All nodes run a raft-capable LavinMQ release. Upgrade the binaries first with a normal rolling upgrade while still on the etcd backend; switch backends as a separate step.
- etcd stays reachable until the cutover is complete (the secret is read from it in step 1).
- `etcdctl` pointed at the cluster's `etcd_endpoints`; the key prefix below is the default `etcd_prefix` (`lavinmq`).

#### Step 1 — distribute the replication secret

On the raft backend the shared replication secret lives in `<data_dir>/.clustering_password` (see [Security](#security)). Copy it out of etcd on **every node**:

```sh
etcdctl get --print-value-only lavinmq/clustering_secret > /var/lib/lavinmq/.clustering_password
chmod 0600 /var/lib/lavinmq/.clustering_password
```

Verify the file has identical content on all nodes.

#### Step 2 — identify the current leader and prepare the configs

Find the current leader — the election value is its `advertised_uri`. The command streams leader changes, so bound it with `timeout`:

```sh
timeout 2 etcdctl elect --listen lavinmq/leader
# lavinmq/leader/694d89b0c464e40a
# tcp://node2.example.com:5679
```

Then update the configs on all nodes, but do not restart anything yet: set `backend = raft`, keep `advertised_uri`, and open the raft port (default 5680) between the nodes. On the **followers** also set `seed_uris` to every node's management URI — joins authenticate automatically with the `.clustering_password` distributed in step 1, so the URIs carry no credentials:

```ini
[clustering]
enabled = true
backend = raft
advertised_uri = tcp://node1.example.com:5679
seed_uris = http://node1.example.com:15672,http://node2.example.com:15672,http://node3.example.com:15672
```

On the **leader**, leave `seed_uris` unset for now — an empty seed list is what makes it bootstrap the new cluster in step 4.

#### Step 3 — stop the cluster: followers first, leader last

Stop each follower, then the leader. The order matters: clients keep writing to the leader between follower stops, so earlier-stopped nodes can be missing the newest data. The last-stopped leader is guaranteed to hold every message that has been confirmed to a client — which is why it must also be the node that forms the new cluster. Starting the migration from any other node can silently discard the confirmed tail (rejoining nodes sync *from* the bootstrapper, deleting what it lacks).

#### Step 4 — start the old leader, alone

With no `seed_uris` it bootstraps a single-node raft cluster and begins serving clients immediately. Verify:

```sh
lavinmqctl raft_status
# Role:         leader
```

#### Step 5 — start the followers

They probe the seed list, join the leader and perform a full data sync. Until the first follower has finished syncing, the raft ISR is empty and treats every voter as in-sync — a leader crash in that window could elect a node without data — so start the followers promptly after step 4.

Verify all nodes joined (`lavinmqctl raft_status` lists every node id under `Peers:`) and re-entered the ISR (the leader logs `In-sync replicas: [...]` with every node id).

#### Step 6 — post-migration cleanup

- Add `seed_uris` to the bootstrapper's config (same list as the others). It resumes from its raft state, so the setting only matters for future recovery — but make sure its `advertised_uri` host appears in the seed list.
- Remove `etcd_endpoints` and `etcd_prefix` from all configs.
- Decommission etcd.

From here on, node recovery follows the [recovery runbook](#recovery-runbook).

#### Rolling back

The migration never writes to etcd, so the cluster can return to it: stop the raft nodes (if clients already wrote to the raft cluster, stop followers first and the leader last, for the same reason as step 3), revert `backend = etcd` and restore `etcd_endpoints`, run `lavinmqctl raft_reset --force` on every node that formed raft state (wipes the `raft/` directories; message data and `.clustering_id` are kept), and start the nodes. Leftover `.clustering_password` files are harmless under the etcd backend.

#### If something goes wrong

| Symptom | Cause / action |
|---------|----------------|
| A node exits with `Replication secret file missing or empty` | Step 1 was skipped or wrote a different secret on that node — re-copy `.clustering_password` |
| A follower keeps logging failed join attempts | The bootstrapper isn't serving yet, `seed_uris` is wrong, or the management port is unreachable — it retries indefinitely, so fix the cause and wait |
| A node exits with `rejected our clustering password` | Its `.clustering_password` differs from the cluster's — redo step 1 on that node and restart it |
| Two nodes serve clients at once | Backends were mixed, or a follower was started with an empty `seed_uris` and bootstrapped its own cluster. Stop the extra "leader", run `lavinmqctl raft_reset --force` on it, give it the full `seed_uris`, and start it again — it rejoins and resyncs |

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
- **`raft`** — stored in each node's `<data_dir>/.clustering_password` and **not** replicated between nodes. The bootstrapping node auto-generates one (mode `0600`) if absent; every other node must already have the *same* file or it exits when it tries to join. The same secret also authenticates cluster-membership changes: the mutating `/raft/admin/*` endpoints on the management port accept it as the basic-auth password (any username). Distribute the identical secret to all nodes before forming the cluster — see [Declarative cluster formation with `seed_uris`](#declarative-cluster-formation-with-seed_uris) for the prerequisite and the generation command. When migrating from the etcd backend, populate the file from the existing etcd secret instead — see [Migrating from the etcd backend to the raft backend](#migrating-from-the-etcd-backend-to-the-raft-backend).
