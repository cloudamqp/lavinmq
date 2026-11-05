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

### Replication Durability

For AMQP publisher confirms and MQTT QoS 1 PUBACKs, the leader requests synchronization of the affected files before waiting for acknowledgments from every in-sync follower. A follower synchronizes the requested files before acknowledging past that point in the replication stream. If a follower leaves the ISR during the wait, the leader commits that membership change before confirming to the publisher, so a node that missed the confirmed data cannot be promoted.

Replication protocol version 2 carries these synchronization requests, allowing followers to sync only the requested files instead of flushing the whole filesystem before every acknowledgment. A whole-filesystem sync is requested for transaction commits, and a follower also falls back to it when the number of requested files exceeds its own `syncfs_threshold` setting in `[main]` (default `64`).

Replication protocol version 3 adds a challenge-response: instead of sending the clustering password, both sides prove they know it with an HMAC-SHA256 over a nonce from each. The follower checks the leader's proof before it sends anything else, so it never follows, or deletes data for, something at the leader's address that doesn't know the password. A follower whose handshake fails logs it and retries, waiting twice as long each time, up to a minute. The replication stream itself isn't encrypted, so the network between the nodes should be trusted.

Version 1 and 2 peers, such as 2.11.0-rc.1 nodes, remain compatible, so a cluster can be upgraded one node at a time. They send the password, and version 1 connections use the previous behavior: the follower syncs the whole filesystem before each acknowledgment. The protocol version is negotiated automatically: a follower tries version 3 with each new leader, and falls back to the version an older leader answers with.

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

If the leader fails, etcd coordinates leader election among ISR members. A failed leader is detected when its etcd lease expires. The leader renews the lease every third of `etcd_lease_ttl` (default 10 seconds), so after a crash the lease expires between two thirds of `etcd_lease_ttl` and `etcd_lease_ttl` seconds later. A leader that pauses longer than two thirds of `etcd_lease_ttl`, for example in a long GC pause, can lose its leadership. A leader that loses its lease stops serving without exiting: it disconnects its followers first, so nothing more is confirmed, then its clients, and continues as a follower of the new leader in the same process. It exits with code 3 only if stopping to serve takes longer than 60 seconds. The first ISR member to successfully campaign becomes the new leader. A node that wins the election while no longer in the ISR (its candidacy was queued before it fell out of sync) steps down immediately — it releases its lease and exits so an in-sync candidate can win, and rejoins as a follower after re-syncing.

### Leader Election Hooks

Shell commands can be executed on leadership transitions:

```ini
[clustering]
on_leader_elected = /usr/local/bin/update-dns.sh
on_leader_lost = /usr/local/bin/drain-connections.sh
```

## Clustering Proxy

Every node binds the client ports when it starts and keeps them bound through leadership changes. A node that serves handles the connections itself, a follower forwards them to the current leader, so clients can connect to any node on the normal protocol ports without needing to know which node is the leader. Connections a node forwarded to the old leader are closed when it starts to serve itself, so their clients reconnect to it. While a node neither serves nor knows the leader, such as during an election or while a new leader loads its data, it doesn't accept connections: they wait in the listen backlog until it can serve or forward them.

Forwarding is transparent and covers:

- AMQP and AMQPS (TCP and Unix socket)
- MQTT and MQTTS (TCP and Unix socket)
- HTTP/management, HTTPS and WebSockets (TCP and Unix socket)

The TLS ports (`amqps_port`, `mqtts_port`, `https_port`) are bound when TLS is configured (`tls_cert_path` is set) and the port is above 0. TLS is forwarded as is: the follower doesn't terminate it, the client's TLS session is with the leader, so the certificate has to be valid for every node's name, and SNI and kTLS are handled by the leader. Unix-socket forwarding activates per protocol when the matching `unix_path` is configured in `[amqp]`, `[mqtt]`, or `[mgmt]`, and goes to the leader's TCP port of that protocol; Unix sockets are always plaintext. Connections go to the same ports on the leader, so the client ports should be the same on all nodes.

For AMQP and MQTT, with or without TLS, the follower sends a PROXY protocol v1 header first, which the leader reads before the TLS handshake, so it sees the original client address. HTTP and HTTPS connections get no PROXY header. No further configuration is needed.

## Security

Followers authenticate to the leader using a shared secret stored in etcd. The secret is randomly generated on first cluster initialization and stored under `{etcd_prefix}/clustering_secret`.
