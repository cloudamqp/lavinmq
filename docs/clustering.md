<!-- prettier-ignore-start -->

LavinMQ 2.0 and above supports clustering to ensure high availability for your messaging infrastructure. It achieves this by integrating with etcd, a distributed key-value store based on the [Raft consensus algorithm](https://lavinmq.com/blog/lavinmq-high-availability). Consequently, LavinMQ can now run in either cluster or standalone mode. This configuration can be adjusted in the `/etc/lavinmq/lavinmq.ini` file.

## What is clustering in LavinMQ?

Clustering in LavinMQ creates a leader node and one or more follower nodes. The leader tracks changes—like published messages, acknowledgments, and metadata updates—by recording them in a log. This log, which represents the shared state as a sequence of actions, is replicated to follower nodes in real-time. LavinMQ ensures that these actions are consistently processed across the entire cluster.

![Clustering in LavinMQ](img/docs/illustration-1-3.svg)

## When can I use clustering?

This feature is ideal for minimizing downtime and prioritizing data safety. By distributing data across multiple nodes, follower nodes can immediately take over if the leader node fails.

## Benefits of clustering

With clustering, LavinMQ offers key benefits:

- <b>Data Redundancy:</b> Messages are replicated to follower nodes, safeguarding against data loss.

- <b>High Availability:</b> Automatic leader election enables quick recovery from failures, reducing disruptions.

## How to set up clustering in LavinMQ

While LavinMQ checks for the possibility of becoming a leader, etcd handles the actual leader election process. LavinMQ and etcd can be on separate machines, allowing you to have a dedicated etcd cluster that supports multiple LavinMQ clusters or nodes. Alternatively, you can set up etcd on each node in your LavinMQ cluster.

This guide explains how to set up etcd on each LavinMQ node.

### Step 1: Decide on the number of nodes in your cluster.

Before setting up your cluster, determine the number of nodes you will include. The recommended minimum for a fault-tolerant cluster is three nodes, and we recommend using an odd number of nodes.

### Step 2: Download and install the etcd binary

Next, download and install the etcd binary on each node in your cluster. You can find the latest etcd releases [here](https://github.com/etcd-io/etcd/releases){:target="\_blank"}. LavinMQ requires at least etcd version 3.4.

We also provide `.deb` packages for simple installation.

```
curl -fsSL https://packagecloud.io/cloudamqp/etcd/gpgkey | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/cloudamqp_etcd.gpg
. /etc/os-release
echo "deb https://packagecloud.io/cloudamqp/etcd/any any main" | sudo tee /etc/apt/sources.list.d/cloudamqp_etcd.list
apt-get update
apt-get install etcd
```

### Step 3: Configure etcd nodes

You need to specify the cluster members on each etcd node. This configuration step enables the nodes to recognize and communicate with each other, forming the cluster. See the code snippet below for an example etcd config for the first node of a three-node cluster.

`/etc/etcd/etcd.conf.yml`

```
name: my-server-name-01
data-dir: /var/lib/etcd/
listen-peer-urls: http://0.0.0.0:2380
listen-client-urls: http://0.0.0.0:2379
initial-advertise-peer-urls: http://my-server-name-01.cloudamqp.com:2380
advertise-client-urls: http://my-server-name-01.cloudamqp.com:2379
initial-cluster-token: my-cluster-token
initial-cluster-state: new
initial-cluster: my-server-name-01=http://my-server-name-01.cloudamqp.com:2380,my-server-name-02=http://my-server-name-02.cloudamqp.com:2380,my-server-name-03=http://my-server-name-03.cloudamqp.com:2380
```

For the second and third nodes in the cluster, change the `initial-cluster-state` to existing. Also, change name, `initial-advertise-peer-urls`, and `advertise-client-urls` to match your node.

### Step 4: Install LavinMQ

LavinMQ releases are hosted in packagecloud. First, you will need a key to access the repository. Execute the following commands in a terminal:

```
curl -fsSL https://packagecloud.io/cloudamqp/lavinmq/gpgkey | gpg --dearmor | sudo tee /usr/share/keyrings/lavinmq.gpg > /dev/null
. /etc/os-release
echo "deb [signed-by=/usr/share/keyrings/lavinmq.gpg] https://packagecloud.io/cloudamqp/lavinmq/$ID $VERSION_CODENAME main" | sudo tee /etc/apt/sources.list.d/lavinmq.list
```

Now, LavinMQ is ready to be installed. Execute the following commands in a terminal:

```
sudo apt-get update
sudo apt-get install lavinmq
```

[Read our LavinMQ installation guide](https://lavinmq.com/documentation/installation-guide) for more information.

### Step 5: Configure LavinMQ

You need to enable clustering in the LavinMQ config. You can do this either by updating your `lavinmq.ini` config or by providing options when starting LavinMQ.

```
[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
advertised_uri = tcp://my-server-name-01.internal.cloudamqp.com:5679
```

<div class="mt-4"></div>

```
lavinmq
--clustering
--clustering-bind=0.0.0.0
--clustering-port=5679
--clustering-advertised-uri=tcp://my-server-name-01.internal.cloudamqp.com:5679
```

<b>Note:</b> LavinMQ by default looks for etcd at `127.0.0.1:2379`. However, that can be changed by providing `--clustering-etcd-endpoints` when starting LavinMQ or setting `etcd_endpoints` in the `clustering` segment of the LavinMQ config.

#### etcd Authentication and Security

LavinMQ supports authenticated and encrypted connections to etcd for secure cluster coordination:

- **Basic Auth:** Include credentials in etcd_endpoints using the format `user:password@host:port`
- **HTTPS/TLS:** Use the `https://` prefix in etcd_endpoints for encrypted connections
- **Example with Basic Auth:** `etcd_endpoints = admin:secret@localhost:2379`
- **Example with HTTPS:** `etcd_endpoints = https://secure-etcd.example.com:2379`

These security features ensure that cluster coordination traffic is protected and only authorized LavinMQ nodes can participate in the cluster.

### Step 6: Start etcd and let LavinMQ connect.

LavinMQ nodes query etcd for the current cluster status. Start by bringing the etcd nodes online. You can pass a config file to etcd, otherwise default configurations will be used.

```
etcd --config-file=/path-to-config-file
```

Once the etcd cluster is running, start the LavinMQ nodes. You can also pass a config file to LavinMQ:

```
lavinmq --config-file=/path-to-config-file
```

On starting, a LavinMQ node checks if it can become the leader. If a leader already exists, the node enters follower mode and begins replicating data from the leader.

### Step 7: Start sending data.

Time to start publishing messages to your LavinMQ cluster. [Get started with LavinMQ](https://lavinmq.com/documentation/getting-started) using the various resources we have curated for you.

### Optional: Configure your cluster's DNS

We recommend configuring your DNS to point to all nodes in your cluster. If a client connects to a follower, LavinMQ will forward that traffic to the leader node.

By using etcd with LavinMQ 2.0, you can create a reliable and highly-available messaging system. Once set up, your cluster will handle server outages and keep your data consistent. This way, LavinMQ ensures your messaging infrastructure is fault tolerant.

<!-- prettier-ignore-end -->
