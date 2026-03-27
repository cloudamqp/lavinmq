
Generally, monitoring is the process of tracking a system's behavior through
health checks and metrics over time. It helps detect anomalies, such as
unavailability, unusual loads, resource exhaustion, or deviations from normal
behavior. This ensures early identification of issues for prompt action to maintain
optimal performance and stability.

While the LavinMQ's management interface provides an easy to use built-in monitoring
solution, it is not ideal for long term metric collection, storage and visualisation.

This is where support for Prometheus in LavinMQ comes in.

## What is Prometheus?

In a nutshell, Prometheus is a monitoring tool that collects and stores metrics from
different systems using a simple scraping method. It saves the data in a database and
allows users to analyze it using a user-friendly query language called PromQL.

## When can I use Prometheus with LavinMQ

You can integrate Prometheus with LavinMQ:

- If you need a more long term metric collection and storage solution
- If you need to adopt an external monitoring solution and by extension separate the
  system being monitored from the monitoring system.

## How does LavinMQ support Prometheus?

Every LavinMQ installation/instance ships with two metrics endpoints ouf of the box.
These endpoints expose data in a Prometheus-compatible format. To integrate
Prometheus with LavinMQ, you simply point your Prometheus scraper to these
endpoints:

- **`/metrics`:** This endpoint returns aggregate metrics of all the metrics emitting objects
- **`/metrics/detailed`:** This returns the metric for each metric emitting object depending
  on the query parameters it's passed.

### /metrics

Typically, Prometheus and other compatible solutions assume that metrics are accessible
at the `/metrics` path. LavinMQ, by default, provides aggregated metrics on this endpoint.

As mentioned earlier, this endpoint returns a combined metrics for all the metrics emitting
objects defined in your LavinMQ server. Metrics emitting objects here refers to connections,
channels, queues, and consumers amongst others.

The table below lists all the relevant metrics returned from this endpoint. |

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_connections_opened_total</td>
          <td>Total number of connections opened</td>
        </tr>
        <tr>
          <td>lavinmq_connections_closed_total</td>
          <td>Total number of connections closed or terminated</td>
        </tr>
        <tr>
          <td>lavinmq_channels_opened_total</td>
          <td>Total number of channels opened</td>
        </tr>
        <tr>
          <td>lavinmq_channels_closed_total</td>
          <td>Total number of channels closed</td>
        </tr>
        <tr>
          <td>lavinmq_queues_declared_total</td>
          <td>Total number of queues declared</td>
        </tr>
        <tr>
          <td>lavinmq_queues_deleted_total</td>
          <td>Total number of queues deleted</td>
        </tr>
        <tr>
          <td>lavinmq_process_open_fds</td>
          <td>Open file descriptors</td>
        </tr>
        <tr>
          <td>lavinmq_process_open_tcp_sockets</td>
          <td>Open TCP sockets</td>
        </tr>
        <tr>
          <td>lavinmq_disk_space_available_bytes</td>
          <td>Disk space available in bytes</td>
        </tr>
        <tr>
          <td>lavinmq_process_max_fds</td>
          <td>Open file descriptors limit</td>
        </tr>
        <tr>
          <td>lavinmq_resident_memory_limit_bytes</td>
          <td>Memory high watermark in bytes</td>
        </tr>
        <tr>
          <td>lavinmq_connections</td>
          <td>Connections currently open</td>
        </tr>
        <tr>
          <td>lavinmq_channels</td>
          <td>Channels currently open</td>
        </tr>
        <tr>
          <td>lavinmq_consumers</td>
          <td>Consumers currently connected</td>
        </tr>
        <tr>
          <td>lavinmq_queues</td>
          <td>Queues available</td>
        </tr>
        <tr>
          <td>lavinmq_queue_messages_ready</td>
          <td>Messages ready to be delivered to consumers</td>
        </tr>
        <tr>
          <td>lavinmq_queue_messages_unacked</td>
          <td>Messages delivered to consumers but not yet acknowledged</td>
        </tr>
        <tr>
          <td>lavinmq_queue_messages</td>
          <td>Sum of ready and unacknowledged messages - total queue depth</td>
        </tr>
        <tr>
          <td>lavinmq_global_messages_delivered_total</td>
          <td>Total number of messages delivered to consumers</td>
        </tr>
        <tr>
          <td>lavinmq_global_messages_redelivered_total</td>
          <td>Total number of messages redelivered to consumers</td>
        </tr>
        <tr>
          <td>lavinmq_global_messages_acknowledged_total</td>
          <td>Total number of messages acknowledged by consumers</td>
        </tr>
        <tr>
          <td>lavinmq_global_messages_confirmed_total</td>
          <td>Total number of messages confirmed to publishers</td>
        </tr>
        <tr>
          <td>lavinmq_deliver_no_ack</td>
          <td>Number of messages delivered with auto-acknowledgment (no_ack=true)</td>
        </tr>
        <tr>
          <td>lavinmq_get_no_ack</td>
          <td>Number of messages retrieved via Basic.Get with auto-acknowledgment</td>
        </tr>
        <tr>
          <td>lavinmq_uptime</td>
          <td>Server uptime in seconds</td>
        </tr>
        <tr>
          <td>lavinmq_cpu_system_time_total</td>
          <td>Total CPU system time</td>
        </tr>
        <tr>
          <td>lavinmq_rss_bytes</td>
          <td>Memory RSS in bytes</td>
        </tr>
        <tr>
          <td>lavinmq_gc_heap_size_bytes</td>
          <td>Heap size in bytes (including the area unmapped to OS)</td>
        </tr>
        <tr>
          <td>lavinmq_gc_free_bytes</td>
          <td>Total bytes contained in free and unmapped blocks</td>
        </tr>
        <tr>
          <td>lavinmq_gc_unmapped_bytes</td>
          <td>Amount of memory unmapped to OS</td>
        </tr>
        <tr>
          <td>lavinmq_gc_since_recent_collection_allocated_bytes</td>
          <td>Number of bytes allocated since the recent GC</td>
        </tr>
        <tr>
          <td>lavinmq_gc_before_recent_collection_allocated_bytes_total</td>
          <td>Number of bytes allocated before the recent GC (value may wrap)</td>
        </tr>
        <tr>
          <td>lavinmq_gc_non_candidate_bytes</td>
          <td>Number of bytes not considered candidates for GC</td>
        </tr>
        <tr>
          <td>lavinmq_gc_cycles_total</td>
          <td>Garbage collection cycle number (value may wrap)</td>
        </tr>
        <tr>
          <td>lavinmq_gc_marker_threads</td>
          <td>Number of marker threads (excluding the initiating one)</td>
        </tr>
        <tr>
          <td>lavinmq_gc_since_recent_collection_reclaimed_bytes</td>
          <td>Approximate number of reclaimed bytes after recent GC</td>
        </tr>
        <tr>
          <td>lavinmq_gc_before_recent_collection_reclaimed_bytes_total</td>
          <td>Approximate number of bytes reclaimed before the recent GC (value may wrap)</td>
        </tr>
        <tr>
          <td>lavinmq_gc_since_recent_collection_explicitly_freed_bytes</td>
          <td>Number of bytes freed explicitly since the recent GC</td>
        </tr>
        <tr>
          <td>lavinmq_gc_from_os_obtained_bytes_total</td>
          <td>Total amount of memory obtained from OS, in bytes</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

All the metrics returned from this endpoint are prefixed with `lavinmq` by default.
However, this is customisable - just pass the the query param `prefix`
with your custom prefix like so: `/metrics?prefix=prometheus`.

### /metrics/detailed

As opposed to returning aggregate metrics, this endpoint allows
filtering metrics per object via query params. This way, you only get
the metrics you need.

By default, this endpoint returns nothing until you pass a filter/query
param.

With this endpoint, you can either pass one ore **family** values or one or more
**vhost** values. Let's cover each of them one at a time.

#### family:

In the context of these metrics, every entity on a LavinMQ node
belongs to a family. E.g all queues belong to the queue family, connections to the
connection family and so on and so forth. The _family_ query parameter essentially
allows you retrieve metrics for a specific group of objects(queues, connections, channels etc).

For example, if you want to grab metrics on queues only, you will pass
the following values to the family parameter:

`/metrics/detailed?family=queue_coarse_metrics`

LavinMQ supports the following family values:

##### `queue_coarse_metrics`

In this group, every metric points to a particular queue through its label.
As a result, the size of the response here grows in direct proportion to the
number of queues hosted on the node.

Thus, passing this value to the family query param will return the following metrics
for each queue defined on your LavinMQ node:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_detailed_queue_messages_ready</td>
          <td>Messages ready to be delivered to consumers</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_queue_messages_unacked</td>
          <td>Messages delivered to consumers but not yet acknowledged</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_queue_messages</td>
          <td>Sum of ready and unacknowledged messages - total queue depth</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

**Deprecated Metric Names:**

The following queue metric names are deprecated and will be removed in the next major version. Please migrate to the new metric names:

- **DEPRECATED:** `ready` → Use `messages_ready` instead
- **DEPRECATED:** `ready_bytes` → Use `message_bytes_ready` instead
- **DEPRECATED:** `unacked` → Use `messages_unacknowledged` instead
- **DEPRECATED:** `unacked_bytes` → Use `message_bytes_unacknowledged` instead

##### `connection_churn_metrics`

This retrieves generic metrics on connection. Passing this value to the family query
param will return the following metrics:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_detailed_consumers_added_total</td>
          <td>Total number of consumers added</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_consumers_removed_total</td>
          <td>Total number of consumers removed</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_connections_opened_total</td>
          <td>Total number of connections opened</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_connections_closed_total</td>
          <td>Total number of connections closed or terminated</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_channels_opened_total</td>
          <td>Total number of channels opened</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_channels_closed_total</td>
          <td>Total number of channels closed</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_queues_declared_total</td>
          <td>Total number of queues declared</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_queues_deleted_total</td>
          <td>Total number of queues deleted</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

##### `queue_consumer_count`

This metric serves a valuable purpose by promptly identifying consumer-related
issues, such as when no consumers are online. That's why it's made available
separately for easy access and monitoring.

Passing this value to the family query param will return just one metric:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_detailed_queue_consumers</td>
          <td>Consumers on a queue</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

##### `connection_coarse_metrics`

Passing this value to the family query param will return the following metrics:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_detailed_connection_incoming_bytes_total</td>
          <td>Total number of bytes received on a connection</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_connection_outgoing_bytes_total</td>
          <td>Total number of bytes sent on a connection</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_connection_process_reductions_total</td>
          <td>Total number of connection process reductions</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

##### `channel_metrics`

Passing this value to the family query param will return the following metrics:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Metric</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>lavinmq_detailed_channel_consumers</td>
          <td>Consumers on a channel</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_channel_messages_unacked</td>
          <td>Delivered but not yet acknowledged messages</td>
        </tr>
        <tr>
          <td>lavinmq_detailed_channel_prefetch</td>
          <td>Total limit of unacknowledged messages for all consumers on a channel</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

#### vhost:

Family values would allow you retrieve metrics for a group of objects on a node.
For example, the `queue_coarse_metrics` will return metrics for all the queues
defined on a node. But what if you just want metrics for all the queues defined
in a specific vhost?

You can do that with the `vhost` query param like so:

`/metrics/detailed?vhost=vhost-name&family=queue_coarse_metrics`

The query above will return metrics for the queues in the specified vhost only.

**Note:** All the metrics returned from the `/metrics/detailed` endpoint are prefixed with `lavinmq_detailed`.
However, this is customisable - again, just pass the the query param `prefix`
with your custom prefix like so: `/metrics/detailed?prefix=prometheus`.
