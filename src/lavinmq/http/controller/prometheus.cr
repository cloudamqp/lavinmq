require "uri"
require "benchmark"
require "../controller"
require "../binding_helpers"

module LavinMQ
  module HTTP
    class PrometheusWriter
      alias MetricValue = UInt16 | Int32 | UInt32 | UInt64 | Int64 | Float64
      alias MetricLabels = Hash(String, String) |
                           NamedTuple(name: String) |
                           NamedTuple(channel: String) |
                           NamedTuple(id: String) |
                           NamedTuple(queue: String, vhost: String) |
                           NamedTuple(exchange: String, vhost: String)
      alias Metric = NamedTuple(name: String, value: MetricValue) |
                     NamedTuple(name: String, value: MetricValue, labels: MetricLabels) |
                     NamedTuple(name: String, value: MetricValue, help: String) |
                     NamedTuple(name: String, value: MetricValue, type: String, help: String) |
                     NamedTuple(name: String, value: MetricValue, help: String, labels: MetricLabels) |
                     NamedTuple(name: String, value: MetricValue, type: String, help: String, labels: MetricLabels)

      getter prefix

      def initialize(@io : IO, @prefix : String)
      end

      private def write_labels(io, labels)
        first = true
        io << "{"
        labels.each do |k, v|
          io << ", " unless first
          io << k << "=\"" << v << "\""
          first = false
        end
        io << "}"
      end

      def write(m : Metric)
        return if m[:value].nil?
        io = @io

        name = name_writer(m)
        if t = m[:type]?
          io << "# TYPE " << name << " " << t << "\n"
        end
        if h = m[:help]?
          io << "# HELP " << name << " " << h << "\n"
        end
        io << name
        if l = m[:labels]?
          write_labels(io, l)
        end
        io << " " << m[:value] << "\n"
      end

      struct NameWriter
        def initialize(@prefix : String, @name : String)
        end

        def to_s(io : IO) : IO
          io << @prefix << "_" << @name
        end
      end

      private def name_writer(m : Metric)
        NameWriter.new(@prefix, m[:name])
      end
    end

    class PrometheusController < Controller
      private def target_vhosts(context)
        u = user(context)
        vhosts = vhosts(u)
        selected = context.request.query_params.fetch_all("vhost")
        vhosts = vhosts.select { |vhost| selected.includes? vhost.name } unless selected.empty?
        vhosts.to_a
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/metrics" do |context, _|
          context.response.content_type = "text/plain"
          prefix = context.request.query_params["prefix"]? || "lavinmq"
          bad_request(context, "Prefix too long (max 20 characters)") if prefix.bytesize > 20
          vhosts = target_vhosts(context)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            overview_broker_metrics(vhosts, writer)
            overview_queue_metrics(vhosts, writer)
            custom_metrics(writer)
            gc_metrics(writer)
            global_metrics(writer)
          end
          context
        end

        get "/metrics/detailed" do |context, _|
          context.response.content_type = "text/plain"
          prefix = context.request.query_params["prefix"]? || "lavinmq"
          bad_request(context, "Prefix too long (max 20 characters)") if prefix.bytesize > 20
          families = context.request.query_params.fetch_all("family")
          vhosts = target_vhosts(context)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            families.each do |family|
              case family
              when "connection_churn_metrics"
                detailed_connection_churn_metrics(vhosts, writer)
              when "queue_coarse_metrics"
                detailed_queue_coarse_metrics(vhosts, writer)
              when "queue_consumer_count"
                detailed_queue_consumer_count(vhosts, writer)
              when "connection_coarse_metrics", "connection_metrics"
                detailed_connection_coarse_metrics(vhosts, writer)
              when "channel_metrics"
                detailed_channel_metrics(vhosts, writer)
              when "exchange_metrics"
                detailed_exchange_metrics(vhosts, writer)
              end
            end
          end
          context
        end
      end

      private def report(io, &)
        mem = 0
        elapsed = Time.measure do
          mem = Benchmark.memory do
            begin
              yield
            rescue ex
              Log.error(exception: ex) { "Error while reporting prometheus metrics" }
            end
          end
        end
        writer = PrometheusWriter.new(io, "telemetry")
        writer.write({name:  "scrape_duration_seconds",
                      type:  "gauge",
                      value: elapsed.total_seconds,
                      help:  "Duration for metrics collection in seconds"})
        writer.write({name:  "scrape_mem",
                      type:  "gauge",
                      value: mem,
                      help:  "Memory used for metrics collections in bytes"})
      end

      private def overview_broker_metrics(vhosts, writer)
        stats = vhost_stats(vhosts)
        writer.write({name:   "identity_info",
                      type:   "gauge",
                      value:  1,
                      help:   "System information",
                      labels: {
                        "#{writer.prefix}_version" => LavinMQ::VERSION,
                        "#{writer.prefix}_cluster" => System.hostname,
                      }})

        writer.write({name:  "connections_opened_total",
                      value: stats[:connection_created],
                      type:  "counter",
                      help:  "Total number of connections opened"})
        writer.write({name:  "connections_closed_total",
                      value: stats[:connection_closed],
                      type:  "counter",
                      help:  "Total number of connections closed or terminated"})
        writer.write({name:  "channels_opened_total",
                      value: stats[:channel_created],
                      type:  "counter",
                      help:  "Total number of channels opened"})
        writer.write({name:  "channels_closed_total",
                      value: stats[:channel_closed],
                      type:  "counter",
                      help:  "Total number of channels closed"})
        writer.write({name:  "queues_declared_total",
                      value: stats[:queue_declared],
                      type:  "counter",
                      help:  "Total number of queues declared"})
        writer.write({name:  "queues_deleted_total",
                      value: stats[:queue_deleted],
                      type:  "counter",
                      help:  "Total number of queues deleted"})
        writer.write({name:  "process_open_fds",
                      value: System.file_descriptor_count,
                      type:  "gauge",
                      help:  "Open file descriptors"})
        writer.write({name:  "process_open_tcp_sockets",
                      value: @amqp_server.vhosts.sum { |_, v| v.connections.size },
                      type:  "gauge",
                      help:  "Open TCP sockets"})
        writer.write({name:  "process_resident_memory_bytes",
                      type:  "gauge",
                      value: @amqp_server.rss,
                      help:  "Memory used in bytes"})
        writer.write({name:  "disk_space_available_bytes",
                      type:  "gauge",
                      value: @amqp_server.disk_free,
                      help:  "Disk space available in bytes"})
        writer.write({name:  "process_max_fds",
                      value: System.file_descriptor_limit[0],
                      type:  "gauge",
                      help:  "Open file descriptors limit"})
        writer.write({name:  "resident_memory_limit_bytes",
                      value: @amqp_server.mem_limit,
                      type:  "gauge",
                      help:  "Memory high watermark in bytes"})
      end

      private def global_metrics(writer)
        writer.write({name:  "global_messages_delivered_total",
                      value: @amqp_server.deleted_vhosts_messages_delivered_total +
                             @amqp_server.vhosts.sum { |_, v| v.message_details[:message_stats][:deliver] },
                      type: "counter",
                      help: "Total number of messaged delivered to consumers"})
        writer.write({name:  "global_messages_redelivered_total",
                      value: @amqp_server.deleted_vhosts_messages_redelivered_total +
                             @amqp_server.vhosts.sum { |_, v| v.message_details[:message_stats][:redeliver] },
                      type: "counter",
                      help: "Total number of messages redelivered to consumers"})
        writer.write({name:  "global_messages_acknowledged_total",
                      value: @amqp_server.deleted_vhosts_messages_acknowledged_total +
                             @amqp_server.vhosts.sum { |_, v| v.message_details[:message_stats][:ack] },
                      type: "counter",
                      help: "Total number of messages acknowledged by consumers"})
        writer.write({name:  "global_messages_confirmed_total",
                      value: @amqp_server.deleted_vhosts_messages_confirmed_total +
                             @amqp_server.vhosts.sum { |_, v| v.message_details[:message_stats][:confirm] },
                      type: "counter",
                      help: "Total number of messages confirmed to publishers"})
      end

      private def overview_queue_metrics(vhosts, writer)
        ready = unacked = connections = channels = consumers = queues = 0_u64
        vhosts.each do |vhost|
          d = vhost.message_details
          ready += d[:messages_ready]
          unacked += d[:messages_unacknowledged]
          connections += vhost.connections.size
          vhost.connections.each do |conn|
            channels += conn.channels.size
            conn.channels.each_value do |ch|
              consumers += ch.consumers.size
            end
          end
          queues += vhost.queues.size
        end
        writer.write({name:  "connections",
                      value: connections,
                      type:  "gauge",
                      help:  "Connections currently open"})
        writer.write({name:  "channels",
                      value: channels,
                      type:  "gauge",
                      help:  "Channels currently open"})
        writer.write({name:  "consumers",
                      value: consumers,
                      type:  "gauge",
                      help:  "Consumers currently connected"})
        writer.write({name:  "queues",
                      value: queues,
                      type:  "gauge",
                      help:  "Queues available"})
        writer.write({name:  "queue_messages_ready",
                      value: ready,
                      type:  "gauge",
                      help:  "Messages ready to be delivered to consumers"})
        writer.write({name:  "queue_messages_unacked",
                      value: unacked,
                      type:  "gauge",
                      help:  "Messages delivered to consumers but not yet acknowledged"})
        writer.write({name:  "queue_messages",
                      value: ready + unacked,
                      type:  "gauge",
                      help:  "Sum of ready and unacknowledged messages - total queue depth"})
      end

      private def custom_metrics(writer)
        writer.write({name: "uptime", value: @amqp_server.uptime.to_i,
                      type: "counter",
                      help: "Server uptime in seconds"})
        writer.write({name:  "cpu_system_time_total",
                      value: @amqp_server.sys_time,
                      type:  "counter",
                      help:  "Total CPU system time"})
        writer.write({name:  "cpu_user_time_total",
                      value: @amqp_server.user_time,
                      type:  "counter",
                      help:  "Total CPU user time"})
        writer.write({name:  "stats_collection_duration_seconds_total",
                      value: @amqp_server.stats_collection_duration_seconds_total.to_f,
                      type:  "gauge",
                      help:  "Total time it takes to collect metrics (stats_loop)"})
        writer.write({name:  "stats_rates_collection_duration_seconds",
                      value: @amqp_server.stats_rates_collection_duration_seconds.to_f,
                      type:  "gauge",
                      help:  "Time it takes to update stats rates (update_stats_rates)"})
        writer.write({name:  "stats_system_collection_duration_seconds",
                      value: @amqp_server.stats_system_collection_duration_seconds.to_f,
                      type:  "gauge",
                      help:  "Time it takes to collect system metrics"})
        writer.write({name:  "total_connected_followers",
                      value: @amqp_server.followers.size,
                      type:  "gauge",
                      help:  "Amount of follower nodes connected"})
        @amqp_server.followers.each do |f|
          writer.write({name:   "follower_lag_in_bytes",
                        labels: {id: f.id.to_s(36)},
                        value:  f.lag_in_bytes,
                        type:   "gauge",
                        help:   "Bytes that hasn't been synchronized with the follower yet"})
        end
      end

      private def gc_metrics(writer)
        gc_stats = @amqp_server.gc_stats

        writer.write({name: "gc_heap_size_bytes", value: gc_stats.heap_size,
                      type: "gauge",
                      help: "Heap size in bytes (including the area unmapped to OS)"})

        writer.write({name: "gc_free_bytes", value: gc_stats.free_bytes,
                      type: "gauge",
                      help: "Total bytes contained in free and unmapped blocks"})

        writer.write({name: "gc_unmapped_bytes", value: gc_stats.unmapped_bytes,
                      type: "gauge",
                      help: "Amount of memory unmapped to OS"})

        writer.write({name: "gc_since_recent_collection_allocated_bytes", value: gc_stats.bytes_since_gc,
                      type: "gauge",
                      help: "Number of bytes allocated since the recent GC"})

        writer.write({name: "gc_before_recent_collection_allocated_bytes_total", value: gc_stats.bytes_before_gc,
                      type: "counter",
                      help: "Number of bytes allocated before the recent GC (value may wrap)"})

        writer.write({name: "gc_non_candidate_bytes", value: gc_stats.non_gc_bytes,
                      type: "gauge",
                      help: "Number of bytes not considered candidates for GC"})

        writer.write({name: "gc_cycles_total", value: gc_stats.gc_no,
                      type: "counter",
                      help: "Garbage collection cycle number (value may wrap)"})

        writer.write({name: "gc_marker_threads", value: gc_stats.markers_m1,
                      type: "gauge",
                      help: "Number of marker threads (excluding the initiating one)"})

        writer.write({name: "gc_since_recent_collection_reclaimed_bytes", value: gc_stats.bytes_reclaimed_since_gc,
                      type: "gauge",
                      help: "Approximate number of reclaimed bytes after recent GC"})

        writer.write({name: "gc_before_recent_collection_reclaimed_bytes_total", value: gc_stats.reclaimed_bytes_before_gc,
                      type: "counter",
                      help: "Approximate number of bytes reclaimed before the recent GC (value may wrap)"})

        writer.write({name: "gc_since_recent_collection_explicitly_freed_bytes", value: gc_stats.expl_freed_bytes_since_gc,
                      type: "counter",
                      help: "Number of bytes freed explicitly since the recent GC"})

        writer.write({name: "gc_from_os_obtained_bytes_total", value: gc_stats.obtained_from_os_bytes,
                      type: "counter",
                      help: "Total amount of memory obtained from OS, in bytes"})
      end

      SERVER_METRICS = {:connection_created, :connection_closed, :channel_created, :channel_closed,
                        :queue_declared, :queue_deleted, :consumer_added, :consumer_removed}

      private def vhost_stats(vhosts)
        {% for sm in SERVER_METRICS %}
          {{sm.id}} = 0_u64
        {% end %}
        vhosts.each do |vhost|
          {% for sm in SERVER_METRICS %}
            {{sm.id}} += vhost.stats_details[:{{sm.id}}]
          {% end %}
        end
        {% begin %}
        {
          {% for sm in SERVER_METRICS %}
            {{sm.id}}: {{sm.id}},
          {% end %}
        }
        {% end %}
      end

      private def detailed_connection_churn_metrics(vhosts, writer)
        stats = vhost_stats(vhosts)
        writer.write({name:  "detailed_connections_opened_total",
                      value: stats[:connection_created],
                      type:  "counter",
                      help:  "Total number of connections opened"})
        writer.write({name:  "detailed_connections_closed_total",
                      value: stats[:connection_closed],
                      type:  "counter",
                      help:  "Total number of connections closed or terminated"})
        writer.write({name:  "detailed_channels_opened_total",
                      value: stats[:channel_created],
                      type:  "counter",
                      help:  "Total number of channels opened"})
        writer.write({name:  "detailed_channels_closed_total",
                      value: stats[:channel_closed],
                      type:  "counter",
                      help:  "Total number of channels closed"})
        writer.write({name:  "detailed_queues_declared_total",
                      value: stats[:queue_declared],
                      type:  "counter",
                      help:  "Total number of queues declared"})
        writer.write({name:  "detailed_queues_deleted_total",
                      value: stats[:queue_deleted],
                      type:  "counter",
                      help:  "Total number of queues deleted"})
        writer.write({name:  "detailed_consumers_added_total",
                      value: stats[:consumer_added],
                      type:  "counter",
                      help:  "Total number of consumers added"})
        writer.write({name:  "detailed_consumers_removed_total",
                      value: stats[:consumer_removed],
                      type:  "counter",
                      help:  "Total number of consumers removed"})
      end

      private def detailed_queue_coarse_metrics(vhosts, writer)
        vhosts.each do |vhost|
          vhost.queues.each_value do |q|
            labels = {queue: q.name, vhost: vhost.name}
            ready = q.message_count
            unacked = q.unacked_count
            writer.write({name:   "detailed_queue_messages_ready",
                          value:  ready,
                          type:   "gauge",
                          labels: labels,
                          help:   "Messages ready to be delivered to consumers"})
            writer.write({name:   "detailed_queue_messages_unacked",
                          value:  unacked,
                          type:   "gauge",
                          labels: labels,
                          help:   "Messages delivered to consumers but not yet acknowledged"})
            writer.write({name:   "detailed_queue_messages",
                          value:  ready + unacked,
                          type:   "gauge",
                          labels: labels,
                          help:   "Sum of ready and unacknowledged messages - total queue depth"})
            writer.write({name:   "detailed_queue_deduplication",
                          value:  q.dedup_count,
                          type:   "counter",
                          labels: labels,
                          help:   "Number of deduplicated messages for this queue"})
          end
        end
      end

      private def detailed_queue_consumer_count(vhosts, writer)
        vhosts.each do |vhost|
          vhost.queues.each_value do |q|
            labels = {queue: q.name, vhost: vhost.name}
            writer.write({name:   "detailed_queue_consumers",
                          value:  q.consumers.size,
                          type:   "gauge",
                          labels: labels,
                          help:   "Consumers on a queue"})
          end
        end
      end

      private def detailed_exchange_metrics(vhosts, writer)
        vhosts.each do |vhost|
          vhost.exchanges.each_value do |e|
            labels = {exchange: e.name, vhost: vhost.name}
            writer.write({name:   "detailed_exchange_deduplication",
                          value:  e.dedup_count,
                          type:   "counter",
                          labels: labels,
                          help:   "Number of deduplicated messages for this queue"})
          end
        end
      end

      private def detailed_connection_coarse_metrics(vhosts, writer)
        vhosts.each do |vhost|
          vhost.connections.each do |conn|
            labels = {channel: conn.name}
            writer.write({name:   "detailed_connection_incoming_bytes_total",
                          value:  conn.recv_oct_count,
                          type:   "counter",
                          labels: labels,
                          help:   "Total number of bytes received on a connection"})
            writer.write({name:   "detailed_connection_outgoing_bytes_total",
                          value:  conn.send_oct_count,
                          type:   "counter",
                          labels: labels,
                          help:   "Total number of bytes sent on a connection"})
            writer.write({name:   "detailed_connection_channels",
                          value:  conn.channels.size,
                          type:   "counter",
                          labels: labels,
                          help:   "Channels on a connection"})
          end
        end
      end

      private def detailed_channel_metrics(vhosts, writer)
        vhosts.each do |vhost|
          vhost.connections.each do |conn|
            conn.channels.each_value do |ch|
              labels = {channel: ch.name}
              d = ch.details_tuple
              writer.write({name:   "detailed_channel_consumers",
                            value:  d[:consumer_count],
                            type:   "gauge",
                            labels: labels,
                            help:   "Consumers on a channels"})
              writer.write({name:   "detailed_messages_unacked",
                            value:  d[:messages_unacknowledged],
                            type:   "gauge",
                            labels: labels,
                            help:   "Delivered but not yet acknowledged messages"})
              writer.write({name:   "detailed_channel_prefetch",
                            value:  d[:prefetch_count],
                            type:   "gauge",
                            labels: labels,
                            help:   "Total limit of unacknowledged messages for all consumers on a channel"})
            end
          end
        end
      end
    end
  end
end
