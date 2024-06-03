require "uri"
require "../controller"
require "../binding_helpers"

module LavinMQ
  module HTTP
    class PrometheusWriter
      alias MetricValue = UInt16 | Int32 | UInt32 | UInt64 | Int64 | Float64
      alias MetricLabels = Hash(String, String) |
                           NamedTuple(name: String) |
                           NamedTuple(channel: String) |
                           NamedTuple(queue: String, vhost: String)
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
        name = "#{@prefix}_#{m[:name]}"
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
    end

    class PrometheusController < Controller
      private def target_vhosts(context)
        u = user(context)
        vhosts = vhosts(u)
        selected = context.request.query_params.fetch_all("vhost")
        vhosts = vhosts.select { |vhost| selected.includes? vhost.name } unless selected.empty?
        vhosts.to_a
      end

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
            custom_metrics(vhosts, writer)
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
              end
            end
          end
          context
        end
      end

      private def report(io, &blk)
        mem = 0
        elapsed = Time.measure do
          mem = Benchmark.memory(&blk)
        end
        writer = PrometheusWriter.new(io, "telemetry")
        writer.write({name:  "scrape_duration_seconds",
                      type:  "counter",
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

      private def custom_metrics(vhosts, writer)
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
        writer.write({name:  "rss_bytes",
                      type:  "gauge",
                      value: @amqp_server.rss,
                      help:  "Memory RSS in bytes"})
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
                      value: @amqp_server.@replicator.followers.size,
                      type:  "gauge",
                      help:  "Amount of follower nodes connected"})
        @amqp_server.@replicator.followers.each_with_index do |f, i|
          writer.write({name:  "follower_lag_#{i}",
                        value: f.lag,
                        type:  "gauge",
                        help:  "Lag for follower on address: #{f.@socket.remote_address}"})
        end
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
