require "uri"
require "../controller"
require "../resource_helpers"
require "../binding_helpers"

module AvalancheMQ
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

      getter :prefix

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
        return unless m[:value]?
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
      private def register_routes
        get "/metrics" do |context, _|
          prefix = context.request.query_params["prefix"]? || "lavinmq"
          bad_request(context, "prefix to long") if prefix.size > 20
          u = user(context)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            overview_broker_metrics(writer)
            overview_queue_metrics(u, writer)
            custom_metrics(u, writer)
          end
          context
        end

        get "/metrics/detailed" do |context, _|
          prefix = context.request.query_params["prefix"]? || "lavinmq"
          bad_request(context, "prefix to long") if prefix.size > 20
          families = context.request.query_params.fetch_all("family")
          u = user(context)
          vhosts = vhosts(u)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            families.each do |family|
              case family
              when "connection_churn_metrics"
                detailed_connection_churn_metrics(writer)
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
                      value: elapsed.total_seconds,
                      help:  "Duration for metrics collection in seconds"})
        writer.write({name:  "scrape_mem",
                      value: mem,
                      help:  "Memory used for metrics collections in Kb"})
      end

      private def overview_broker_metrics(writer)
        writer.write({name:   "identity_info",
                      value:  1,
                      help:   "System information",
                      labels: {
                        "#{writer.prefix}_version" => AvalancheMQ::VERSION,
                        "#{writer.prefix}_cluster" => System.hostname,
                      }})
        writer.write({name:  "connections_opened_total",
                      value: @amqp_server.connection_created_count,
                      type:  "counter",
                      help:  "Total number of connections opened"})
        writer.write({name:  "connections_closed_total",
                      value: @amqp_server.connection_closed_count,
                      type:  "counter",
                      help:  "Total number of connections closed or terminated"})
        writer.write({name:  "channels_opened_total",
                      value: @amqp_server.channel_created_count,
                      type:  "counter",
                      help:  "Total number of channels opened"})
        writer.write({name:  "channels_closed_total",
                      value: @amqp_server.channel_closed_count,
                      type:  "counter",
                      help:  "Total number of channels closed"})
        writer.write({name:  "queues_declared_total",
                      value: @amqp_server.queue_declared_count,
                      type:  "counter",
                      help:  "Total number of queues declared"})
        writer.write({name:  "queues_deleted_total",
                      value: @amqp_server.queue_deleted_count,
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
                      value: @amqp_server.rss,
                      help:  "Memory used in bytes"})
        writer.write({name:  "disk_space_available_bytes",
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

      private def overview_queue_metrics(u, writer)
        ready = unacked = connections = channels = consumers = queues = 0_u64
        vhosts(u).each do |vhost|
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

      private def custom_metrics(u, writer)
        writer.write({name: "uptime", value: @amqp_server.uptime.to_i,
                      help: "Server uptime in seconds"})
        writer.write({name:  "cpu_system_time_total",
                      value: @amqp_server.sys_time,
                      type:  "counter",
                      help:  "Total CPU system time"})
        writer.write({name:  "cpu_user_time_total",
                      value: @amqp_server.user_time,
                      type:  "counter",
                      help:  "Total CPU user time"})
        writer.write({name: "rss_bytes", value: @amqp_server.rss,
                      help: "Memory RSS in bytes"})
        vhosts(u).each do |vhost|
          labels = {name: vhost.name}
          writer.write({name:   "gc_runs_total",
                        value:  vhost.gc_runs,
                        labels: labels,
                        type:   "counter",
                        help:   "Number of GC runs"})
          vhost.gc_timing.each do |k, v|
            writer.write({name:   "gc_time_#{k.downcase.tr(" ", "_")}",
                          value:  v,
                          labels: labels,
                          type:   "counter",
                          help:   "GC time spent in #{k}"})
          end
        end
      end

      private def detailed_connection_churn_metrics(writer)
        writer.write({name:  "detailed_connections_opened_total",
                      value: @amqp_server.connection_created_count,
                      type:  "counter",
                      help:  "Total number of connections opened"})
        writer.write({name:  "detailed_connections_closed_total",
                      value: @amqp_server.connection_closed_count,
                      type:  "counter",
                      help:  "Total number of connections closed or terminated"})
        writer.write({name:  "detailed_channels_opened_total",
                      value: @amqp_server.channel_created_count,
                      type:  "counter",
                      help:  "Total number of channels opened"})
        writer.write({name:  "detailed_channels_closed_total",
                      value: @amqp_server.channel_closed_count,
                      type:  "counter",
                      help:  "Total number of channels closed"})
        writer.write({name:  "detailed_queues_declared_total",
                      value: @amqp_server.queue_declared_count,
                      type:  "counter",
                      help:  "Total number of queues declared"})
        writer.write({name:  "detailed_queues_deleted_total",
                      value: @amqp_server.queue_deleted_count,
                      type:  "counter",
                      help:  "Total number of queues deleted"})
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
