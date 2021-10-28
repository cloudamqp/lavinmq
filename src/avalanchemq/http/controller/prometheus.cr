require "uri"
require "../controller"
require "../resource_helpers"
require "../binding_helpers"

module AvalancheMQ
  module HTTP
    class PrometheusWriter
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

      def write(m)
        return unless m[:value]?
        io = @io
        if t = m[:type]?
          io << "# TYPE " << m[:name] << " " << t << "\n"
        end
        if h = m[:help]?
          io << "# HELP " << m[:name] << " " << h << "\n"
        end
        io << @prefix << "_" << m[:name]
        if l = m[:labels]?
          write_labels(io, l)
        end
        io << " " << m[:value] << "\n"
      end
    end

    class PrometheusController < Controller
      private def register_routes
        get "/metrics" do |context, _|
          prefix = context.request.query_params["prefix"] || "lavinmq"
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

        get "/metrics/custom" do |context, params|
          prefix = context.request.query_params["prefix"] || "lavinmq"
          bad_request(context, "prefix to long") if prefix.size > 20
          u = user(context)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            detailed_custom(writer, vhosts(u))
          end
          context
        end

        get "/metrics/detailed" do |context, params|
          prefix = context.request.query_params["prefix"] || "lavinmq"
          bad_request(context, "prefix to long") if prefix.size > 20
          families = context.request.query_params.fetch_all("family")
          u = user(context)
          report(context.response) do
            writer = PrometheusWriter.new(context.response, prefix)
            ## TODO
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
        writer.write({name: "scrape_duration_seconds", value: elapsed.total_seconds})
        writer.write({name: "scrape_mem", value: mem})
      end

      private def detailed_custom(writer, vhosts)
        vhosts.each do |vhost|
          details = vhost.message_details
          labels = { name: vhost.name }
          writer.write({name: "vhost_messages_ready",
                        value: details[:messages_ready],
                        labels: labels})
          writer.write({name: "vhost_messages_unacked",
                        value: details[:messages_unacknowledged],
                        labels: labels})
          details[:message_stats].each do |k, v|
            writer.write({name: "vhost_messages_#{k}", value: v, labels: labels})
          end
          vhost.exchanges.each_value do |e|
            labels = { name: e.name, vhost: vhost.name }
            writer.write({name: "exchange_publish_in", value: e.publish_in_count, labels: labels})
            writer.write({name: "exchange_publish_out", value: e.publish_out_count, labels: labels})
            writer.write({name: "exchange_unroutable", value: e.unroutable_count, labels: labels})
          end
          vhost.queues.each_value do |q|
            labels = { name: q.name, vhost: vhost.name }
            writer.write({name: "queue_messages_ready", value: q.message_count, labels: labels})
            writer.write({name: "queue_messages_unacked", value: q.unacked_count, labels: labels})
            writer.write({name: "queue_ack", value: q.ack_count, labels: labels})
            writer.write({name: "queue_deliver", value: q.deliver_count, labels: labels})
            writer.write({name: "queue_get", value: q.get_count, labels: labels})
            writer.write({name: "queue_publish", value: q.publish_count, labels: labels})
            writer.write({name: "queue_redeliver", value: q.redeliver_count, labels: labels})
            writer.write({name: "queue_reject", value: q.reject_count, labels: labels})
          end
        end
      end

      private def overview_broker_metrics(writer)
        writer.write({name: "identity_info", value: 1, labels: {
                        "#{writer.prefix}_version" => AvalancheMQ::VERSION,
                        "#{writer.prefix}_node" => "rabbit@test-cheerful-beige-lemming-02",
                        "#{writer.prefix}_cluster" => "test-cheerful-beige-lemming"}})
        writer.write({name: "connections_opened_total",
                      value: @amqp_server.connection_created_count,
                      type: "counter",
                      help: "Total number of connections opened"})
        writer.write({name: "connections_closed_total",
                      value: @amqp_server.connection_closed_count,
                      type: "counter",
                      help: "Total number of connections closed or terminated"})
        writer.write({name: "channels_opened_total",
                      value: @amqp_server.channel_created_count,
                      type: "counter",
                      help: "Total number of channels opened"})
        writer.write({name: "channels_closed_total",
                      value: @amqp_server.channel_closed_count,
                      type: "counter",
                      help: "Total number of channels closed"})
        writer.write({name: "queues_declared_total",
                      value: @amqp_server.queue_declared_count,
                      type: "counter",
                      help: "Total number of queues declared"})
        writer.write({name: "queues_deleted_total",
                      value: @amqp_server.queue_deleted_count,
                      type: "counter",
                      help: "Total number of queues deleted"})
        writer.write({name: "process_open_fds",
                      value: System.file_descriptor_count,
                      type: "gauge",
                      help: "Open file descriptors"})
        writer.write({name: "process_open_tcp_sockets",
                      value: @amqp_server.vhosts.sum { |_, v| v.connections.size },
                      type: "gauge",
                      help: "Open TCP sockets"})
        writer.write({name: "process_resident_memory_bytes",
                      value: @amqp_server.rss,
                      gauge: "Memory used in bytes"})
        writer.write({name: "disk_space_available_bytes",
                      value: @amqp_server.disk_free,
                      gauge: "Disk space available in bytes"})
        writer.write({name: "process_max_fds",
                      value: System.file_descriptor_limit[0],
                      type: "gauge",
                      help: "Open file descriptors limit"})
        writer.write({name: "resident_memory_limit_bytes",
                      value: System.physical_memory,
                      type: "gauge",
                      help: "Memory high watermark in bytes"})
        writer.write({name: "disk_space_available_bytes",
                      value: @amqp_server.disk_free,
                      type: "gauge",
                      help: "Disk space available in bytes"})
        # writer.write({name: "alarms_file_descriptor_limit", value: 0})
        # writer.write({name: "alarms_free_disk_space_watermark", value: 0, type: "counter"})
        # writer.write({name: "alarms_memory_used_watermark", value: 0})
        # writer.write({name: "io_read_ops_total", value: 0, type: "counter", help: "Total number of I/O read operations"})
        # writer.write({name: "io_read_bytes_total", value: 0, type: "counter", help: "Total number of I/O bytes read"})
        # writer.write({name: "io_write_ops_total", value: 0, type: "counter", help: "Total number of I/O write operations"})
        # writer.write({name: "io_write_bytes_total", value: 0, type: "counter", help: "Total number of I/O bytes written"})
        # writer.write({name: "io_sync_ops_total", value: 0, type: "counter", help: "Total number of I/O sync operations"})
        # writer.write({name: "io_seek_ops_total", value: 0, type: "counter", help: "Total number of I/O seek operations"})
        # writer.write({name: "io_open_attempt_ops_total", value: 0, type: "counter", help: "Total number of file open attempts"})
        # writer.write({name: "io_reopen_ops_total", value: 0, type: "counter", help: "Total number of times files have been reopened"})
        # writer.write({name: "io_read_time_seconds_total", value: 0, type: "counter", help: "Total I/O read time"})
        # writer.write({name: "io_write_time_seconds_total", value: 0, type: "counter", help: "Total I/O write time"})
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
          end
          consumers += vhost.consumers.size
          queues += vhost.queues.size
        end
        writer.write({name: "connections",
                      value: connections,
                      type: "gauge",
                      help: "Connections currently open"})
        writer.write({name: "channels",
                      value: channels,
                      type: "gauge",
                      help: "Channels currently open"})
        writer.write({name: "consumers",
                      value: consumers,
                      type: "gauge",
                      help: "Consumers currently connected"})
        writer.write({name: "queues",
                      value: queues,
                      type: "gauge",
                      help: "Queues available"})
        writer.write({ name: "queue_messages_ready",
                       value: ready,
                       type: "gauge",
                       help: "Messages ready to be delivered to consumers"})
        writer.write({ name: "queue_messages_unacked",
                       value: unacked,
                       type: "gauge",
                       help: "Messages delivered to consumers but not yet acknowledged"})
        writer.write({ name: "queue_messages",
                       value: ready + unacked,
                       type: "gauge",
                       help: "Sum of ready and unacknowledged messages - total queue depth"})
        # writer.write({name: "queue_messages_persistent", value: 0, type: "gauge", help: "Persistent messages"})
        # writer.write({name: "queue_messages_bytes", value: 0, type: "gauge", help: "Size in bytes of ready and unacknowledged messages"})
        # writer.write({name: "queue_messages_ready_bytes", value: 0, type: "gauge", help: "Size in bytes of ready messages"})
        # writer.write({name: "queue_messages_unacked_bytes", value: 0, type: "gauge", help: "Size in bytes of all unacknowledged messages"})
        # writer.write({name: "channel_messages_unacked", value: 0, type: "gauge", help: "Delivered but not yet acknowledged messages"})
        # writer.write({name: "channel_messages_unconfirmed", value: 0, type: "gauge", help: "Published but not yet confirmed messages"})
        # writer.write({name: "channel_messages_published_total", value: 0, type: "counter", help: ""})
        # writer.write({name: "channel_messages_confirmed_total", value: 0, type: "counter", help: "Total number of messages published into an exchange and confirmed on the channel"})
        # writer.write({name: "channel_messages_unroutable_returned_total", value: 0, type: "counter", help: "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"})
        # writer.write({name: "channel_messages_unroutable_dropped_total", value: 0, type: "counter", help: "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"})
        # writer.write({name: "channel_get_ack_total", value: 0, type: "counter", help: "Total number of messages fetched with basic.get in manual acknowledgement mode"})
        # writer.write({name: "channel_get_total", value: 0, type: "counter", help: "Total number of messages fetched with basic.get in automatic acknowledgement mode"})
        # writer.write({name: "channel_messages_delivered_ack_total", value: 0, type: "counter", help: "Total number of messages delivered to consumers in manual acknowledgement mode"})
        # writer.write({name: "channel_messages_delivered_total", value: 0, type: "counter", help: "Total number of messages delivered to consumers in automatic acknowledgement mode"})
        # writer.write({name: "channel_messages_redelivered_total", value: 0, type: "counter", help: "Total number of messages redelivered to consumers"})
        # writer.write({name: "channel_messages_acked_total", value: 0, type: "counter", help: "Total number of messages acknowledged by consumers"})
        # writer.write({name: "channel_get_empty_total", value: 0, type: "counter", help: "Total number of times basic.get operations fetched no message"})
        # writer.write({name: "connection_incoming_bytes_total", value: 0, type: "counter", help: "Total number of bytes received on a connection"})
        # writer.write({name: "connection_outgoing_bytes_total", value: 0, type: "counter", help: "Total number of bytes sent on a connection"})
        # writer.write({name: "queue_messages_published_total", value: 0, type: "gauge", help: "Total number of messages published to queues"})
      end

      private def custom_metrics(u, writer)
        writer.write({name: "uptime", value: @amqp_server.uptime.to_i,
                      help: "Server uptime in seconds"})
        writer.write({name: "cpu_system_time_total",
                      value: @amqp_server.sys_time,
                      type: "counter",
                      help: "Total CPU system time"})
        writer.write({name: "cpu_user_time_total",
                      value: @amqp_server.user_time,
                      type: "counter",
                      help: "Total CPU user time"})
        writer.write({name: "rss_bytes", value: @amqp_server.rss,
                      help: "Memory RSS in bytes"})
        writer.write({name: "uptime", value: @amqp_server.uptime.to_i,
                      help: "Server uptime in seconds"})
        vhosts(u).each do |vhost|
          labels = { name: vhost.name }
          writer.write({name: "gc_runs",
                        value: vhost.gc_runs,
                        labels: labels,
                        type: "counter",
                        help: "Number of GC runs"})
          vhost.gc_timing.each do |k,v|
            writer.write({name: "gc_time_#{k.downcase.tr(" ", "_")}",
                          value: v,
                          labels: labels,
                          type: "counter",
                          help: "GC time spent in #{k}"})
          end
        end
      end
    end
  end
end
