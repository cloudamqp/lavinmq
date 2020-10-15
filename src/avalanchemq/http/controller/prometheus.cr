
require "uri"
require "../controller"
require "../resource_helpers"
require "../binding_helpers"

module AvalancheMQ
  module HTTP
    class PrometheusController < Controller
      private def append_labels(io, labels)
        key = true
        first = true
        io << "{"
        labels.each do |v|
          io << ", " if !first && key
          io << v << "=" if key
          io << "\"" << v << "\"" unless key
          key = !key
          first = false
        end
        io << "}"
      end

      private def append(io, name, value, labels = Tuple.new)
          io << "avalanchemq_"
          case name
          when Tuple
            io << name.join("_")
          else
            io << name
          end
          append_labels(io, labels) unless labels.empty?
          io << " " << value << "\n"
      end

      private def register_routes
        get "/metrics" do |context, _|
          o = context.response
          u = user(context)
          append(o, "server_info", 1, { "avalanchemq_version", AvalancheMQ::VERSION })
          append(o, "server_uptime_seconds", @amqp_server.uptime.to_i)
          append(o, "server_cpu_system_time", @amqp_server.sys_time)
          append(o, "server_cpu_system_time", @amqp_server.sys_time)
          append(o, "server_cpu_user_time", @amqp_server.user_time)
          append(o, "server_cpu_system_time", @amqp_server.sys_time)
          append(o, "server_rss_bytes", @amqp_server.rss)
          append(o, "server_disk_total_bytes", @amqp_server.disk_total)
          append(o, "server_disk_free_bytes", @amqp_server.disk_free)
          vhosts(u).each do |vhost|
            vhost.message_details.each do |k, v|
              append(o, {"vhost", k}, v, { "name", vhost.name })
            end
            vhost.exchanges.each_value do |e|
               l = { "name", e.name, "vhost", vhost.name }
               append(o, "exchange_publish_in", e.publish_in_count, l)
               append(o, "exchange_publish_out", e.publish_out_count, l)
               append(o, "exchange_unroutable", e.unroutable_count, l)
             end
             vhost.queues.each_value do |q|
               l = { "name", q.name, "vhost", vhost.name }
               append(o, "queue_ack", q.ack_count, l)
               append(o, "queue_deliver", q.deliver_count, l)
               append(o, "queue_get", q.get_count, l)
               append(o, "queue_publish", q.publish_count, l)
               append(o, "queue_redeliver", q.redeliver_count, l)
               append(o, "queue_reject", q.reject_count, l)
             end
          end
          context
        end
      end
    end
  end
end
