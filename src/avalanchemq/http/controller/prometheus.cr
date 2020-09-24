
require "uri"
require "../controller"
require "../resource_helpers"
require "../binding_helpers"

module AvalancheMQ
  module HTTP
    class PrometheusController < Controller
      alias MetricValue = UInt64 | Int64 | Int32
      struct Metric
        def initialize(@name : String, @value : MetricValue, @labels : Hash(String, String)?)
        end
        def initialize(@name : String, @value : MetricValue)
        end
        def to_s(io : IO)
          l = @labels.try { |l| l.map { |k,v| "#{k}=#{v}" }.join(",") }
          if l
            io.write("avalanchemq_#{@name}{#{l}} #{@value}\n".to_slice)
          else
            io.write("avalanchemq_#{@name} #{@value}\n".to_slice)
          end
        end
      end

      private def register_routes
        get "/metrics" do |context, _|
          o = context.response
          u = user(context)
          info = { "avalanchemq_version" => AvalancheMQ::VERSION }
          Metric.new("server_info", 1, info).to_s(o)
          Metric.new("server_info_uptime", @amqp_server.uptime.to_i).to_s(o)
          Metric.new("server_cpu_system_time", @amqp_server.sys_time).to_s(o)
          Metric.new("server_cpu_user_time", @amqp_server.user_time).to_s(o)
          Metric.new("server_cpu_system_time", @amqp_server.sys_time).to_s(o)
          Metric.new("server_rss_bytes", @amqp_server.rss).to_s(o)
          Metric.new("server_disk_total_bytes", @amqp_server.disk_total).to_s(o)
          Metric.new("server_disk_free_bytes", @amqp_server.disk_free).to_s(o)
          vhosts(u).each do |vhost|
            vhost.message_details.each do |k, v|
              l = { "name" => vhost.name }
              Metric.new("vhost_#{k}", v, l).to_s(o)
            end
            vhost.exchanges.each_value do |e|
              l = { "name" => e.name, "vhost" => vhost.name }
              Metric.new("exchange_publish_in", e.publish_in_count, l).to_s(o)
              Metric.new("exchange_publish_out", e.publish_out_count, l).to_s(o)
              Metric.new("exchange_unroutable", e.unroutable_count, l).to_s(o)
            end
            vhost.queues.each_value do |q|
              l = { "name" => q.name, "vhost" => vhost.name }
              Metric.new("queue_ack", q.ack_count, l).to_s(o)
              Metric.new("queue_deliver", q.deliver_count, l).to_s(o)
              Metric.new("queue_get", q.get_count, l).to_s(o)
              Metric.new("queue_publish", q.publish_count, l).to_s(o)
              Metric.new("queue_redeliver", q.redeliver_count, l).to_s(o)
              Metric.new("queue_reject", q.reject_count, l).to_s(o)
            end
          end
          context
        end
      end
    end
  end
end

