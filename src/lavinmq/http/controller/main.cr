require "../controller"
require "../../version"

module LavinMQ
  module HTTP
    class MainController < Controller
      OVERVIEW_STATS = {"ack", "deliver", "get", "deliver_get", "publish", "confirm", "redeliver", "reject", "return_unroutable"}
      EXCHANGE_TYPES = {
        {name: "direct", human: "Direct"},
        {name: "fanout", human: "Fanout"},
        {name: "topic", human: "Topic"},
        {name: "headers", human: "Headers"},
        {name: "x-federation-upstream", human: "Federation Upstream"},
        {name: "x-consistent-hash", human: "Consistent Hash"},
      }
      CHURN_STATS = {"connection_created", "connection_closed", "channel_created", "channel_closed",
                     "queue_declared", "queue_deleted"}

      def initialize(@server : LavinMQ::Server, @amqp_server : LavinMQ::AMQP::Server, @mqtt_server : LavinMQ::MQTT::Server)
        register_routes
      end

      private def register_routes
        get "/api/overview" do |context, _params|
          x_vhost = context.request.headers["x-vhost"]?
          channels, connections, exchanges, queues, bindings, consumers, ready, unacked = 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32
          recv_rate, send_rate = 0_f64, 0_f64
          log_size = LavinMQ::Config.instance.stats_log_size
          messages_log = Array(Int64).new(log_size)
          ready_log = Array(Int64).new(log_size)
          unacked_log = Array(Int64).new(log_size)
          recv_rate_log = Array(Float64).new(log_size)
          send_rate_log = Array(Float64).new(log_size)
          {% for name in OVERVIEW_STATS %}
          {{ name.id }}_count = 0_u64
          {{ name.id }}_rate = 0_f64
          {{ name.id }}_log = Array(Float64).new(log_size)
          {% end %}
          {% for name in CHURN_STATS %}
          {{ name.id }} = 0_u64
          {{ name.id }}_rate = 0_f64
          {% end %}

          unless x_vhost
            deleted_stats = @server.vhosts.deleted_stats
            {% for name in OVERVIEW_STATS %}
            {{ name.id }}_count += deleted_stats.{{ name.id }}
            {% end %}
            {% for name in CHURN_STATS %}
            {{ name.id }} += deleted_stats.{{ name.id }}
            {% end %}
          end

          vhosts(user(context)).each do |vhost|
            next if x_vhost && vhost.name != x_vhost
            vhost.each_connection do |c|
              connections += 1
              channels += c.channel_count
              consumers += c.channels.sum &.consumers_size
            end
            exchanges += vhost.exchanges_size
            queues += vhost.queues_size
            queues += vhost.sessions_size
            vhost.each_exchange { |e| bindings += e.binding_count }
            vhost.each_queue do |q|
              ready += q.message_count
              unacked += q.unacked_count
            end
            vhost.each_session do |s|
              ready += s.message_count
              unacked += s.unacked_count
            end
            vhost.add_messages_ready_log(ready_log)
            vhost.add_messages_ready_log(messages_log)
            vhost.add_messages_unacknowledged_log(unacked_log)
            vhost.add_messages_unacknowledged_log(messages_log)
            vhost_stats = vhost.current_stats_details
            recv_rate += vhost_stats[:recv_oct_details][:rate]
            send_rate += vhost_stats[:send_oct_details][:rate]
            vhost.add_recv_oct_log(recv_rate_log)
            vhost.add_send_oct_log(send_rate_log)
            {% for sm in OVERVIEW_STATS %}
              {{ sm.id }}_count += vhost_stats[:{{ sm.id }}]
              {{ sm.id }}_rate += vhost_stats[:{{ sm.id }}_details][:rate]
              vhost.add_{{ sm.id }}_log({{ sm.id }}_log)
            {% end %}
            {% for sm in CHURN_STATS %}
            {{ sm.id }} += vhost_stats[:{{ sm.id }}]
            {{ sm.id }}_rate += vhost_stats[:{{ sm.id }}_details][:rate]
            {% end %}
          end
          {
            lavinmq_version: LavinMQ::VERSION,
            product_name:    "LavinMQ",
            node:            System.hostname,
            uptime:          @server.uptime.to_i,
            object_totals:   {
              channels:    channels,
              connections: connections,
              consumers:   consumers,
              exchanges:   exchanges,
              queues:      queues,
              bindings:    bindings,
            },
            queue_totals: {
              messages:                    ready + unacked,
              messages_ready:              ready,
              messages_unacknowledged:     unacked,
              messages_log:                messages_log,
              messages_ready_log:          ready_log,
              messages_unacknowledged_log: unacked_log,
            },
            recv_oct_details: {
              rate: recv_rate,
              log:  recv_rate_log,
            },
            send_oct_details: {
              rate: send_rate,
              log:  send_rate_log,
            },
            message_stats: {% begin %} {
              {% for name in OVERVIEW_STATS %}
              {{ name.id }}: {{ name.id }}_count,
              {{ name.id }}_details: {
                rate: {{ name.id }}_rate,
                log: {{ name.id }}_log,
              },
            {% end %} } {% end %},
            churn_rates: {% begin %} {
              {% for name in CHURN_STATS %}
              {{ name.id }}: {{ name.id }},
              {{ name.id }}_details: {
                rate: {{ name.id }}_rate
              },
            {% end %} } {% end %},
            listeners:      @amqp_server.listeners + @mqtt_server.listeners,
            exchange_types: EXCHANGE_TYPES.map { |t| {name: t[:name], human: t[:human]} },
          }.to_json(context.response)
          context
        end

        get "/api/whoami" do |context, _params|
          user(context).user_details.to_json(context.response)
          context
        end

        get "/api/aliveness-test/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            vhost.declare_queue("aliveness-test", false, false)
            vhost.bind_queue("aliveness-test", "amq.direct", "aliveness-test")
            msg = Message.new(Time.utc.to_unix_ms,
              "amq.direct",
              "aliveness-test",
              AMQP::Properties.new,
              4_u64,
              IO::Memory.new("test"))
            routed = vhost.publish(msg).routed?
            env = nil
            vhost.queue("aliveness-test").basic_get(true) { |e| env = e }
            ok = routed && env && String.new(env.message.body) == "test"
            {status: ok ? "ok" : "failed"}.to_json(context.response)
          end
        end

        get "/api/federation-links" do |context, _params|
          arr = vhosts(user(context)).flat_map do |vhost|
            vhost.upstreams.not_nil!.flat_map do |upstream|
              upstream.links
            end
          end
          page(context, arr)
        end

        get "/api/federation-links/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            arr = vhost.upstreams.not_nil!.flat_map do |upstream|
              upstream.links
            end
            page(context, arr)
          end
        end

        get "/api/extensions" do |context, _params|
          Tuple.new.to_json(context.response)
          context
        end
      end
    end
  end
end
