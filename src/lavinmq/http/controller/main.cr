require "../controller"
require "../../version"

module LavinMQ
  module HTTP
    module StatsHelpers
      def add_logs!(logs_a, logs_b)
        until logs_a.size >= logs_b.size
          logs_a.unshift 0
        end
        until logs_b.size >= logs_a.size
          logs_b.unshift 0
        end
        logs_a.size.times do |i|
          logs_a[i] += logs_b[i]
        end
        logs_a
      end

      private def add_logs(logs_a, logs_b)
        add_logs!(logs_a.dup, logs_b)
      end
    end

    class MainController < Controller
      include StatsHelpers
      OVERVIEW_STATS = {"ack", "deliver", "get", "publish", "confirm", "redeliver", "reject"}
      EXCHANGE_TYPES = {"direct", "fanout", "topic", "headers", "x-federation-upstream", "x-consistent-hash"}

      private def register_routes
        get "/" do |context, _params|
          pagename = "Overview"
          ECR.embed "./views/overview.ecr", context.response
          context
        end

        get "/api/overview" do |context, _params|
          x_vhost = context.request.headers["x-vhost"]?
          channels, connections, exchanges, queues, consumers, ready, unacked = 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32, 0_u32
          recv_rate, send_rate = 0_f64, 0_f64
          ready_log = Deque(UInt32).new(LavinMQ::Config.instance.stats_log_size)
          unacked_log = Deque(UInt32).new(LavinMQ::Config.instance.stats_log_size)
          recv_rate_log = Deque(Float64).new(LavinMQ::Config.instance.stats_log_size)
          send_rate_log = Deque(Float64).new(LavinMQ::Config.instance.stats_log_size)
          {% for name in OVERVIEW_STATS %}
          {{name.id}}_count = 0_u64
          {{name.id}}_rate = 0_f64
          {{name.id}}_log = Deque(Float64).new(LavinMQ::Config.instance.stats_log_size)
          {% end %}

          vhosts(user(context)).each do |vhost|
            next if x_vhost && vhost.name != x_vhost
            vhost.connections.each do |c|
              connections += 1
              channels += c.channels.size
              consumers += c.channels.each_value.sum &.consumers.size
              recv_rate += c.stats_details[:recv_oct_details][:rate]
              send_rate += c.stats_details[:send_oct_details][:rate]
              add_logs!(recv_rate_log, c.stats_details[:recv_oct_details][:log])
              add_logs!(send_rate_log, c.stats_details[:send_oct_details][:log])
            end
            exchanges += vhost.exchanges.size
            queues += vhost.queues.size
            vhost.queues.each_value do |q|
              ready += q.message_count
              unacked += q.unacked_count
              add_logs!(ready_log, q.message_count_log)
              add_logs!(unacked_log, q.unacked_count_log)
            end
            vhost_stats_details = vhost.stats_details
            {% for sm in OVERVIEW_STATS %}
              {{sm.id}}_count += vhost_stats_details[:{{sm.id}}]
              {{sm.id}}_rate += vhost_stats_details[:{{sm.id}}_details][:rate]
              add_logs!({{sm.id}}_log, vhost_stats_details[:{{sm.id}}_details][:log])
            {% end %}
          end
          {
            lavinmq_version: LavinMQ::VERSION,
            node:            System.hostname,
            uptime:          @amqp_server.uptime.to_i,
            object_totals:   {
              channels:    channels,
              connections: connections,
              consumers:   consumers,
              exchanges:   exchanges,
              queues:      queues,
            },
            queue_totals: {
              messages:                    ready + unacked,
              messages_ready:              ready,
              messages_unacknowledged:     unacked,
              messages_log:                add_logs(ready_log, unacked_log),
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
              {{name.id}}: {{name.id}}_count,
              {{name.id}}_details: {
                rate: {{name.id}}_rate,
                log: {{name.id}}_log,
              },
            {% end %} } {% end %},
            listeners:      @amqp_server.listeners,
            exchange_types: EXCHANGE_TYPES.map { |name| {name: name} },
          }.to_json(context.response)
          context
        end

        get "/api/whoami" do |context, _params|
          user(context).user_details.to_json(context.response)
          context
        end

        get "/api/aliveness-test/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            @amqp_server.vhosts[vhost].declare_queue("aliveness-test", false, false)
            @amqp_server.vhosts[vhost].bind_queue("aliveness-test", "amq.direct", "aliveness-test")
            msg = Message.new(Time.utc.to_unix_ms,
              "amq.direct",
              "aliveness-test",
              AMQP::Properties.new,
              4_u64,
              IO::Memory.new("test"))
            ok = @amqp_server.vhosts[vhost].publish(msg)
            env = nil
            @amqp_server.vhosts[vhost].queues["aliveness-test"].basic_get(true) { |e| env = e }
            ok = ok && env && String.new(env.message.body) == "test"
            {status: ok ? "ok" : "failed"}.to_json(context.response)
          end
        end

        get "/api/shovels" do |context, _params|
          itrs = vhosts(user(context)).flat_map do |v|
            v.shovels.not_nil!.each_value
          end
          page(context, itrs)
        end

        get "/api/shovels/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            page(context, @amqp_server.vhosts[vhost].shovels.not_nil!.each_value)
          end
        end

        get "/api/federation-links" do |context, _params|
          itrs = vhosts(user(context)).flat_map do |vhost|
            vhost.upstreams.not_nil!.flat_map do |upstream|
              upstream.links.each
            end
          end
          page(context, itrs)
        end

        get "/api/federation-links/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            itrs = @amqp_server.vhosts[vhost].upstreams.not_nil!.map do |upstream|
              upstream.links.each
            end
            page(context, Iterator(Federation::Upstream::Link).chain(itrs))
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
