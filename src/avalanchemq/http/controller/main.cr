require "../controller"

module AvalancheMQ
  class MainController < Controller
    private def register_routes
      get "/api/overview" do |context, _params|
        {
          "avalanchemq_version": AvalancheMQ::VERSION,
          "object_totals":       {
            "channels":    @amqp_server.connections.reduce(0) { |memo, i| memo + i.channels.size },
            "connections": @amqp_server.connections.size,
            "consumers":   nr_of_consumers,
            "exchanges":   @amqp_server.vhosts.reduce(0) { |memo, i| memo + i.exchanges.size },
            "queues":      @amqp_server.vhosts.reduce(0) { |memo, i| memo + i.queues.size },
          },
          "listeners":      @amqp_server.listeners,
          "exchange_types": Exchange.types.map { |name| {"name": name} },
        }.to_json(context.response)
        context
      end

      get "/api/whoami" do |context, params|
        user(context).user_details.to_json(context.response)
        context
      end

      get "/api/aliveness-test/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].declare_queue("aliveness-test", false, false)
          @amqp_server.vhosts[vhost].bind_queue("aliveness-test", "amq.direct", "aliveness-test")
          msg = Message.new(Time.utc_now.epoch_ms,
            "amq.direct",
            "aliveness-test",
            AMQP::Properties.new,
            4_u64,
            "test".to_slice)
          ok = @amqp_server.vhosts[vhost].publish(msg)
          env = @amqp_server.vhosts[vhost].queues["aliveness-test"].get(true)
          ok = env && String.new(env.message.body) == "test"
          {status: ok ? "ok" : "failed"}.to_json(context.response)
        end
      end
    end

    private def nr_of_consumers
      @amqp_server.connections.reduce(0) do |memo_i, i|
        memo_i + i.channels.values.reduce(0) { |memo_j, j| memo_j + j.consumers.size }
      end
    end
  end
end
