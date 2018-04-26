
module AvalancheMQ

  class MainController < Controller
    private def register_routes
      get "/api/overview" do |context, _params|
        {
          "avalanchemq_version": AvalancheMQ::VERSION,
          "object_totals": {
            "channels": @amqp_server.connections.reduce(0) { |memo, i| memo + i.channels.size },
            "connections": @amqp_server.connections.size,
            "consumers": nr_of_consumers,
            "exchanges": @amqp_server.vhosts.reduce(0) { |memo, i| memo + i.exchanges.size },
            "queues": @amqp_server.vhosts.reduce(0) { |memo, i| memo + i.queues.size },
          },
          "listeners": @amqp_server.listeners
        }.to_json(context.response)
        context
      end

      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end
      get "/api/queues" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.queues.values }.to_json(context.response)
        context
      end
      get "/api/policies" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.policies.values }.to_json(context.response)
        context
      end
      get "/api/vhosts" do |context, _params|
        @amqp_server.vhosts.to_json(context.response)
        context
      end

      post "/api/parameters" do |context, _params|
        p = Parameter.from_json context.request.body.not_nil!
        @amqp_server.add_parameter p
        context
      end
      post "/api/policies" do |context, _params|
        p = Policy.from_json context.request.body.not_nil!
        vhost = @amqp_server.vhosts[p.vhost]? || nil
        raise HTTPServer::NotFoundError.new("No vhost named #{p.vhost}") unless vhost
        vhost.add_policy(p)
        context
      end
      post "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.create(body["name"].as_s)
        context
      end

      delete "/api/policies" do |context, _params|
        body = parse_body(context)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
          raise HTTPServer::NotFoundError.new("No vhost named #{body["vhost"].as_s}") unless vhost
        vhost.delete_policy(body["name"].as_s)
        context
      end
      delete "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.delete(body["name"].as_s)
        context
      end
    end

    private def nr_of_consumers
      @amqp_server.connections.reduce(0) do |memo_i, i|
        memo_i + i.channels.values.reduce(0) { |memo_j, j| memo_j + j.consumers.size }
      end
    end
  end
end
