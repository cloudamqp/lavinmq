require "uri"
require "../controller"

module AvalancheMQ
  class ExchangesController < Controller
    private def register_routes
      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end

      get "/api/exchanges/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].exchanges.values.to_json(context.response)
        end
      end

      get "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          name = params["name"]
          e = @amqp_server.vhosts[vhost].exchanges[name]?
          if e
            e.to_json(context.response)
          else
            not_found(context, "Exchange #{name} does not exist")
          end
        end
      end
    end
  end
end
