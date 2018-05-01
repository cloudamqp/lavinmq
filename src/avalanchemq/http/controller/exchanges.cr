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
          not_found(context, "Exchange #{name} does not exist") unless e
          e.to_json(context.response)
        end
      end

      put "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          name = params["name"]
          body = parse_body(context)
          type = body["type"]?
          bad_request(context, "Field 'type' is required") unless type
          durable = body["durable"].as_bool? || false
          auto_delete = body["auto_delete"].as_bool? || false
          internal = body["internal"].as_bool? || false
          if args = body["arguments"].as_h?
            arguments = AMQP.cast_to_field(args).as Hash(String, AMQP::Field)
          else
            arguments = Hash(String, AMQP::Field).new
          end
          @amqp_server.vhosts[vhost]
            .declare_exchange(name, type.as_s, durable, auto_delete, internal, arguments)
          context.response.status_code = 204
        end
      end
    end
  end
end
