require "uri"
require "../controller"
require "../resource_helper"

module AvalancheMQ

  module ExchangeHelpers
    private def exchange(context, params, vhost, key = "name")
      name = params[key]
      e = @amqp_server.vhosts[vhost].exchanges[name]?
      not_found(context, "Exchange #{name} does not exist") unless e
      e
    end
  end
  class ExchangesController < Controller
    include ResourceHelper
    include ExchangeHelpers

    private def register_routes
      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts(user(context)).flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end

      get "/api/exchanges/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          @amqp_server.vhosts[vhost].exchanges.values.to_json(context.response)
        end
      end

      get "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          e.to_json(context.response)
        end
      end

      put "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          user = user(context)
          name = params["name"]
          body = parse_body(context)
          type = body["type"]?.try &.as_s
          bad_request(context, "Field 'type' is required") unless type
          durable = body["durable"]?.try(&.as_bool?) || false
          auto_delete = body["auto_delete"]?.try(&.as_bool?) || false
          internal = body["internal"]?.try(&.as_bool?) || false
          arguments = parse_arguments(body)
          ae = arguments["x-alternate-exchange"]?.try &.as?(String)
          ae_ok = ae.nil? || (user.can_write?(vhost, ae) && user.can_read?(vhost, name))
          unless user.can_config?(vhost, name) && ae_ok
            access_refused(context, "User doesn't have permissions to declare exchange '#{name}'")
          end
          e = @amqp_server.vhosts[vhost].exchanges[name]?
          if e
            unless e.match?(type, durable, auto_delete, internal, arguments)
              bad_request(context, "Existing exchange declared with other arguments arg")
            end
            context.response.status_code = 200
          elsif name.starts_with? "amq."
            bad_request(context, "Not allowed to use the amq. prefix")
          else
            @amqp_server.vhosts[vhost]
              .declare_exchange(name, type, durable, auto_delete, internal, arguments)
            context.response.status_code = 201
          end
        end
      end

      delete "/api/exchanges/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          user = user(context)
          unless user.can_config?(e.vhost.name, e.name)
            access_refused(context, "User doesn't have permissions to delete exchange '#{e.name}'")
          end
          if context.request.query_params["if-unused"]? == "true"
            bad_request(context, "Exchange #{e.name} in vhost #{e.vhost.name} in use") if e.in_use?
          end
          e.delete
          context.response.status_code = 204
        end
      end

      get "/api/exchanges/:vhost/:name/bindings/source" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          e.bindings_details.to_json(context.response)
        end
      end

      get "/api/exchanges/:vhost/:name/bindings/destination" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          all_bindings = e.vhost.exchanges.values.flat_map(&.bindings_details)
          all_bindings.select { |b| b[:destination] == e.name }.to_json(context.response)
        end
      end

      post "/api/exchanges/:vhost/:name/publish" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          refuse_unless_management(context, user, vhost)
          e = exchange(context, params, vhost)
          unless user.can_write?(e.vhost.name, e.name)
            access_refused(context, "User doesn't have permissions to write exchange '#{e.name}'")
          end
          body = parse_body(context)
          properties = body["properties"]?
          routing_key = body["routing_key"]?.try(&.as_s)
          payload = body["payload"]?.try(&.as_s)
          payload_encoding = body["payload_encoding"]?.try(&.as_s)
          unless properties && routing_key && payload && payload_encoding
            bad_request(context, "Fields 'properties', 'routing_key', 'payload' and 'payload_encoding' are required")
          end
          case payload_encoding
          when "string"
            content = payload
          when "base64"
            content = Base64.decode(payload)
          else
            bad_request(context, "Unknown payload_encoding #{payload_encoding}")
          end
          size = content.bytesize.to_u64
          msg = Message.new(Time.utc_now.epoch_ms,
                            e.name,
                            routing_key,
                            AMQP::Properties.from_json(properties),
                            size,
                            content.to_slice)
          @log.debug { "Post to exchange=#{e.name} on vhost=#{e.vhost.name} with routing_key=#{routing_key} payload_encoding=#{payload_encoding} properties=#{properties} size=#{size}" }
          ok = e.vhost.publish(msg)
          { routed: ok }.to_json(context.response)
        end
      end
    end
  end
end
