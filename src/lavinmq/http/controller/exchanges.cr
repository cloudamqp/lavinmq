require "uri"
require "../controller"
require "../binding_helpers"

module LavinMQ
  module HTTP
    module ExchangeHelpers
      private def exchange(context, params, vhost, key = "name")
        name = URI.decode_www_form(params[key])
        name = "" if name == "amq.default"
        e = @amqp_server.vhosts[vhost].exchanges[name]?
        not_found(context) unless e
        e
      end
    end

    class ExchangesController < Controller
      include ExchangeHelpers
      include BindingHelpers

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/exchanges" do |context, _params|
          itr = vhosts(user(context)).flat_map &.exchanges.each_value
          page(context, itr)
        end

        get "/api/exchanges/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].exchanges.each_value)
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
            tbl = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            if delayed = body["delayed"]?
              tbl["x-delayed-exchange"] = delayed.try(&.as_bool?)
            end
            ae = tbl["x-alternate-exchange"]?.try &.as?(String)
            ae_ok = ae.nil? || (user.can_write?(vhost, ae) && user.can_read?(vhost, name))
            unless user.can_config?(vhost, name) && ae_ok
              access_refused(context, "User doesn't have permissions to declare exchange '#{name}'")
            end
            e = @amqp_server.vhosts[vhost].exchanges[name]?
            if e
              unless e.match?(type, durable, auto_delete, internal, tbl)
                bad_request(context, "Existing exchange declared with other arguments arg")
              end
              if e.internal?
                bad_request(context, "Not allowed to publish to internal exchange")
              end
              context.response.status_code = 204
            elsif NameValidator.reserved_prefix?(name)
              bad_request(context, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
            elsif name.bytesize > UInt8::MAX
              bad_request(context, "Exchange name too long, can't exceed 255 characters")
            else
              @amqp_server.vhosts[vhost]
                .declare_exchange(name, type.not_nil!, durable, auto_delete, internal, tbl)
              context.response.status_code = 201
            end
          rescue ex : Error::ExchangeTypeError
            bad_request(context, ex.message)
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
            if e.internal?
              bad_request(context, "Not allowed to delete internal exchange")
            end
            @amqp_server.vhosts[vhost].delete_exchange(e.name)
            context.response.status_code = 204
          end
        end

        get "/api/exchanges/:vhost/:name/bindings/source" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            page(context, e.bindings_details.each)
          end
        end

        get "/api/exchanges/:vhost/:name/bindings/destination" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            itr = bindings(e.vhost).select { |b| b.destination.name == e.name }
            page(context, itr)
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
            if e.internal?
              bad_request(context, "Not allowed to publish to internal exchange")
            end
            body = parse_body(context)
            properties = body["properties"]?
            routing_key = body["routing_key"]?.try(&.as_s)
            payload = body["payload"]?.try(&.as_s)
            payload_encoding = body["payload_encoding"]?.try(&.as_s)
            unless properties && routing_key && payload && payload_encoding
              bad_request(context, "Fields 'properties', 'routing_key', 'payload' and 'payload_encoding' are required")
            end
            if exp = properties["expiration"]?.try(&.as_s)
              if exp = exp.to_i?
                if exp.negative?
                  bad_request(context, "Negative expiration not allowed")
                end
              else
                bad_request(context, "Expiration not a number")
              end
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
            msg = Message.new(Time.utc.to_unix_ms,
              e.name,
              routing_key,
              AMQP::Properties.from_json(properties),
              size,
              IO::Memory.new(content))
            Log.debug { "Post to exchange=#{e.name} on vhost=#{e.vhost.name} with routing_key=#{routing_key} payload_encoding=#{payload_encoding} properties=#{properties} size=#{size}" }
            ok = e.vhost.publish(msg)
            {routed: ok}.to_json(context.response)
          end
        end
      end
    end
  end
end
