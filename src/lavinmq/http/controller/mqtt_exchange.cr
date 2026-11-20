require "../controller"
require "../binding_helpers"
require "../mqtt_exchange_compat"
require "../../name_validator"
require "./exchanges"
require "./queues"

module LavinMQ
  module HTTP
    # The exchange and binding routes for the MQTT exchange, which isn't an
    # AMQP exchange. They answer as they did when it was one, so API clients
    # keep working. Must be added before ExchangesController and
    # BindingsController, which don't find it.
    class MQTTExchangeController < Controller
      include BindingHelpers
      include ExchangeHelpers
      include QueueHelpers

      NAME = MQTT::EXCHANGE

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/exchanges/:vhost/#{NAME}" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            MQTTExchangeView.new(vhost).to_json(context.response.output)
          end
        end

        put "/api/exchanges/:vhost/#{NAME}" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            user = user(context)
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
            ae_ok = ae.nil? || (user.can_write?(vhost.name, ae) && user.can_read?(vhost.name, NAME))
            unless user.can_config?(vhost.name, NAME) && ae_ok
              access_refused(context, "User doesn't have permissions to declare exchange '#{NAME}'")
            end
            if type == "x-mqtt-topic" && !user.can_read?(vhost.name, NAME)
              access_refused(context, "User doesn't have read permissions to exchange '#{NAME}', " \
                                      "which declaring an x-mqtt-topic exchange requires")
            end
            # A redeclare must match its properties exactly
            if type != "mqtt" || durable || auto_delete || !internal || !tbl.empty?
              bad_request(context, "Existing exchange declared with other arguments arg")
            end
            context.response.status_code = 204
          end
        end

        delete "/api/exchanges/:vhost/#{NAME}" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            unless user(context).can_config?(vhost.name, NAME)
              access_refused(context, "User doesn't have permissions to delete exchange '#{NAME}'")
            end
            access_refused(context, "Can not delete exchange with prefix #{NameValidator::PREFIX_LIST}")
          end
        end

        post "/api/exchanges/:vhost/#{NAME}/publish" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            unless user.can_write?(vhost.name, NAME)
              access_refused(context, "User doesn't have permissions to write exchange '#{NAME}'")
            end
            bad_request(context, "Not allowed to publish to internal exchange")
          end
        end

        get "/api/exchanges/:vhost/#{NAME}/bindings/source" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, vhost.mqtt_exchange.bindings_details)
          end
        end

        get "/api/exchanges/:vhost/#{NAME}/bindings/destination" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            # Matched on name, so a session named mqtt.default is listed
            page(context, bindings(vhost).select { |b| b.destination.name == NAME })
          end
        end

        get "/api/bindings/:vhost/e/#{NAME}/q/:queue" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost, "queue")
            page(context, vhost.mqtt_exchange.bindings_details.select { |b| b.destination == q })
          end
        end

        post "/api/bindings/:vhost/e/#{NAME}/q/:queue" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost, "queue")
            user = user(context)
            if !user.can_read?(vhost.name, NAME)
              access_refused(context, "User doesn't have read permissions to exchange '#{NAME}'")
            elsif !user.can_write?(vhost.name, q.name)
              access_refused(context, "User doesn't have write permissions to queue '#{q.name}'")
            elsif q.internal?
              access_refused(context, "Queue '#{q.name}' is an internal queue")
            end
            body = parse_body(context)
            routing_key = body["routing_key"]?.try(&.as_s?) ||
                          body["routingKey"]?.try(&.as_s?)
            arguments = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            unless routing_key
              bad_request(context, "Field 'routing_key' is required")
            end
            # Only sessions subscribe
            access_refused(context, "Access Refused") unless q.is_a?(MQTT::Session)
            vhost.bind_queue(q.name, NAME, routing_key, arguments)
            props = AMQP::BindingKey.new(routing_key, arguments).properties_key
            context.response.headers["Location"] = q.name + "/" + props
            context.response.status_code = 201
          end
        end

        get "/api/bindings/:vhost/e/#{NAME}/q/:queue/:props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost, "queue")
            props = params["props"]
            unless binding = subscription?(vhost, q, props)
              not_found(context, "Binding '#{props}' on exchange '#{NAME}' -> exchange '#{q.name}' does not exist")
            end
            binding.to_json(context.response.output)
          end
        end

        delete "/api/bindings/:vhost/e/#{NAME}/q/:queue/*props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost, "queue")
            user = user(context)
            if !user.can_read?(vhost.name, NAME)
              access_refused(context, "User doesn't have read permissions to exchange '#{NAME}'")
            elsif !user.can_write?(vhost.name, q.name)
              access_refused(context, "User doesn't have write permissions to queue '#{q.name}'")
            end
            if binding = subscription?(vhost, q, params["props"])
              vhost.unbind_queue(q.name, NAME, binding.routing_key, binding.arguments || AMQP::Table.new)
            end
            context.response.status_code = binding ? 204 : 404
          end
        end

        # Exchange-to-exchange: the MQTT exchange can't be bound to or from,
        # so there are never bindings to find
        {"/api/bindings/:vhost/e/#{NAME}/e/:destination", "/api/bindings/:vhost/e/:name/e/#{NAME}"}.each do |path|
          get path do |context, params|
            with_vhost(context, params) do |vhost|
              refuse_unless_management(context, user(context), vhost)
              # Only for the 404 when the other exchange is missing
              amqp_exchange?(context, params, vhost, "name")
              amqp_exchange?(context, params, vhost, "destination")
              page(context, Array(AMQP::BindingDetails).new)
            end
          end

          post path do |context, params|
            with_vhost(context, params) do |vhost|
              refuse_unless_management(context, user(context), vhost)
              source = amqp_exchange?(context, params, vhost, "name")
              destination = amqp_exchange?(context, params, vhost, "destination")
              source_name = source.try(&.name) || NAME
              destination_name = destination.try(&.name) || NAME
              user = user(context)
              if !user.can_read?(vhost.name, source_name)
                access_refused(context, "User doesn't have read permissions to exchange '#{source_name}'")
              elsif !user.can_write?(vhost.name, destination_name)
                access_refused(context, "User doesn't have write permissions to exchange '#{destination_name}'")
              elsif source_name.empty? || destination_name.empty?
                access_refused(context, "Not allowed to bind to the default exchange")
              elsif destination.nil? || destination.internal?
                bad_request(context, "Not allowed to bind to an internal exchange")
              end
              body = parse_body(context)
              unless body["routing_key"]?.try(&.as_s?) || body["routingKey"]?.try(&.as_s?)
                bad_request(context, "Field 'routing_key' is required")
              end
              access_refused(context, "Access Refused")
            end
          end

          get "#{path}/:props" do |context, params|
            with_vhost(context, params) do |vhost|
              refuse_unless_management(context, user(context), vhost)
              source_name = amqp_exchange?(context, params, vhost, "name").try(&.name) || NAME
              destination_name = amqp_exchange?(context, params, vhost, "destination").try(&.name) || NAME
              not_found(context, "Binding '#{params["props"]}' on exchange '#{source_name}' -> exchange '#{destination_name}' does not exist")
            end
          end

          delete "#{path}/*props" do |context, params|
            with_vhost(context, params) do |vhost|
              refuse_unless_management(context, user(context), vhost)
              source = amqp_exchange?(context, params, vhost, "name")
              destination = amqp_exchange?(context, params, vhost, "destination")
              source_name = source.try(&.name) || NAME
              destination_name = destination.try(&.name) || NAME
              user = user(context)
              if !user.can_read?(vhost.name, source_name)
                access_refused(context, "User doesn't have read permissions to exchange '#{source_name}'")
              elsif !user.can_write?(vhost.name, destination_name)
                access_refused(context, "User doesn't have write permissions to queue '#{destination_name}'")
              elsif source_name.empty? || destination_name.empty?
                access_refused(context, "Not allowed to unbind from the default exchange")
              elsif destination.nil? || destination.internal?
                bad_request(context, "Not allowed to unbind from an internal exchange")
              end
              context.response.status_code = 404
            end
          end
        end
      end

      private def subscription?(vhost, q, props) : MQTT::SubscriptionDetails?
        return unless q.is_a?(MQTT::Session)
        vhost.session_subscriptions(q).find(&.binding_key.properties_key.== props)
      end

      # nil when the path has the MQTT exchange at *key*, else the AMQP
      # exchange there, or 404
      private def amqp_exchange?(context, params, vhost, key)
        name = params[key]?
        return if name.nil? || name == NAME
        exchange(context, params, vhost, key)
      end
    end
  end
end
