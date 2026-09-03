require "../controller"
require "../../mqtt/consts"

module LavinMQ
  module HTTP
    # Registered ahead of the exchange and binding controllers, so these routes
    # never reach them. The MQTT exchange is listed and read through the ordinary
    # GET routes, but it is created with the vhost rather than declared, and its
    # subscriptions belong to MQTT sessions, so none of it can be managed over
    # the HTTP API.
    class MQTTExchangeController < Controller
      private def register_routes
        put "/api/exchanges/:vhost/#{MQTT::EXCHANGE}" do |context, _params|
          not_supported(context, "#{MQTT::EXCHANGE} is created with the vhost")
        end

        delete "/api/exchanges/:vhost/#{MQTT::EXCHANGE}" do |context, _params|
          not_supported(context, "#{MQTT::EXCHANGE} is deleted with the vhost")
        end

        post "/api/exchanges/:vhost/#{MQTT::EXCHANGE}/publish" do |context, _params|
          not_supported(context, "Publish to #{MQTT::EXCHANGE} over MQTT")
        end

        post "/api/bindings/:vhost/e/#{MQTT::EXCHANGE}/q/:queue" do |context, _params|
          not_supported(context, "Subscribe to #{MQTT::EXCHANGE} over MQTT")
        end

        delete "/api/bindings/:vhost/e/#{MQTT::EXCHANGE}/q/:queue/*props" do |context, _params|
          not_supported(context, "Unsubscribe from #{MQTT::EXCHANGE} over MQTT")
        end

        post "/api/bindings/:vhost/e/#{MQTT::EXCHANGE}/e/:destination" do |context, _params|
          not_supported(context, "#{MQTT::EXCHANGE} routes to sessions only")
        end

        delete "/api/bindings/:vhost/e/#{MQTT::EXCHANGE}/e/:destination/*props" do |context, _params|
          not_supported(context, "#{MQTT::EXCHANGE} routes to sessions only")
        end

        post "/api/bindings/:vhost/e/:name/e/#{MQTT::EXCHANGE}" do |context, _params|
          not_supported(context, "Nothing can be bound to #{MQTT::EXCHANGE}")
        end

        delete "/api/bindings/:vhost/e/:name/e/#{MQTT::EXCHANGE}/*props" do |context, _params|
          not_supported(context, "Nothing can be bound to #{MQTT::EXCHANGE}")
        end
      end

      private def not_supported(context, message)
        halt(context, 501, {error: "not_implemented", reason: message})
      end
    end
  end
end
