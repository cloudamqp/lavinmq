require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.session?(name(client_id))
      end

      def [](client_id : String) : Session
        @vhost.session(name(client_id))
      end

      def declare(client : Client) : Session
        session = self[client.client_id]? ||
                  @vhost.mqtt.declare_session(name(client.client_id), client.clean_session?) ||
                  self[client.client_id]
        session.client = client
        session
      end

      private def name(client_id : String) : String
        "mqtt.#{client_id}"
      end
    end
  end
end
