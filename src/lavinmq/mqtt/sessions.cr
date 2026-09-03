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

      # Returns nil if creating the session would exceed the vhost's max-queues
      # limit. An existing session is always returned, reusing one consumes no
      # new resource.
      def declare(client : Client) : Session?
        session = self[client.client_id]? || begin
          return if @vhost.queue_limit_reached?
          @vhost.mqtt.declare_session(name(client.client_id), client.clean_session?) ||
            self[client.client_id]
        end
        session.client = client
        session
      end

      private def name(client_id : String) : String
        "#{SESSION_PREFIX}#{client_id}"
      end
    end
  end
end
