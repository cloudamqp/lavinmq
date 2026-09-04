require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.session?("mqtt.#{client_id}")
      end

      def [](client_id : String) : Session
        @vhost.session("mqtt.#{client_id}")
      end

      # Returns nil if creating the session would exceed the vhost's max-queues
      # limit. An existing session is always returned, reusing one consumes no
      # new resource.
      def declare(client : Client) : Session?
        self[client.client_id]? || begin
          return if @vhost.queue_limit_reached?
          @vhost.declare_queue("mqtt.#{client.client_id}", !client.@clean_session, client.@clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          self[client.client_id].client = client
          self[client.client_id]
        end
      end
    end
  end
end
