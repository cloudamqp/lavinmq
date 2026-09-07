require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.session?(MQTT.session_name(client_id))
      end

      def [](client_id : String) : Session
        @vhost.session(MQTT.session_name(client_id))
      end

      # Returns nil if creating the session would exceed the vhost's max-queues
      # limit. An existing session is always returned, reusing one consumes no
      # new resource.
      def declare(client : Client) : Session?
        self[client.client_id]? || begin
          return if @vhost.queue_limit_reached?
          # The interval is the single input: it decides durability, auto-delete
          # and - carried in the arguments - survives a restart.
          interval = client.session_expiry_interval
          arguments = AMQP::Table.new({
            "x-queue-type"     => "mqtt",
            SESSION_EXPIRY_ARG => interval,
          })
          @vhost.declare_queue(MQTT.session_name(client.client_id), !interval.zero?, interval.zero?, arguments)
          self[client.client_id].client = client
          self[client.client_id]
        end
      end
    end
  end
end
