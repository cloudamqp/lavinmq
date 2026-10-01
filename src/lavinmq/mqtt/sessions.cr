require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      class LimitReached < MQTT::Error; end

      # A queue that is not a session already has the session's name. AMQP and
      # the HTTP API refuse the prefix, but a definitions import does not.
      class NameTaken < MQTT::Error; end

      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.session?("#{SESSION_PREFIX}#{client_id}")
      end

      # Raises rather than returning nil, so a caller can tell the failures
      # apart. An existing session is always returned, reusing one consumes no
      # new resource.
      def declare(client_id : String, session_expiry_interval : UInt32) : Session
        if session = self[client_id]?
          return session
        end
        raise LimitReached.new if @vhost.queue_limit_reached?
        name = "#{SESSION_PREFIX}#{client_id}"
        # The interval is the single input: it decides durability, auto-delete
        # and - carried in the arguments - survives a restart.
        arguments = AMQP::Table.new({
          "x-queue-type"     => "mqtt",
          SESSION_EXPIRY_ARG => session_expiry_interval,
        })
        @vhost.declare_queue(name, !session_expiry_interval.zero?, session_expiry_interval.zero?, arguments)
        self[client_id]? || raise NameTaken.new(name)
      end
    end
  end
end
