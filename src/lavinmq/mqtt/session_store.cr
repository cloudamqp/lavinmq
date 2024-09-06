#holds all sessions in a vhost
require "./session"
module LavinMQ
  module MQTT
    class SessionStore
      getter vhost, sessions
      def initialize(@vhost : VHost)
        @sessions = Hash(String, MQTT::Session).new
      end

      forward_missing_to @sessions

    end
  end
end
