module LavinMQ
  module MQTT
    class Broker

      getter vhost
      def initialize(@vhost : VHost)
        @queues = Hash(String, Session).new
        @sessions = Hash(String, Session).new
      end

      def clean_session?(client_id : String) : Bool
        session = @sessions[client_id]?
        return false if session.nil?
        session.set_clean_session
      end

      def session_present?(client_id : String, clean_session) : Bool
        session = @sessions[client_id]?
        clean_session?(client_id)
        return false if session.nil? || clean_session
        true
      end

      def start_session(client_id, clean_session)
        session = MQTT::Session.new(@vhost, client_id)
        session.clean_session if clean_session
        @sessions[client_id] = session
        @queues[client_id] = session
      end

      def clear_session(client_id)
        @sessions.delete client_id
        @queues.delete client_id
      end

      # def connected(client) : MQTT::Session
      #   session = Session.new(client.vhost, client.client_id)
      #   session.connect(client)
      #   session
      # end
    end
  end
end
