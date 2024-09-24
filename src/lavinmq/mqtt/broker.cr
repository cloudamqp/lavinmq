module LavinMQ
  module MQTT
    class Broker

      getter vhost, sessions
      def initialize(@vhost : VHost)
        @queues = Hash(String, Session).new
        @sessions = Hash(String, Session).new
        @clients = Hash(String, Client).new
      end

      def connect_client(socket, connection_info, user, vhost, packet)
        if prev_client = @clients[packet.client_id]?
          Log.trace { "Found previous client connected with client_id: #{packet.client_id}, closing" }
          pp "rev client"
          prev_client.close
        end
        client = MQTT::Client.new(socket, connection_info, user, vhost, self, packet.client_id, packet.clean_session?, packet.will)
        @clients[packet.client_id] = client
        client
      end

      def session_present?(client_id : String, clean_session) : Bool
        session = @sessions[client_id]?
        pp "session_present? #{session.inspect}"
        return false if session.nil? || ( clean_session && session.set_clean_session )
        true
      end

      def start_session(client_id, clean_session)
        session = MQTT::Session.new(@vhost, client_id)
        session.set_clean_session if clean_session
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
