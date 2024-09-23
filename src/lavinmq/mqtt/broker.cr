module LavinMQ
  module MQTT
    class Broker

      getter vhost
      def initialize(@vhost : VHost)
        @queues = Hash(String, Session).new
        @sessions = Hash(String, Session).new
      end

      def start_session(client : Client)
        client_id = client.client_id
        session = MQTT::Session.new(self, client_id)
        @sessions[client_id] = session
        @queues[client_id] = session
      end

      def clear_session(client : Client)
        @sessions.delete client.client_id
        @queues.delete client.client_id
      end

      # def connected(client) : MQTT::Session
      #   session = Session.new(client.vhost, client.client_id)
      #   session.connect(client)
      #   session
      # end
    end
  end
end
