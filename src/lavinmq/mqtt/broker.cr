module LavinMQ
  module MQTT
    struct Sessions

      @queues : Hash(String, Queue)

      def initialize( @vhost : VHost)
        @queues = @vhost.queues
      end

      def []?(client_id : String) : Session?
        @queues["amq.mqtt-#{client_id}"]?.try &.as(Session)
      end

      def [](client_id : String) : Session
        @queues["amq.mqtt-#{client_id}"].as(Session)
      end

      def declare(client_id : String, clean_session : Bool)
        if session = self[client_id]?
        return session
        end
        @vhost.declare_queue("amq.mqtt-#{client_id}", !clean_session, clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
        return self[client_id]
      end

      def delete(client_id : String)
        @vhost.delete_queue("amq.mqtt-#{client_id}")
      end
    end

    class Broker

      getter vhost, sessions

      def initialize(@vhost : VHost)
        @sessions = Sessions.new(@vhost)
        @clients = Hash(String, Client).new
      end

      #remember to remove the old client entry form the hash if you replace a client. (maybe it already does?)
      def connect_client(socket, connection_info, user, vhost, packet)
        if prev_client = @clients[packet.client_id]?
          Log.trace { "Found previous client connected with client_id: #{packet.client_id}, closing" }
          prev_client.close
        end
        client = MQTT::Client.new(socket, connection_info, user, vhost, self, packet.client_id, packet.clean_session?, packet.will)
        @clients[packet.client_id] = client
        client
      end

      def subscribe(client, packet)
        name = "amq.mqtt-#{client.client_id}"
        durable = false
        auto_delete = false
        pp "clean_session: #{client.@clean_session}"
        @sessions.declare(client.client_id, client.@clean_session)
        # Handle bindings, packet.topics
      end

      def session_present?(client_id : String, clean_session) : Bool
        session = @sessions[client_id]?
        return false if session.nil? || clean_session
        true
      end

      def clear_session(client_id)
        @sessions.delete client_id
      end
    end
  end
end
