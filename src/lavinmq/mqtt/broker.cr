module LavinMQ
  module MQTT
    struct Sessions
      @queues : Hash(String, Queue)

      def initialize(@vhost : VHost)
        @queues = @vhost.queues
      end

      def []?(client_id : String) : Session?
        @queues["amq.mqtt-#{client_id}"]?.try &.as(Session)
      end

      def [](client_id : String) : Session
        @queues["amq.mqtt-#{client_id}"].as(Session)
      end

      def declare(client_id : String, clean_session : Bool)
        self[client_id]? || begin
          @vhost.declare_queue("amq.mqtt-#{client_id}", !clean_session, clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          self[client_id]
        end
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
        #retain store init, and give to exhcange
        # #one topic per file line, (use read lines etc)
        # #TODO: remember to block the mqtt namespace
        exchange = MQTTExchange.new(@vhost, "mqtt.default", true, false, true)
        @vhost.exchanges["mqtt.default"] = exchange
      end

      def session_present?(client_id : String, clean_session) : Bool
        session = @sessions[client_id]?
        return false if session.nil? || clean_session
        true
      end

      def connect_client(socket, connection_info, user, vhost, packet)
        if prev_client = @clients[packet.client_id]?
          Log.trace { "Found previous client connected with client_id: #{packet.client_id}, closing" }
          prev_client.close
        end

        client = MQTT::Client.new(socket, connection_info, user, vhost, self, packet.client_id, packet.clean_session?, packet.will)
        if session = @sessions[client.client_id]?
          session.client = client
        end
        @clients[packet.client_id] = client
        client
      end

      def disconnect_client(client_id)
        if session = @sessions[client_id]?
          session.client = nil
          sessions.delete(client_id) if session.clean_session?
        end

        @clients.delete client_id
      end

      def subscribe(client, packet)
        unless session = @sessions[client.client_id]?
          session = sessions.declare(client.client_id, client.@clean_session)
          session.client = client
        end
        qos = Array(MQTT::SubAck::ReturnCode).new(packet.topic_filters.size)
        packet.topic_filters.each do |tf|
          qos << MQTT::SubAck::ReturnCode.from_int(tf.qos)
          rk = topicfilter_to_routingkey(tf.topic)
          session.subscribe(rk, tf.qos)
        end
        publish_retained_messages packet.topic_filters
        qos
      end

      def publish_retained_messages(topic_filters)
      end


      def unsubscribe(client, packet)
        session = sessions[client.client_id]
        packet.topics.each do |tf|
          rk = topicfilter_to_routingkey(tf)
          session.unsubscribe(rk)
        end
      end

      def topicfilter_to_routingkey(tf) : String
        tf.gsub("/", ".")
      end

      def clear_session(client_id)
        sessions.delete client_id
      end
    end
  end
end
