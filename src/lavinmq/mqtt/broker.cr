require "./client"
require "./consts"
require "./exchange"
require "./protocol"
require "./session"
require "./sessions"
require "./retain_store"
require "../vhost"

module LavinMQ
  module MQTT
    class Broker
      getter vhost, sessions
      # The Broker class acts as an intermediary between the MQTT client and the Vhost & Server,
      # It is initialized when starting a connection and it manages a clients connections,
      # sessions, and message exchange.
      # The broker is responsible for:
      # - Handling client connections and disconnections
      # - Managing client sessions, including clean and persistent sessions
      # - Publishing messages to the exchange
      # - Subscribing and unsubscribing clients to/from topics
      # - Handling the retain_store
      # - Interfacing with the virtual host (vhost) and the exchange to route messages.
      # The Broker class helps keep the MQTT Client concise and focused on the protocol.
      def initialize(@vhost : VHost, @replicator : Clustering::Replicator)
        @sessions = Sessions.new(@vhost)
        @clients = Hash(String, Client).new
        @retain_store = RetainStore.new(Path[@vhost.data_dir].join("mqtt_reatined_store").to_s, @replicator)
        @exchange = MQTT::Exchange.new(@vhost, EXCHANGE, @retain_store)
        @vhost.exchanges[EXCHANGE] = @exchange
      end

      def session_present?(client_id : String, clean_session) : Bool
        return false if clean_session
        session = sessions[client_id]?
        return false if session.nil? || session.clean_session?
        true
      end

      def connect_client(socket, connection_info, user, packet)
        if prev_client = @clients[packet.client_id]?
          Log.trace { "Found previous client connected with client_id: #{packet.client_id}, closing" }
          prev_client.close
        end
        client = MQTT::Client.new(socket, connection_info, user, @vhost, self, packet.client_id, packet.clean_session?, packet.will)
        if session = sessions[client.client_id]?
          if session.clean_session?
            sessions.delete session
          else
            session.client = client
          end
        end
        @clients[packet.client_id] = client
        client
      end

      def disconnect_client(client)
        client_id = client.client_id
        if session = sessions[client_id]?
          session.client = nil
          sessions.delete(client_id) if session.clean_session?
        end
        @clients.delete client_id
        vhost.rm_connection(client)
      end

      def publish(packet : MQTT::Publish)
        @exchange.publish(packet)
      end

      def subscribe(client, packet)
        unless session = sessions[client.client_id]?
          session = sessions.declare(client.client_id, client.@clean_session)
          session.client = client
        end
        qos = Array(MQTT::SubAck::ReturnCode).new(packet.topic_filters.size)
        packet.topic_filters.each do |tf|
          qos << MQTT::SubAck::ReturnCode.from_int(tf.qos)
          session.subscribe(tf.topic, tf.qos)
          @retain_store.each(tf.topic) do |topic, body|
            headers = AMQP::Table.new
            headers[RETAIN_HEADER] = true
            msg = Message.new(EXCHANGE, topic, String.new(body),
              AMQP::Properties.new(headers: headers,
                delivery_mode: tf.qos))
            session.publish(msg)
          end
        end
        qos
      end

      def unsubscribe(client, packet)
        session = sessions[client.client_id]
        packet.topics.each do |tf|
          session.unsubscribe(tf)
        end
      end

      def clear_session(client_id)
        sessions.delete client_id
      end

      def close
        @retain_store.close
      end
    end
  end
end
