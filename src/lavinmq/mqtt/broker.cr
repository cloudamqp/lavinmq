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

      # The `Broker` class acts as an intermediary between the `Server` and MQTT connections.
      # It is initialized by the `Server` and manages client connections, sessions, and message exchange.
      # Responsibilities include:
      # - Handling client connections and disconnections
      # - Managing client sessions (clean and persistent)
      # - Publishing messages to the exchange
      # - Subscribing and unsubscribing clients to/from topics
      # - Handling the retain store
      # - Interfacing with the virtual host (vhost) and the exchange to route messages
      # The `Broker` class helps keep the MQTT client concise and focused on the protocol.
      def initialize(@vhost : VHost, @replicator : Clustering::Replicator)
        @sessions = Sessions.new(@vhost)
        @clients = Hash(String, Client).new
        @retain_store = RetainStore.new(File.join(@vhost.data_dir, "mqtt_retained_store"), @replicator)
        @exchange = MQTT::Exchange.new(@vhost, EXCHANGE, @retain_store)
        @vhost.exchanges[EXCHANGE] = @exchange
      end

      def session_present?(client_id : String, clean_session) : Bool
        return false if clean_session
        session = sessions[client_id]? || return false
        return false if session.clean_session?
        true
      end

      def add_client(socket, connection_info, user, packet)
        if prev_client = @clients[packet.client_id]?
          prev_client.close("New client #{connection_info.src} (username=#{packet.username}) connected as #{packet.client_id}")
        end
        client = MQTT::Client.new(socket,
          connection_info,
          user,
          self,
          packet.client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.will)
        if session = sessions[client.client_id]?
          if client.clean_session?
            sessions.delete session
          else
            session.client = client
          end
        end
        @clients[packet.client_id] = client
      end

      def remove_client(client)
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

      def subscribe(client, topics)
        session = sessions[client.client_id]? || sessions.declare(client)
        session.client = client
        topics.map do |tf|
          session.subscribe(tf.topic, tf.qos)
          @retain_store.each(tf.topic) do |topic, body|
            headers = AMQP::Table.new({RETAIN_HEADER => true})
            msg = Message.new(EXCHANGE, topic, String.new(body),
              AMQP::Properties.new(headers: headers,
                delivery_mode: tf.qos))
            session.publish(msg)
          end
          MQTT::SubAck::ReturnCode.from_int(tf.qos)
        end
      end

      def unsubscribe(client_id, topics)
        session = sessions[client_id]
        topics.each do |tf|
          session.unsubscribe(tf)
        end
      end

      def close
        @retain_store.close
      end
    end
  end
end
