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
      def initialize(@vhost : VHost)
        @sessions = Sessions.new(@vhost)
        @clients = Hash(String, Client).new
        @retain_store = RetainStore.new(File.join(@vhost.data_dir, "mqtt_retained_store"), @vhost.replicator)
        @exchange = MQTT::Exchange.new(@vhost, EXCHANGE, @retain_store)
        @vhost.register_exchange(@exchange)
      end

      # Clean Start = 1 always reports no session, because the stored one is about
      # to be discarded [MQTT-3.1.2-4].
      #
      # The auto_delete? guard is for takeover: this runs before add_client, so a
      # 0-interval session belonging to a still-connected client is visible here,
      # and that session is ended by the takeover rather than resumed (3.1.4).
      def session_present?(client_id : String, clean_start) : Bool
        return false if clean_start
        session = sessions[client_id]? || return false
        !session.auto_delete?
      end

      # v3 has no expiry property, so its clean-session bit carries both meanings:
      # 1 ends the session with the connection, 0 keeps it forever, which is what
      # LavinMQ has always done. v5 reads the property, absent meaning 0
      # [MQTT-3.1.2-11].
      private def session_expiry_interval(packet : Protocol::Connect) : UInt32
        return packet.clean_session? ? 0u32 : UInt32::MAX unless packet.version.v5?
        packet.properties.session_expiry_interval || 0u32
      end

      def add_client(io, connection_info, user, packet) : Client
        if prev_client = @clients[packet.client_id]?
          prev_client.close(
            "New client #{connection_info.remote_address} " \
            "(username=#{packet.username}) connected as #{packet.client_id}")
          remove_client(prev_client)
        end
        client = MQTT::Client.new(io,
          connection_info,
          user,
          self,
          packet.client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.will,
          packet.properties.maximum_packet_size,
          session_expiry_interval(packet))
        # Clean Start and the expiry are separate inputs: the first decides
        # whether to discard the stored session, the second how long the session
        # this connection ends up with will outlive it.
        if packet.clean_session?
          sessions[client.client_id]?.try &.delete
        else
          # Reuse an existing session, adopting this connection's interval. No
          # session yet means it is created on first subscribe.
          if session = sessions[client.client_id]?
            session.session_expiry_interval = client.session_expiry_interval
            session.client = client
          end
        end
        @clients[packet.client_id] = client
        @vhost.add_connection client
        client
      end

      def run_client(io, connection_info, user, packet) : Client
        client = add_client(io, connection_info, user, packet)
        begin
          client.run
        ensure
          remove_client(client)
        end
        client
      end

      def remove_client(client)
        client_id = client.client_id
        if session = sessions[client_id]?
          if session.client.nil? || (session.client == client)
            session.client = nil
            session.delete if session.auto_delete?
          end
        end
        @clients.delete(client_id) if @clients[client_id]? == client
        @vhost.rm_connection(client)
      end

      def publish(packet : Protocol::Publish)
        @exchange.publish(packet)
      end

      def subscribe(client, topics)
        session = sessions.declare(client)
        headers = AMQP::Table.new({RETAIN_HEADER => true})
        topics.map do |tf|
          # We only deliver up to MAX_QOS, so grant (and store/deliver at) the
          # clamped QoS - the SUBACK must report the granted max [MQTT-3.8.4-7].
          granted = Math.min(tf.qos, MAX_QOS)
          session.subscribe(tf.topic, granted)
          ts = RoughTime.unix_ms
          @retain_store.each(tf.topic) do |topic, body_io, body_bytesize|
            props = AMQP::Properties.new(headers: headers, delivery_mode: granted)
            msg = Message.new(ts, EXCHANGE, topic, props, body_bytesize, body_io)
            session.publish(msg)
          end
          Protocol::SubAck::ReasonCode.from_value(granted)
        end
      end

      def unsubscribe(client_id, topics) : Array(Protocol::UnsubAck::ReasonCode)
        session = sessions[client_id]
        topics.map do |tf|
          if session.unsubscribe(tf)
            Protocol::UnsubAck::ReasonCode::Success
          else
            Protocol::UnsubAck::ReasonCode::NoSubscriptionExisted
          end
        end
      end

      def close
        @retain_store.close
      end
    end
  end
end
