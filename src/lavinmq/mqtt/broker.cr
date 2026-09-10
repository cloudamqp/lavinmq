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
      Log = LavinMQ::Log.for "mqtt.broker"

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
        @exchange = @vhost.mqtt_exchange
      end

      # Packet ids of QoS 2 PUBLISHes answered with PUBREC and not yet released
      # by a PUBREL, per client_id. Holding the id is the whole of the
      # exactly-once guarantee on this side: a re-sent PUBLISH carrying one is
      # answered with another PUBREC and not delivered onward a second time
      # [MQTT-4.3.3-1].
      #
      # Here rather than on `Session`, because a publish-only client never gets
      # one - `Sessions#declare` is only reached from `#subscribe`. And not on
      # `Client`, because a reconnect would forget it, which is the exact
      # duplicate QoS 2 exists to prevent. In memory only, like the outbound
      # packet ids in `SessionMessageStore`.
      @qos2_received = Hash(String, Set(UInt16)).new

      # Records `packet_id` as an incomplete QoS 2 delivery. False if it was
      # already recorded, i.e. this PUBLISH is a re-send of one already
      # delivered onward.
      def qos2_publish_received?(client_id : String, packet_id : UInt16) : Bool
        ids = @qos2_received[client_id] ||= Set(UInt16).new
        # A conformant client cannot hold more unreleased ids than its own
        # in-flight window, so past the server's window this is a client
        # holding ids open to make us allocate. PacketDecode lands in
        # `read_loop`'s decode-error arm and closes the connection.
        if ids.size >= Config.instance.max_inflight_messages && !ids.includes?(packet_id)
          Log.warn { "client_id=#{client_id} holds #{ids.size} unreleased QoS 2 packet ids" }
          raise Protocol::Error::PacketDecode.new("too many unreleased QoS 2 packet ids")
        end
        ids.add?(packet_id)
      end

      # Releases `packet_id` on PUBREL. False if we were not holding it.
      def qos2_release(client_id : String, packet_id : UInt16) : Bool
        ids = @qos2_received[client_id]? || return false
        released = ids.delete(packet_id)
        @qos2_received.delete(client_id) if ids.empty?
        released
      end

      private def forget_qos2(client_id : String) : Nil
        @qos2_received.delete(client_id)
      end

      def session_present?(client_id : String, clean_session) : Bool
        return false if clean_session
        session = sessions[client_id]? || return false
        return false if session.clean_session?
        true
      end

      # A reconnecting client_id displaces the existing connection in
      # `add_client`, so the connection count doesn't grow
      def connection_limit_reached?(client_id : String) : Bool
        return false if @clients.has_key?(client_id)
        @vhost.connection_limit_reached?
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
          ProtocolVersion.from_value(packet.version),
          packet.clean_session?,
          packet.keepalive,
          packet.will)
        if client.clean_session?
          sessions[client.client_id]?.try &.delete
          # A clean session starts with no state of any kind [MQTT-3.1.2-6],
          # including the ids of QoS 2 publishes its predecessor never released.
          forget_qos2(client.client_id)
        else
          # If an existing session exists, reuse it. If no session exists
          # it will be created on first subscribe
          sessions[client.client_id]?.try &.client = client
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
            if session.clean_session?
              session.delete
              forget_qos2(client_id)
            end
          end
        else
          # No session to resume into, so there is nothing for the dedupe to
          # protect: the next CONNECT for this client_id is answered
          # session_present=false, which entitles the client to reset its own
          # half. This is also what bounds the map - without it, a client can
          # connect non-clean, publish one QoS 2 message, vanish, and repeat
          # with a fresh client_id forever. Live entries are now bounded by
          # connected clients plus persistent sessions, and those are bounded
          # by max-queues.
          forget_qos2(client_id)
        end
        @clients.delete(client_id) if @clients[client_id]? == client
        @vhost.rm_connection(client)
      end

      def publish(packet : Protocol::Publish)
        @retain_store.retain(packet) if packet.retain?
        @exchange.publish(packet)
      end

      def subscribe(client, topics) : Array(Protocol::SubAck::ReturnCode)
        session = sessions.declare(client)
        unless session
          Log.warn { "Rejecting subscribe from client_id=#{client.client_id}, queue limit in vhost '#{@vhost.name}' (#{@vhost.max_queues}) is reached" }
          return topics.map { Protocol::SubAck::ReturnCode::Failure }
        end
        headers = AMQP::Table.new({RETAIN_HEADER => true})
        topics.map do |tf|
          qos = tf.qos.zero? ? 0u8 : 1u8 # downgrade to 1 if > 1
          session.subscribe(tf.topic, qos)
          ts = RoughTime.unix_ms
          @retain_store.each(tf.topic) do |topic, body_io, body_bytesize|
            props = AMQP::Properties.new(headers: headers, delivery_mode: qos)
            msg = Message.new(ts, EXCHANGE, topic, props, body_bytesize, body_io)
            session.publish(msg)
          end
          Protocol::SubAck::ReturnCode.from_int(qos)
        end
      end

      def unsubscribe(client_id, topics)
        session = sessions[client_id]? || return
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
