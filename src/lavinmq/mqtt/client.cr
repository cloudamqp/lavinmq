require "openssl"
require "socket"
require "../client"
require "../error"
require "../rough_time"
require "./session"
require "./protocol"
require "../bool_channel"
require "./consts"
require "../stats"

module LavinMQ
  module MQTT
    # Raised by a packet handler when the connection must be torn down with a
    # reason code. Caught centrally in Client#read_loop, which sends a v5
    # DISCONNECT carrying the reason (v3 has no server DISCONNECT, so it just
    # closes).
    class ProtocolViolation < Exception
      getter reason : Protocol::Disconnect::ReasonCode

      def initialize(@reason : Protocol::Disconnect::ReasonCode)
        super(@reason.to_s)
      end
    end

    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter log, name, user, client_id, socket, connection_info
      # The client's advertised Maximum Packet Size (v5); nil = no limit. Used to
      # enforce [MQTT-3.1.2-24] on outbound packets in the session delivery path.
      getter max_packet_size : UInt32?
      getter? clean_session
      @connected_at = RoughTime.unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @session : MQTT::Session?
      rate_stats({"send_oct", "recv_oct"})
      Log = LavinMQ::Log.for "mqtt.client"

      def vhost
        @broker.vhost
      end

      # Stub channel accessors for polymorphic dispatch with AMQP::Client

      def channel_count : Int32
        0
      end

      def each_channel(& : LavinMQ::Client::Channel ->) : Nil
      end

      def channels : Array(LavinMQ::Client::Channel)
        [] of LavinMQ::Client::Channel
      end

      def channel?(id : UInt16) : LavinMQ::Client::Channel?
        nil
      end

      def initialize(@io : Protocol::IO,
                     @connection_info : ConnectionInfo,
                     @user : Auth::BaseUser,
                     @broker : MQTT::Broker,
                     @client_id : String,
                     @clean_session : Bool = false,
                     @keepalive : UInt16 = 30,
                     @will : Protocol::Will? = nil,
                     @max_packet_size : UInt32? = nil)
        @lock = Mutex.new
        @waitgroup = WaitGroup.new(1)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @connection_info.remote_address.to_s, client_id: client_id})
        @log = Logger.new(Log, metadata)
      end

      def run : Nil
        @log.info { "Connection established for user=#{@user.name}" }
        case user = @user
        when Auth::OAuthUser
          user.on_expiration do
            close("token expired")
          end
        end
        read_loop
      end

      def client_name
        "mqtt-client-#{@client_id}"
      end

      private def protocol_name : String
        case @io.version
        when .v5?   then "MQTT 5.0"
        when .v3_1? then "MQTT 3.1"
        else             "MQTT 3.1.1"
        end
      end

      private def apply_keepalive_timeout
        socket = @io.io
        return unless socket.responds_to?(:"read_timeout=")
        # 50% grace period according to [MQTT-3.1.2-24]
        socket.read_timeout = @keepalive.zero? ? nil : (@keepalive * 1.5).seconds
      end

      private def read_loop
        received_bytes = 0_u32
        apply_keepalive_timeout
        loop do
          @log.trace { "waiting for packet" }
          packet, bytesize = read_and_handle_packet
          if (received_bytes &+= bytesize) > Config.instance.yield_each_received_bytes
            received_bytes = 0_u32
            Fiber.yield
          end
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          if packet.is_a?(Protocol::Disconnect)
            @log.debug { "Received disconnect: #{packet.reason_code}" }
            # Only reason 0x00 discards the will [MQTT-3.14.4-3]. 0x04
            # (DisconnectWithWillMessage) and every error code publish it.
            publish_will unless packet.reason_code.normal_disconnection?
            break
          end
        end
      rescue ex : ProtocolViolation
        @log.warn { "Protocol violation, disconnecting client: #{ex.reason}" }
        disconnect(ex.reason)
        publish_will
      rescue ex : Protocol::Error::ProtocolError
        # The shard raises this (with a reason byte) for codec-level protocol
        # violations, e.g. an empty PUBLISH topic with no alias (0x82). Map it to
        # a v5 server DISCONNECT; v3 just closes.
        @log.warn { "Protocol error, disconnecting client: #{ex.message}" }
        disconnect(disconnect_reason(ex.reason_code))
        publish_will
      rescue ex : Protocol::Error::PacketDecode
        @log.warn(exception: ex) { "Packet decode error" }
        publish_will
      rescue ex : ::IO::TimeoutError
        @log.warn { "Keepalive timeout (keepalive:#{@keepalive}): #{ex.message}" }
        publish_will
      rescue ex : ::IO::Error
        @log.error { "Client unexpectedly closed connection: #{ex.message}" } unless @closed
        publish_will
      rescue ex
        @log.error(exception: ex) { "Read Loop error" }
        publish_will
      ensure
        case user = @user
        when Auth::OAuthUser
          user.cleanup
        end
        @waitgroup.done
        close_socket
        @log.info { "Connection disconnected for user=#{@user.name} duration=#{duration}" }
      end

      private def duration
        ms = RoughTime.unix_ms - @connected_at
        seconds = (ms / 1000).round.to_i
        Time::Span.new(seconds: seconds)
      end

      def read_and_handle_packet
        packet = @io.read_packet
        @log.trace { "Received packet:  #{packet.inspect}" }
        bytesize = @io.bytesize(packet)
        @recv_oct_count.add(bytesize, :relaxed)
        vhost.add_recv_bytes(bytesize.to_u64)

        case packet
        when Protocol::Publish     then recieve_publish(packet)
        when Protocol::PubAck      then recieve_puback(packet)
        when Protocol::Subscribe   then recieve_subscribe(packet)
        when Protocol::Unsubscribe then recieve_unsubscribe(packet)
        when Protocol::PingReq     then receive_pingreq(packet)
        when Protocol::Disconnect  then return {packet, bytesize}
        else                            raise "received unexpected packet: #{packet}"
        end
        {packet, bytesize}
      end

      def send(packet)
        @lock.synchronize do
          @io.write_packet(packet)
          @io.flush
          bytesize = @io.bytesize(packet)
          @send_oct_count.add(bytesize, :relaxed)
          vhost.add_send_bytes(bytesize.to_u64)
        end
        case packet
        when Protocol::Publish
          if packet.dup?
            vhost.event_tick(EventType::ClientRedeliver)
          else
            vhost.event_tick(EventType::ClientDeliverNoAck) if packet.qos == 0
            vhost.event_tick(EventType::ClientDeliver) if packet.qos > 0
          end
        when Protocol::PubAck
          vhost.event_tick(EventType::ClientPublishConfirm)
        end
      end

      # Server-initiated disconnect. v5 clients get a DISCONNECT carrying the
      # reason code; v3 has no server DISCONNECT packet, so we just let the
      # caller's cleanup close the socket. The socket close itself happens in
      # read_loop's ensure block.
      private def disconnect(reason : Protocol::Disconnect::ReasonCode)
        send(Protocol::Disconnect.new(reason)) if @io.version.v5?
      rescue ::IO::Error
        # peer may already be gone; read_loop's ensure still closes the socket
      end

      # Map a shard reason byte to a DISCONNECT reason code, defaulting to a
      # generic protocol error if it isn't a known DISCONNECT code.
      private def disconnect_reason(reason_byte : UInt8) : Protocol::Disconnect::ReasonCode
        Protocol::Disconnect::ReasonCode.from_value?(reason_byte) ||
          Protocol::Disconnect::ReasonCode::ProtocolError
      end

      def receive_pingreq(packet : Protocol::PingReq)
        send Protocol::PingResp.new
      end

      # Enforce the v5 limits we advertised in CONNACK. A conformant client
      # honours them, so a violation is a protocol error -> server DISCONNECT
      # (raised as ProtocolViolation, handled in read_loop). v3 has no such
      # contract and is unaffected.
      private def validate_v5_publish!(packet : Protocol::Publish)
        return unless @io.version.v5?
        # maximum_qos=1: QoS 2 is not supported (v3 downgrades on delivery instead).
        if packet.qos > MAX_QOS
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::QoSNotSupported)
        end
        # topic_alias_maximum=0: we accept no Topic Aliases.
        if packet.properties.topic_alias
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::TopicAliasInvalid)
        end
        # (An empty topic with no alias is rejected by the shard on decode with a
        # ProtocolError 0x82, mapped to a server DISCONNECT in read_loop.)
      end

      def recieve_publish(packet : Protocol::Publish)
        validate_v5_publish!(packet)
        if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
          Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
          return refuse_publish(packet)
        end
        matched = @broker.publish(packet)
        vhost.event_tick(EventType::ClientPublish)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && (packet_id = packet.packet_id)
          # 0x10 lets the publisher see that nothing was subscribed (3.4.2.1).
          # The shard drops the reason tail on v3, so no version branch here.
          reason = matched.zero? ? Protocol::PubAck::ReasonCode::NoMatchingSubscribers : Protocol::PubAck::ReasonCode::Success
          send(Protocol::PubAck.new(packet_id, reason))
        end
      end

      # An unauthorized PUBLISH gets a reason code instead of a bare TCP close:
      # PUBACK 0x87 when there is an ack to carry it, otherwise a server
      # DISCONNECT 0x87 (spec 3.3.4). v3 has no way to say why, so it just closes.
      private def refuse_publish(packet : Protocol::Publish) : Nil
        unless @io.version.v5?
          close_socket
          return
        end
        if packet.qos > 0 && (packet_id = packet.packet_id)
          send(Protocol::PubAck.new(packet_id, Protocol::PubAck::ReasonCode::NotAuthorized))
        else
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::NotAuthorized)
        end
      end

      def recieve_puback(packet : Protocol::PubAck)
        # A non-success PUBACK still terminates the QoS 1 delivery (3.4.2.1), so
        # the message is acked either way and the code is purely diagnostic.
        unless packet.reason_code.success?
          @log.warn { "PUBACK for packet id #{packet.packet_id} with reason #{packet.reason_code}" }
        end
        @broker.sessions[@client_id].ack(packet)
        vhost.event_tick(EventType::ClientAck)
      end

      # Enforce the v5 SUBSCRIBE limits we advertised in CONNACK. Both are
      # packet-level protocol errors -> server DISCONNECT (spec 3.2.2.3.12 /
      # 3.2.2.3.13), raised via ProtocolViolation and handled in read_loop.
      private def validate_v5_subscribe!(packet : Protocol::Subscribe)
        return unless @io.version.v5?
        # subscription_identifier_available=0
        if packet.properties.subscription_identifier
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::SubscriptionIdentifiersNotSupported)
        end
        # shared_subscription_available=0: any $share/ filter fails the whole packet.
        if packet.topic_filters.any?(&.topic.starts_with?("$share/"))
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::SharedSubscriptionsNotSupported)
        end
      end

      def recieve_subscribe(packet : Protocol::Subscribe)
        validate_v5_subscribe!(packet)
        if Config.instance.mqtt_permission_check_enabled?
          unless user.can_read?(@broker.vhost.name, EXCHANGE) && user.can_write?(@broker.vhost.name, "mqtt.#{client_id}")
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            # A v3 SUBACK can only say 0x00-0x02 or 0x80, so v3 keeps closing
            # without an explanation.
            if @io.version.v5?
              codes = Array.new(packet.topic_filters.size, Protocol::SubAck::ReasonCode::NotAuthorized)
              send(Protocol::SubAck.new(codes, packet.packet_id))
            else
              close_socket
            end
            return
          end
        end
        qos = @broker.subscribe(self, packet.topic_filters)
        send(Protocol::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet : Protocol::Unsubscribe)
        reason_codes = @broker.unsubscribe(client_id, packet.topics)
        # v5 UNSUBACK carries a reason code per topic filter; the shard drops
        # the payload on v3, so no version branch is needed here.
        send(Protocol::UnsubAck.new(packet.packet_id, reason_codes))
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              @user.name,
          protocol:          protocol_name,
          client_id:         @client_id,
          name:              @name,
          timeout:           @keepalive,
          connected_at:      @connected_at,
          state:             state,
          host:              @connection_info.local_address.address,
          port:              @connection_info.local_address.port,
          peer_host:         @connection_info.remote_address.address,
          peer_port:         @connection_info.remote_address.port,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          client_properties: NamedTuple.new,
        }.merge(current_stats_details)
      end

      def to_json(json : JSON::Builder)
        details_tuple.merge(stats_details).to_json(json)
      end

      def search_match?(value : String) : Bool
        @name.includes?(value) ||
          @user.name.includes?(value)
      end

      def search_match?(value : Regex) : Bool
        value === @name ||
          value === @user.name
      end

      private def publish_will
        if will = @will
          if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            return
          end
          @broker.publish(Protocol::Publish.new(
            topic: will.topic,
            payload: will.payload,
            packet_id: nil,
            qos: will.qos,
            retain: will.retain?,
            dup: false,
          ))
        end
      rescue ex
        @log.warn { "Failed to publish will: #{ex.message}" }
      end

      # should only be used when server needs to froce close client
      def close(reason = "")
        return if @closed
        @log.info { "Closing connection: #{reason}" }
        @closed = true
        close_socket
        @waitgroup.wait
      end

      def state
        @closed ? "closed" : (@broker.vhost.flow? ? "running" : "flow")
      end

      def force_close
        close_socket
      end

      private def close_socket
        socket = @io.io
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = 1.seconds
        end
        socket.close
      rescue ::IO::Error
      end
    end
  end
end
