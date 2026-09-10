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
    # Protocol level from the CONNECT packet:
    # level 3 is MQTT 3.1 (MQIsdp), level 4 is MQTT 3.1.1 (MQTT).
    enum ProtocolVersion : UInt8
      V3_1   = 3
      V3_1_1 = 4

      def name
        case self
        in .v3_1?   then "MQTT 3.1"
        in .v3_1_1? then "MQTT 3.1.1"
        end
      end
    end

    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter log, name, user, client_id, socket, connection_info
      getter? clean_session
      @connected_at = RoughTime.unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @session : MQTT::Session?
      @protocol : String
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
                     protocol_version : ProtocolVersion,
                     @clean_session : Bool = false,
                     @keepalive : UInt16 = 30,
                     @will : Protocol::Will? = nil)
        @protocol = protocol_version.name
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

      private def read_loop
        received_bytes = 0_u32
        socket = @io.io
        if socket.responds_to?(:"read_timeout=")
          # 50% grace period according to [MQTT-3.1.2-24]
          socket.read_timeout = @keepalive.zero? ? nil : (@keepalive * 1.5).seconds
        end
        loop do
          @log.trace { "waiting for packet" }
          packet = read_and_handle_packet
          if (received_bytes &+= packet.bytesize) > Config.instance.yield_each_received_bytes
            received_bytes = 0_u32
            Fiber.yield
          end
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          if packet.is_a?(Protocol::Disconnect)
            @log.debug { "Received disconnect" }
            break
          end
        end
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
        @recv_oct_count.add(packet.bytesize, :relaxed)
        vhost.add_recv_bytes(packet.bytesize.to_u64)

        case packet
        when Protocol::Publish     then recieve_publish(packet)
        when Protocol::PubAck      then recieve_puback(packet)
        when Protocol::PubRec      then recieve_pubrec(packet)
        when Protocol::PubRel      then recieve_pubrel(packet)
        when Protocol::PubComp     then recieve_pubcomp(packet)
        when Protocol::Subscribe   then recieve_subscribe(packet)
        when Protocol::Unsubscribe then recieve_unsubscribe(packet)
        when Protocol::PingReq     then receive_pingreq(packet)
        when Protocol::Disconnect  then return packet
        else                            raise "received unexpected packet: #{packet}"
        end
        packet
      end

      def send(packet)
        @lock.synchronize do
          @io.write_packet(packet)
          @io.flush
          @send_oct_count.add(packet.bytesize, :relaxed)
          vhost.add_send_bytes(packet.bytesize.to_u64)
        end
        case packet
        when Protocol::Publish
          if packet.dup?
            vhost.event_tick(EventType::ClientRedeliver)
          else
            vhost.event_tick(EventType::ClientDeliverNoAck) if packet.qos == 0
            vhost.event_tick(EventType::ClientDeliver) if packet.qos > 0
          end
        when Protocol::PubAck, Protocol::PubRec
          # One confirm per inbound publish, whichever acknowledgement it takes.
          vhost.event_tick(EventType::ClientPublishConfirm)
        end
      end

      def receive_pingreq(packet : Protocol::PingReq)
        send Protocol::PingResp.new
      end

      def recieve_publish(packet : Protocol::Publish)
        if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
          Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
          close_socket
          return
        end
        packet_id = packet.packet_id
        if packet.qos == 2 && packet_id
          # Figure 4.3: the receiver stores the packet id and initiates onward
          # delivery before answering PUBREC. Dedupe is by packet id alone -
          # `dup` is never consulted, since a first send may carry dup=1 after
          # the client's own reconnect and a re-send may carry dup=0
          # [MQTT-3.3.1-3].
          if @broker.qos2_publish_received?(@client_id, packet_id)
            @broker.publish(packet)
            vhost.event_tick(EventType::ClientPublish)
          end
          # Answered on both paths: a re-sent PUBLISH means our first PUBREC was
          # lost, and re-answering is the only way the client can move on.
          send(Protocol::PubRec.new(packet_id))
          return
        end
        @broker.publish(packet)
        vhost.event_tick(EventType::ClientPublish)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && packet_id
          send(Protocol::PubAck.new(packet_id))
        end
      end

      # PUBREC and PUBCOMP acknowledge something we sent, so without a session
      # there is nothing they can refer to. Logged and dropped rather than
      # closed: `recieve_puback`'s `close_socket` below also publishes the will,
      # because the read loop's next read then raises into the IO::Error arm.
      # The asymmetry is deliberate - see `Session#pubrec`.
      def recieve_pubrec(packet : Protocol::PubRec)
        unless session = @broker.sessions[@client_id]?
          @log.warn { "Received PubRec from client without a session" }
          return
        end
        vhost.event_tick(EventType::ClientAck) if session.pubrec(packet)
      end

      def recieve_pubcomp(packet : Protocol::PubComp)
        unless session = @broker.sessions[@client_id]?
          @log.warn { "Received PubComp from client without a session" }
          return
        end
        session.pubcomp(packet)
      end

      def recieve_pubrel(packet : Protocol::PubRel)
        id = packet.packet_id
        unless @broker.qos2_release(@client_id, id)
          # MQTT 3.1.1 leaves the answer to an unknown id implementation
          # defined, and PUBCOMP is the only one that lets the client release
          # the id at all. An unknown id here is ordinary rather than
          # exceptional: `@qos2_received` does not survive a broker restart, so
          # every resuming QoS 2 publisher arrives with one. Raising would take
          # that through `read_loop`'s rescue and publish the will of every such
          # publisher in the vhost.
          @log.debug { "PUBREL for unknown packet id '#{id}', answering PUBCOMP anyway" }
        end
        send(Protocol::PubComp.new(id))
      end

      def recieve_puback(packet : Protocol::PubAck)
        # No session means we never delivered anything to ack
        unless session = @broker.sessions[@client_id]?
          @log.warn { "Received PubAck from client without a session" }
          close_socket
          return
        end
        session.ack(packet)
        vhost.event_tick(EventType::ClientAck)
      end

      def recieve_subscribe(packet : Protocol::Subscribe)
        if Config.instance.mqtt_permission_check_enabled?
          unless user.can_read?(@broker.vhost.name, EXCHANGE) && user.can_write?(@broker.vhost.name, "mqtt.#{client_id}")
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            close_socket
            return
          end
        end
        qos = @broker.subscribe(self, packet.topic_filters)
        send(Protocol::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet : Protocol::Unsubscribe)
        @broker.unsubscribe(client_id, packet.topics)
        send(Protocol::UnsubAck.new(packet.packet_id))
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              @user.name,
          protocol:          @protocol,
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
        socket = @io
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = 1.seconds
        end
        socket.close
      rescue ::IO::Error
      end
    end
  end
end
