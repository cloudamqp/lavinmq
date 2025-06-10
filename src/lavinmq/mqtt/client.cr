require "openssl"
require "socket"
require "../client"
require "../error"
require "../rough_time"
require "./session"
require "./protocol"
require "../bool_channel"

module LavinMQ
  module MQTT
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter channels, log, name, user, client_id, socket, connection_info
      getter? clean_session
      @connected_at = RoughTime.unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @session : MQTT::Session?
      rate_stats({"send_oct", "recv_oct"})
      Log = LavinMQ::Log.for "mqtt.client"

      def vhost
        @broker.vhost
      end

      def initialize(@socket : ::IO,
                     @connection_info : ConnectionInfo,
                     @user : User,
                     @broker : MQTT::Broker,
                     @client_id : String,
                     @clean_session : Bool = false,
                     @keepalive : UInt16 = 30,
                     @will : MQTT::Will? = nil)
        @io = MQTT::IO.new(@socket)
        @lock = Mutex.new
        @waitgroup = WaitGroup.new(1)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @connection_info.remote_address.to_s, client_id: client_id})
        @log = Logger.new(Log, metadata)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop, name: "MQTT read_loop #{@connection_info.remote_address}"
      end

      def client_name
        "mqtt-client-#{@client_id}"
      end

      private def read_loop
        received_bytes = 0_u32
        socket = @socket
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
          if packet.is_a?(MQTT::Disconnect)
            @log.debug { "Received disconnect" }
            break
          end
        end
      rescue ex : ::MQTT::Protocol::Error::PacketDecode
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
        @broker.remove_client(self)
        @waitgroup.done
        close_socket
      end

      def read_and_handle_packet
        packet : MQTT::Packet = MQTT::Packet.from_io(@io)
        @log.trace { "Received packet:  #{packet.inspect}" }
        @recv_oct_count.add(packet.bytesize)

        case packet
        when MQTT::Publish     then recieve_publish(packet)
        when MQTT::PubAck      then recieve_puback(packet)
        when MQTT::Subscribe   then recieve_subscribe(packet)
        when MQTT::Unsubscribe then recieve_unsubscribe(packet)
        when MQTT::PingReq     then receive_pingreq(packet)
        when MQTT::Disconnect  then return packet
        else                        raise "received unexpected packet: #{packet}"
        end
        packet
      end

      def send(packet)
        @lock.synchronize do
          packet.to_io(@io)
          @socket.flush
          @send_oct_count.add(packet.bytesize)
        end
        case packet
        when MQTT::Publish
          if packet.dup?
            vhost.event_tick(EventType::ClientRedeliver)
          else
            vhost.event_tick(EventType::ClientDeliverNoAck) if packet.qos == 0
            vhost.event_tick(EventType::ClientDeliver) if packet.qos > 0
          end
        when MQTT::PubAck
          vhost.event_tick(EventType::ClientPublishConfirm)
        end
      end

      def receive_pingreq(packet : MQTT::PingReq)
        send MQTT::PingResp.new
      end

      def recieve_publish(packet : MQTT::Publish)
        @broker.publish(packet)
        vhost.event_tick(EventType::ClientPublish)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && (packet_id = packet.packet_id)
          send(MQTT::PubAck.new(packet_id))
        end
      end

      def recieve_puback(packet : MQTT::PubAck)
        @broker.sessions[@client_id].ack(packet)
        vhost.event_tick(EventType::ClientAck)
      end

      def recieve_subscribe(packet : MQTT::Subscribe)
        qos = @broker.subscribe(self, packet.topic_filters)
        send(MQTT::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet : MQTT::Unsubscribe)
        @broker.unsubscribe(self.client_id, packet.topics)
        send(MQTT::UnsubAck.new(packet.packet_id))
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              @user.name,
          protocol:          "MQTT 3.1.1",
          client_id:         @client_id,
          name:              @name,
          timeout:           @keepalive,
          connected_at:      @connected_at,
          state:             state,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          client_properties: NamedTuple.new,
        }.merge(stats_details)
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
          @broker.publish MQTT::Publish.new(
            topic: will.topic,
            payload: will.payload,
            packet_id: nil,
            qos: will.qos,
            retain: will.retain?,
            dup: false,
          )
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
        socket = @socket
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = 1.seconds
        end
        socket.close
      rescue ::IO::Error
      end
    end

    class Consumer < LavinMQ::Client::Channel::Consumer
      getter unacked = 0_u32
      getter tag : String
      getter has_capacity = BoolChannel.new(true)
      property prefetch_count = 0_u16

      def initialize(@client : Client, @session : MQTT::Session)
        @tag = "mqtt.#{@client.client_id}"
      end

      def details_tuple
        {
          queue: {
            name:  "mqtt.#{@client.client_id}",
            vhost: @client.vhost.name,
          },
          consumer_tag:    @tag,
          exclusive:       exclusive?,
          ack_required:    !no_ack?,
          prefetch_count:  @prefetch_count,
          priority:        priority,
          channel_details: {
            peer_host:       @client.connection_info.remote_address.address,
            peer_port:       @client.connection_info.remote_address.port,
            connection_name: @client.name,
            user:            @client.user.name,
            number:          0_u16,
            name:            "#{@client.connection_info.remote_address}[0]",
          },
        }
      end

      def no_ack?
        true
      end

      def accepts? : Bool
        true
      end

      def deliver(msg : MQTT::Publish)
        @client.send(msg)
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        raise NotImplementedError.new("MQTT Consumer can't deliver AMQP messages")
      end

      def exclusive?
        true
      end

      def cancel
        @client.close("Server force closed client")
      end

      def close
        @client.close("Server force closed client")
      end

      def closed?
        false
      end

      def flow(active : Bool)
        raise NotImplementedError.new("MQTT Consumer doesn't support flow")
      end

      def ack(sp)
        raise NotImplementedError.new("MQTT Consumer doesn't support ack")
      end

      def reject(sp, requeue = false)
        raise NotImplementedError.new("MQTT Consumer doesn't support reject")
      end

      def priority
        0
      end
    end
  end
end
