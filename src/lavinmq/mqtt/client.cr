require "openssl"
require "socket"
require "../client"
require "../error"
require "./session"

module LavinMQ
  module MQTT
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter vhost, channels, log, name, user, client_id, socket
      @channels = Hash(UInt16, Client::Channel).new
      @session : MQTT::Session?
      rate_stats({"send_oct", "recv_oct"})
      Log = ::Log.for "mqtt.client"

      def initialize(@socket : ::IO,
                     @connection_info : ConnectionInfo,
                     @user : User,
                     @vhost : VHost,
                     @broker : MQTT::Broker,
                     @client_id : String,
                     @clean_session = false,
                     @will : MQTT::Will? = nil)
        @io = MQTT::IO.new(@socket)
        @lock = Mutex.new
        @remote_address = @connection_info.src
        @local_address = @connection_info.dst
        @name = "#{@remote_address} -> #{@local_address}"
        @metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @remote_address.to_s, client_id: client_id})
        @log = Logger.new(Log, @metadata)
        @broker.vhost.add_connection(self)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop
      end

      def client_name
        "mqtt-client"
      end

      private def read_loop
        loop do
          @log.trace { "waiting for packet" }
          packet = read_and_handle_packet
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          break if packet.is_a?(MQTT::Disconnect)
        end
      rescue ex : ::MQTT::Protocol::Error::PacketDecode
        @log.warn(exception: ex) { "Packet decode error" }
        publish_will if @will
      rescue ex : MQTT::Error::Connect
        @log.warn { "Connect error: #{ex.message}" }
      rescue ex : ::IO::Error
        @log.warn(exception: ex) { "Read Loop error" }
        publish_will if @will
      rescue ex
        publish_will if @will
        raise ex
      ensure
        @broker.disconnect_client(self)
        @socket.close
      end

      def read_and_handle_packet
        packet : MQTT::Packet = MQTT::Packet.from_io(@io)
        @log.trace { "Recieved packet:  #{packet.inspect}" }
        @recv_oct_count += packet.bytesize

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
        end
        @send_oct_count += packet.bytesize
      end

      def receive_pingreq(packet : MQTT::PingReq)
        send MQTT::PingResp.new
      end

      def recieve_publish(packet : MQTT::Publish)
        @broker.publish(packet)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && (packet_id = packet.packet_id)
          send(MQTT::PubAck.new(packet_id))
        end
      end

      def recieve_puback(packet : MQTT::PubAck)
        @broker.sessions[@client_id].ack(packet)
      end

      def recieve_subscribe(packet : MQTT::Subscribe)
        qos = @broker.subscribe(self, packet)
        send(MQTT::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet : MQTT::Unsubscribe)
        @broker.unsubscribe(self, packet)
        send(MQTT::UnsubAck.new(packet.packet_id))
      end

      def details_tuple
        {
          vhost:     @broker.vhost.name,
          user:      @user.name,
          protocol:  "MQTT",
          client_id: @client_id,
        }.merge(stats_details)
      end

      private def publish_will
        return unless will = @will
        packet = MQTT::Publish.new(
          topic: will.topic,
          payload: will.payload,
          packet_id: nil,
          qos: will.qos,
          retain: will.retain?,
          dup: false,
        )
        @broker.publish(packet)
      rescue ex
        @log.warn { "Failed to publish will: #{ex.message}" }
      end

      def update_rates
      end

      def close(reason = "")
        @log.trace { "Client#close" }
        @closed = true
        @socket.close
      end

      def force_close
      end
    end

    class Consumer < LavinMQ::Client::Channel::Consumer
      getter unacked = 0_u32
      getter tag : String = "mqtt"
      property prefetch_count = 1

      def initialize(@client : Client, @session : MQTT::Session)
        @has_capacity.try_send? true
      end

      def details_tuple
        {
          session: {
            name:  "mqtt.client_id",
            vhost: "mqtt",
          },
        }
      end

      def no_ack?
        true
      end

      def accepts? : Bool
        true
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        packet_id = nil
        if message_id = msg.properties.message_id
          packet_id = message_id.to_u16 unless message_id.empty?
        end
        retained = msg.properties.try &.headers.try &.["x-mqtt-retain"]? == true

        qos = msg.properties.delivery_mode || 0u8
        pub_args = {
          packet_id: packet_id,
          payload:   msg.body,
          dup:       redelivered,
          qos:       qos,
          retain:    retained,
          topic:     msg.routing_key.tr(".", "/"),
        }
        @client.send(::MQTT::Protocol::Publish.new(**pub_args))
        # MQTT::Protocol::PubAck.from_io(io) if pub_args[:qos].positive? && expect_response
      end

      def exclusive?
        true
      end

      def cancel
      end

      def close
      end

      def closed?
        false
      end

      def flow(active : Bool)
      end

      getter has_capacity = ::Channel(Bool).new

      def ack(sp)
      end

      def reject(sp, requeue = false)
      end

      def priority
        0
      end
    end
  end
end
