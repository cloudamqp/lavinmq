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
      Log = ::Log.for "MQTT.client"

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
        @metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @remote_address.to_s})
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
        @socket.close
      rescue ex : MQTT::Error::Connect
        @log.warn { "Connect error #{ex.inspect}" }
      rescue ex : ::IO::Error
        @log.warn(exception: ex) { "Read Loop error" }
        publish_will if @will
      rescue ex
        publish_will if @will
        raise ex
      ensure
        @broker.disconnect_client(client_id)

        @socket.close
        @broker.vhost.rm_connection(self)
      end

      def read_and_handle_packet
        packet : MQTT::Packet = MQTT::Packet.from_io(@io)
        @log.info { "RECIEVED PACKET:  #{packet.inspect}" }
        @recv_oct_count += packet.bytesize

        case packet
        when MQTT::Publish     then recieve_publish(packet)
        when MQTT::PubAck      then pp "puback"
        when MQTT::Subscribe   then recieve_subscribe(packet)
        when MQTT::Unsubscribe then recieve_unsubscribe(packet)
        when MQTT::PingReq     then receive_pingreq(packet)
        when MQTT::Disconnect  then return packet

        else                        raise "invalid packet type for client to send"
        end
        packet
      end

      def send(packet)
        pp "SEND PACKET: #{packet.inspect}"
        @lock.synchronize do
          pp 1
          packet.to_io(@io)
          pp 2
          @socket.flush
          pp 3
        end
        pp 4
        @send_oct_count += packet.bytesize
      end

      def receive_pingreq(packet : MQTT::PingReq)
        send MQTT::PingResp.new
      end

      def recieve_publish(packet : MQTT::Publish)
        rk = @broker.topicfilter_to_routingkey(packet.topic)
        headers = AMQ::Protocol::Table.new({
          "qos": packet.qos,
          "packet_id": packet.packet_id
        })
        props = AMQ::Protocol::Properties.new(
          headers: headers
        )
        # TODO: String.new around payload.. should be stored as Bytes
        # Send to MQTT-exchange
        msg = Message.new("mqtt.default", rk, String.new(packet.payload), props)
        @broker.vhost.publish(msg)

        # Ok to not send anything if qos = 0 (at most once delivery)
        if packet.qos > 0 && (packet_id = packet.packet_id)
          send(MQTT::PubAck.new(packet_id))
        end
      end

      def recieve_puback(packet)

      end

      def recieve_subscribe(packet : MQTT::Subscribe)
        qos = @broker.subscribe(self, packet)
        session = @broker.sessions[@client_id]
        send(MQTT::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet)
        session = @broker.sessions[@client_id]
        @broker.unsubscribe(self, packet)
        if consumer = session.consumers.find { |c| c.tag == "mqtt" }
          session.rm_consumer(consumer)
        end
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

      # TODO: actually publish will to session
      private def publish_will
        if will = @will
        end
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

    class MqttConsumer < LavinMQ::Client::Channel::Consumer
      getter unacked = 0_u32
      getter tag : String = "mqtt"
      property prefetch_count = 1

      def initialize(@client : Client, @queue : Queue)
        @has_capacity.try_send? true
        spawn deliver_loop, name: "Consumer deliver loop", same_thread: true
      end

      private def deliver_loop
        queue = @queue
        i = 0
        loop do
          queue.consume_get(self) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue LavinMQ::Queue::ClosedError
      rescue ex
        puts "deliver loop exiting: #{ex.inspect_with_backtrace}"
      end

      def details_tuple
        {
          queue: {
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
        pp "Deliver MSG: #{msg.inspect}"

        packet_id = nil
        if message_id = msg.properties.message_id
          packet_id = message_id.to_u16 unless message_id.empty?
        end

        qos = msg.properties.delivery_mode
        qos = 0u8
        # qos = 1u8
        pub_args = {
          packet_id: packet_id,
          payload:   msg.body,
          dup:       false,
          qos:       qos,
          retain:    false,
          topic:     "test",
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
