require "openssl"
require "socket"
require "../client"
require "../error"

module LavinMQ
  module MQTT
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter vhost, channels, log, name, user, client_id

      @channels = Hash(UInt16, Client::Channel).new
      rate_stats({"send_oct", "recv_oct"})
      Log = ::Log.for "MQTT.client"

      def initialize(@socket : ::IO,
                     @connection_info : ConnectionInfo,
                     @vhost : VHost,
                     @user : User,
                     @client_id : String)
        @io = MQTT::IO.new(@socket)
        @lock = Mutex.new
        @remote_address = @connection_info.src
        @local_address = @connection_info.dst
        @name = "#{@remote_address} -> #{@local_address}"
        @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @remote_address.to_s})
        @log = Logger.new(Log, @metadata)
        @vhost.add_connection(self)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop
      end

      private def read_loop
        loop do
          Log.trace { "waiting for packet" }
          packet = read_and_handle_packet
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          break if packet.is_a?(MQTT::Disconnect)
        end
        rescue ex : MQTT::Error::Connect
        Log.warn { "Connect error #{ex.inspect}" }
        ensure
        @socket.close
        @vhost.rm_connection(self)
      end

      def read_and_handle_packet
        packet : MQTT::Packet = MQTT::Packet.from_io(@io)
        Log.info { "recv #{packet.inspect}" }
        @recv_oct_count += packet.bytesize

        case packet
        when MQTT::Publish     then pp "publish"
        when MQTT::PubAck      then pp "puback"
        when MQTT::Subscribe   then pp "subscribe"
        when MQTT::Unsubscribe then pp "unsubscribe"
        when MQTT::PingReq     then receive_pingreq(packet)
        when MQTT::Disconnect  then return packet
        else raise "invalid packet type for client to send"
        end
        packet
      end

      private def send(packet)
        @lock.synchronize do
          packet.to_io(@io)
          @socket.flush
        end
        @send_oct_count += packet.bytesize
      end

      def receive_pingreq(packet : MQTT::PingReq)
        send(MQTT::PingResp.new)
      end

      def details_tuple
        {
          vhost:             @vhost.name,
          user:              @user.name,
          protocol:          "MQTT",
          client_id:         @client_id,
        }.merge(stats_details)
      end

      def update_rates
      end

      def close(reason)
      end

      def force_close
      end
    end
  end
end
