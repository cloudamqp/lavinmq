require "uri"
require "socket"

module AvalancheMQ
  class Shovel
    enum DeleteAfter
      Never
      QueueLength
    end

    record Source, uri : URI, queue : String?, exchange : String?,
      exchange_key : String?, delete_after : DeleteAfter
    record Destination, uri : URI, queue : String?, exchange : String?,
      exchange_key : String?

    def initialize(@source : Source, @destination : Destination)
    end

    def run
      destination = Connection.new(@destination.uri)
      source = Connection.new(@source.uri)
      source.subscribe(@source.queue) do |d, h, p|
        destination.publish(d, h, p)
      end
    end

    def stop
    end

    class Connection
      def initialize(@uri)
        @socket = TCPSocket.new(@uri.host, @uri.port || 5672)
        @socket.sync = true
        @socket.keepalive = true
        @socket.tcp_nodelay = true
        @socket.tcp_keepalive_idle = 60
        @socket.tcp_keepalive_count = 3
        @socket.tcp_keepalive_interval = 10
        @socket.write_timeout = 15
        @socket.recv_buffer_size = 131072
        negotiate_connection
        open_channel
      end

      def publish(exchange, routing_key, properties, payload)
        @socket.write AMQP::Basic::Publish.new(1_u16, 0_u16, exchange, routing_key,
                                               false, false).to_slice
        @socket.write AMQP::HeaderFrame.new(1_u16, 0_u16, 0_u16,
                                            payload.bytesize, properties).to_slice
        @socket.write AMQP::Body.new(1_u16, payload).to_slice
      end

      def subscribe(queue, &block)
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, 1000_u16, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
        @socket.write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
                                               false, false, false, true,
                                               {} of String => AMQP::Field).to_slice

        delivery_tag = ""
        properties = Properties.new
        body_size = 0_u64
        body = IO::Memory.new(body_size)
        loop do
          frame = AMQP::Frame.decode(@socket)
          case frame
          when AMQP::Basic::Deliver
            delivery_tag = frame.delivery_tag
          when AMQP::HeaderFrame
            body_size = frame.body_size
            properties = frame.properties
            body = IO::Memory.new(body_size)
          when AMQP::BodyFrame
            body.write frame.body
            if body.pos == body_size
              yield properties, body
              body.clear
              ack = AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
              @socket.write ack.to_slice
            end
          when AMQP::Channel::Close
            puts "Server unexpectedly sent #{frame}"
            @socket.write AMQP::Channel::CloseOk.new.to_slice
            @socket.write AMQP::Connection::Close.new(320_u16,
                                                      "channel closed unexpectedly",
                                                      0_u16, 0_u16).to_slice
          when AMQP::Connection::Close
            puts "Server unexpectedly sent #{frame}"
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            @socket.close
          when AMQP::Connection::CloseOk
            @socket.close
          else
            raise "Unexpected frame #{frame}"
          end
        end
      end

      def disconnect
        @socket.close
      end

      def negotiate_connection
        @socket.write AMQP::PROTOCOL_START.to_slice
        start = AMQP::Frame.decode(@socket).as(AMQP::Connection::Start)

        response = "\u0000#{@uri.user}\u0000#{@uri.password}"
        start_ok = AMQP::Connection::StartOk.new(props, "PLAIN", response, "")
        @socket.write start_ok.to_slice
        tune = AMQP::Frame.decode(@socket).as(AMQP::Connection::Tune)
        @socket.write AMQP::Connection::TuneOk.new(channel_max: 1_u16,
                                                   frame_max: 131072_u32,
                                                   heartbeat: 0_u16).to_slice
        @socket.write AMQP::Connection::Open.new(@uri.path[1..-1]).to_slice
        open_ok = AMQP::Frame.decode(@socket).as(AMQP::Connection::OpenOk)
      end

      def open_channel
        @socket.write AMQP::Channel::Open.new(1_u16).to_slice
        open_ok = AMQP::Frame.decode(@socket).as(AMQP::Channel::OpenOk)
      end
    end
  end
end
