require "uri"
require "socket"
require "./amqp"

module AvalancheMQ
  class Shovel
    def initialize(@source : Source, @destination : Destination)
      @stopped = false
    end

    def run
      destination = Publisher.new(@destination)
      source = Consumer.new(@source)
      source.subscribe do |routing_key, properties, body|
        break if @stopped
        destination.publish(@destination.exchange.not_nil!,
                            @destination.exchange_key || routing_key,
                            properties, body)
      end
      destination.stop
      source.stop
    end

    def stop
      @stopped = true
    end

    enum DeleteAfter
      Never
      QueueLength
    end

    struct Source
      getter uri, queue, exchange, exchange_key, delete_after

      def initialize(raw_uri : String, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DeleteAfter::Never)
        @uri = URI.parse(raw_uri)
      end
    end

    struct Destination
      getter uri, exchange, exchange_key

      def initialize(raw_uri : String, queue : String?,
                     @exchange : String? = nil, @exchange_key : String? = nil)
        @uri = URI.parse(raw_uri)
        if queue
          @exchange = ""
          @exchange_key = queue
        end
      end
    end

    class Connection
      def initialize(@uri : URI)
        @socket = TCPSocket.new(@uri.host.not_nil!, @uri.port || 5672)
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

      def stop
        @socket.close
      end

      def negotiate_connection
        @socket.write AMQP::PROTOCOL_START.to_slice
        start = AMQP::Frame.decode(@socket).as(AMQP::Connection::Start)

        props = {} of String => AMQP::Field
        response = "\u0000#{@uri.user}\u0000#{@uri.password}"
        start_ok = AMQP::Connection::StartOk.new(props, "PLAIN", response, "")
        @socket.write start_ok.to_slice
        tune = AMQP::Frame.decode(@socket).as(AMQP::Connection::Tune)
        @socket.write AMQP::Connection::TuneOk.new(channel_max: 1_u16,
                                                   frame_max: 131072_u32,
                                                   heartbeat: 0_u16).to_slice
        path = @uri.path || ""
        vhost = path.size > 1 ? path[1..-1] : "/"
        @socket.write AMQP::Connection::Open.new(vhost).to_slice
        open_ok = AMQP::Frame.decode(@socket).as(AMQP::Connection::OpenOk)
      end

      def open_channel
        @socket.write AMQP::Channel::Open.new(1_u16).to_slice
        open_ok = AMQP::Frame.decode(@socket).as(AMQP::Channel::OpenOk)
      end
    end

    class Publisher < Connection
      def initialize(destination : Destination)
        super(destination.uri)
      end

      def publish(exchange, routing_key, properties : AMQP::Properties, payload)
        @socket.write AMQP::Basic::Publish.new(1_u16, 0_u16, exchange, routing_key,
                                               false, false).to_slice
        @socket.write AMQP::HeaderFrame.new(1_u16, 0_u16, 0_u16,
                                            payload.bytesize.to_u64, properties).to_slice
        @socket.write AMQP::BodyFrame.new(1_u16, payload.to_slice).to_slice
      end
    end

    class Consumer < Connection
      def initialize(@source : Source)
        super(@source.uri)
      end

      def declare_queue
        queue_name = @source.queue || ""
        @socket.write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, true,
                                               false, true, true, false,
                                               {} of String => AMQP::Field).to_slice
        declare_ok = AMQP::Frame.decode(@socket).as(AMQP::Queue::DeclareOk)
        if @source.exchange
          @socket.write AMQP::Queue::Bind.new(1_u16, 0_u16, declare_ok.queue_name,
                                              @source.exchange.not_nil!,
                                              @source.exchange_key || "",
                                              true,
                                              {} of String => AMQP::Field).to_slice
        end
        { declare_ok.queue_name, declare_ok.message_count }
      end

      def subscribe(&block)
        queue, message_count = declare_queue
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, 1000_u16, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
        @socket.write AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
                                               false, false, false, true,
                                               {} of String => AMQP::Field).to_slice
        message_counter = 0_u32
        routing_key = ""
        delivery_tag = 0_u64
        properties = AMQP::Properties.new
        body_size = 0_u64
        body = IO::Memory.new(body_size)
        loop do
          frame = AMQP::Frame.decode(@socket)
          case frame
          when AMQP::Basic::Deliver
            routing_key = frame.routing_key
            delivery_tag = frame.delivery_tag
          when AMQP::HeaderFrame
            body_size = frame.body_size
            properties = frame.properties
            body = IO::Memory.new(body_size)
          when AMQP::BodyFrame
            body.write frame.body
            if body.pos == body_size
              yield routing_key, properties, body
              body.clear
              ack = AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
              @socket.write ack.to_slice

              message_counter += 1
              if @source.delete_after == DeleteAfter::QueueLength &&
                  message_count <= message_counter
                @socket.write AMQP::Connection::Close.new(320_u16,
                                                          "shovel done",
                                                          0_u16, 0_u16).to_slice
              end
            end
          when AMQP::Channel::Close
            puts "Server unexpectedly sent #{frame}"
            @socket.write AMQP::Channel::CloseOk.new(frame.channel).to_slice
            @socket.write AMQP::Connection::Close.new(320_u16,
                                                      "channel closed unexpectedly",
                                                      0_u16, 0_u16).to_slice
          when AMQP::Connection::Close
            puts "Server unexpectedly sent #{frame}"
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            @socket.close
            break
          when AMQP::Connection::CloseOk
            @socket.close
            break
          else
            raise "Unexpected frame #{frame}"
          end
        end
      end
    end
  end
end
