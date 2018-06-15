require "uri"
require "socket"
require "openssl"
require "logger"
require "./amqp"

module AvalancheMQ
  class Shovel
    @pub : Publisher?
    @sub : Consumer?
    @stopped = false
    @log : Logger

    def initialize(@source : Source, @destination : Destination, @name : String, @vhost : VHost)
      @log = @vhost.log.dup
      @log.progname += " shovel=#{@name}"
    end

    def run
      loop do
        break if @stopped
        pub = Publisher.new(@destination, @log)
        sub = Consumer.new(@source, @log)
        sub.on_basic_deliver do |d|
          pub.send_basic_publish(d)
        end
        sub.on_header_frame do |h|
          pub.send_header_frame(h)
        end
        sub.on_body_frame do |b|
          pub.send_body_frame(b)
        end
        sub.on_done { @vhost.delete_parameter("shovel", @name) }
        @pub = pub
        @sub = sub
        spawn(name: "Shovel publish #{@destination.uri.host} read_loop") { pub.read_loop }
        sub.consume_loop
        break
      rescue ex
        break if @stopped
        @log.warn "Shovel failure: #{ex.inspect}"
        sleep 3
      end
    end

    def stop
      @stopped = true
      @pub.try &.close
      @sub.try &.close
    end

    enum DeleteAfter
      Never
      QueueLength
    end

    struct Source
      getter uri, queue, exchange, exchange_key, delete_after, prefetch

      def initialize(raw_uri : String, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DeleteAfter::Never, @prefetch = 1000_u16)
        @uri = URI.parse(raw_uri)
        if @queue.nil? && @exchange.nil?
          raise ArgumentError.new("Shovel source requires a queue or an exchange")
        end
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
        if @exchange.nil?
          raise ArgumentError.new("Shovel destination requires an exchange")
        end
      end
    end

    class Connection
      @socket : TCPSocket | OpenSSL::SSL::Socket::Client
      @closed = false

      def initialize(@uri : URI, @log : Logger)
        host = @uri.host || "localhost"
        tls = @uri.scheme == "amqps"
        socket = TCPSocket.new(host, @uri.port || tls ? 5671 : 5672)
        socket.sync = true
        socket.keepalive = true
        socket.tcp_nodelay = true
        socket.tcp_keepalive_idle = 60
        socket.tcp_keepalive_count = 3
        socket.tcp_keepalive_interval = 10
        socket.write_timeout = 15
        socket.recv_buffer_size = 131072
        @socket =
          if tls
            OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: host)
          else
            socket
          end
        negotiate_connection
        open_channel
      end

      def negotiate_connection
        @socket.write AMQP::PROTOCOL_START.to_slice
        start = AMQP::Frame.decode(@socket).as(AMQP::Connection::Start)

        props = {} of String => AMQP::Field
        user = URI.unescape(@uri.user || "guest")
        password = URI.unescape(@uri.password || "guest")
        response = "\u0000#{user}\u0000#{password}"
        start_ok = AMQP::Connection::StartOk.new(props, "PLAIN", response, "")
        @socket.write start_ok.to_slice
        tune = AMQP::Frame.decode(@socket).as(AMQP::Connection::Tune)
        @socket.write AMQP::Connection::TuneOk.new(channel_max: 1_u16,
          frame_max: 4096_u32,
          heartbeat: 0_u16).to_slice
        path = @uri.path || ""
        vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
        @socket.write AMQP::Connection::Open.new(vhost).to_slice
        frame = AMQP::Frame.decode(@socket)
        case frame
        when AMQP::Connection::Close
          @socket.write AMQP::Connection::CloseOk.new.to_slice
          @socket.close
        else
          frame.as(AMQP::Connection::OpenOk)
        end
      end

      def open_channel
        @socket.write AMQP::Channel::Open.new(1_u16).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Channel::OpenOk)
      end

      def close
        @closed = true
        @socket.write AMQP::Connection::Close.new(200_u16, "shovel stopped", 0_u16, 0_u16).to_slice
      end
    end

    class Publisher < Connection
      def initialize(@destination : Destination, @log : Logger)
        super(@destination.uri, @log)
      end

      def send_basic_publish(frame)
        return if @closed
        exchange = @destination.exchange.not_nil!
        routing_key = @destination.exchange_key || frame.routing_key
        @socket.write AMQP::Basic::Publish.new(1_u16, 0_u16, exchange, routing_key,
          false, false).to_slice
      end

      def send_header_frame(frame)
        @socket.write frame.to_slice
      end

      def send_body_frame(frame)
        @socket.write frame.to_slice
      end

      def read_loop
        loop do
          frame = AMQP::Frame.decode(@socket)
          case frame
          when AMQP::Channel::Close
            @closed = true
            @socket.write AMQP::Channel::CloseOk.new(frame.channel).to_slice
            @socket.write AMQP::Connection::Close.new(320_u16,
              "Shovel can't continue", 0_u16, 0_u16).to_slice
          when AMQP::Connection::Close
            @closed = true
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            @socket.close
            break
          when AMQP::Connection::CloseOk
            @closed = true
            @socket.close
            break
          else raise "Unexpected frame #{frame}"
          end
        end
      end
    end

    class Consumer < Connection
      def initialize(@source : Source, @log : Logger)
        super(@source.uri, @log)
        set_prefetch
      end

      def on_basic_deliver(&blk : AMQP::Basic::Deliver -> Nil)
        @on_basic_deliver = blk
      end

      def on_header_frame(&blk : AMQP::HeaderFrame -> Nil)
        @on_header_frame = blk
      end

      def on_body_frame(&blk : AMQP::BodyFrame -> Nil)
        @on_body_frame = blk
      end

      def on_done(&blk)
        @on_done = blk
      end

      def consume_loop
        queue, message_count = declare_queue
        consume = AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, false, false, true,
          {} of String => AMQP::Field)
        @socket.write consume.to_slice
        message_counter = 0_u32
        body_size = 0_u64
        body_bytes = 0_u64
        delivery_tag = 0_u64
        loop do
          frame = AMQP::Frame.decode(@socket)
          case frame
          when AMQP::Basic::Deliver
            @on_basic_deliver.try &.call(frame)
            delivery_tag = frame.delivery_tag
          when AMQP::HeaderFrame
            @on_header_frame.try &.call(frame)
            body_size = frame.body_size
            ack(delivery_tag) if body_size.zero?
          when AMQP::BodyFrame
            @on_body_frame.try &.call(frame)
            body_bytes += frame.body.bytesize
            if body_bytes == body_size
              ack(delivery_tag)
              body_bytes = 0_u64

              message_counter += 1
              if @source.delete_after == DeleteAfter::QueueLength &&
                 message_count <= message_counter
                @socket.write AMQP::Connection::Close.new(200_u16,
                  "Shovel done", 0_u16, 0_u16).to_slice
                @on_done.try &.call
              end
            end
          when AMQP::Channel::Close
            @socket.write AMQP::Channel::CloseOk.new(frame.channel).to_slice
            @socket.write AMQP::Connection::Close.new(320_u16,
              "Shovel can't continue", 0_u16, 0_u16).to_slice
          when AMQP::Connection::Close
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
      rescue ex : Errno | IO::Error | AMQP::FrameDecodeError
        @log.error { ex.inspect }
      end

      def set_prefetch
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
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
        {declare_ok.queue_name, declare_ok.message_count}
      end

      def ack(delivery_tag)
        ack = AMQP::Basic::Ack.new(1_u16, delivery_tag, false).to_slice
        @socket.write ack.to_slice
      end
    end
  end
end
