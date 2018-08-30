require "logger"
require "./amqp"
require "./connection"

module AvalancheMQ
  class Shovel
    @pub : Publisher?
    @sub : Consumer?
    @log : Logger
    @state = 0_u8

    getter name

    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFUALT_RECONNECT_DELAY = 5
    DEFUALT_DELETE_AFTER    = DeleteAfter::Never
    DEFAULT_PREFETCH        = 1000_u16

    def initialize(@source : Source, @destination : Destination, @name : String, @vhost : VHost,
                   @ack_mode = DEFAULT_ACK_MODE, @reconnect_delay = DEFUALT_RECONNECT_DELAY)
      @log = @vhost.log.dup
      @log.progname += " shovel=#{@name}"
      @channel_a = Channel::Buffered(AMQP::Frame).new(@source.prefetch.to_i32)
      @channel_b = Channel::Buffered(AMQP::Frame).new(@source.prefetch.to_i32)
    end

    def self.merge_defaults(config : JSON::Any)
      c = config.as_h
      c["src-delete-after"] ||= JSON::Any.new("never")
      c["ack-mode"] ||= JSON::Any.new("on-confirm")
      c["reconnect-delay"] ||= JSON::Any.new(DEFUALT_RECONNECT_DELAY.to_i64)
      c["src-prefetch-count"] ||= JSON::Any.new(DEFAULT_PREFETCH.to_i64)
      JSON::Any.new(c)
    end

    def run
      @state = 0
      spawn(name: "Shovel consumer #{@source.uri.host}") do
        loop do
          break if @channel_a.closed?
          sub = Consumer.new(@source, @channel_b, @channel_a, @ack_mode, @log)
          sub.on_done { delete }
          @state += 1
          sub.start
        rescue ex
          @state -= 1
          break if @channel_a.closed?
          @log.warn "Shovel consumer failure: #{ex.inspect_with_backtrace}"
          sub.try &.force_close("Shovel stopped")
          sleep @reconnect_delay.seconds
        end
        @log.debug { "Consumer stopped" }
      end
      spawn(name: "Shovel publisher #{@destination.uri.host}") do
        loop do
          break if @channel_b.closed?
          pub = Publisher.new(@destination, @channel_a, @channel_b, @ack_mode, @log)
          @state += 1
          pub.start
        rescue ex
          @state -= 1
          break if @channel_b.closed?
          @log.warn "Shovel publisher failure: #{ex.message}"
          pub.try &.force_close("Shovel stopped")
          sleep @reconnect_delay.seconds
        end
        @log.debug { "Publisher stopped" }
      end
      @log.info { "Shovel '#{@name}' starting" }
      Fiber.yield
    end

    # Does not trigger reconnect, but a graceful close
    def stop
      @state = -1
      @channel_a.close
      @channel_b.close
    end

    def delete
      stop
      @vhost.delete_parameter("shovel", @name)
    end

    def stopped?
      @channel_a.closed? || @channel_b.closed?
    end

    STARTING   = "starting"
    RUNNING    = "running"
    TERMINATED = "terminated"

    def state
      case @state
      when 0, 1
        STARTING
      when 2
        RUNNING
      else
        TERMINATED
      end
    end

    enum DeleteAfter
      Never
      QueueLength
    end

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    struct Source
      getter uri, queue, exchange, exchange_key, delete_after, prefetch

      def initialize(raw_uri : String, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFUALT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH)
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

    class Publisher < Connection
      def initialize(@destination : Destination, @in : Channel::Buffered(AMQP::Frame),
                     @out : Channel::Buffered(AMQP::Frame), @ack_mode : AckMode, log : Logger)
        @log = log.dup
        @log.progname += " publisher"
        @message_count = 0_u64
        @delivery_tags = Hash(UInt64, UInt64).new
        super(@destination.uri, @log)
        set_confirm if @ack_mode == AckMode::OnConfirm
      end

      def start
        channel_read_loop
        amqp_read_loop
      end

      private def send_basic_publish(frame)
        exchange = @destination.exchange.not_nil!
        routing_key = @destination.exchange_key || frame.routing_key
        mandatory = @ack_mode == AckMode::OnConfirm
        @socket.write AMQP::Basic::Publish.new(1_u16, 0_u16, exchange, routing_key,
          mandatory, false).to_slice
        if mandatory
          @message_count += 1
          @delivery_tags[@message_count] = frame.delivery_tag
        else
          ack(frame.delivery_tag)
        end
      end

      private def amqp_read_loop
        loop do
          Fiber.yield if @out.full?
          frame = AMQP::Frame.decode(@socket)
          @log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::Basic::Nack
            next unless @ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| reject(t) }
            else
              reject(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Basic::Return
            reject(@message_count) if @ack_mode == AckMode::OnConfirm
          when AMQP::Basic::Ack
            next unless @ack_mode == AckMode::OnConfirm
            if frame.multiple
              with_multiple(frame.delivery_tag) { |t| ack(t) }
            else
              ack(@delivery_tags[frame.delivery_tag])
            end
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            break
          else
            @log.warn { "Unexpected frame #{frame}" }
          end
        rescue Channel::ClosedError
          @log.debug { "#amqp_read_loop out channel closed" }
        end
      ensure
        @socket.close
      end

      private def channel_read_loop
        spawn(name: "Shovel publisher #{@destination.uri.host}#channel_read_loop") do
          loop do
            Fiber.yield if @in.empty?
            frame = @in.receive
            case frame
            when AMQP::Basic::Deliver
              send_basic_publish(frame)
            when AMQP::HeaderFrame
              @socket.write frame.to_slice
            when AMQP::BodyFrame
              @socket.write frame.to_slice
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue Channel::ClosedError
            @log.debug { "#channel_read_loop closed" }
            force_close("Shovel stopped")
            break
          end
        end
      end

      private def set_confirm
        @socket.write AMQP::Confirm::Select.new(1_u16, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Confirm::SelectOk)
      end

      private def with_multiple(delivery_tag)
        @delivery_tags.delete_if do |m, t|
          next false unless m <= delivery_tag
          yield t
          true
        end
      end

      private def ack(delivery_tag)
        @out.send AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
      end

      private def reject(delivery_tag)
        @out.send AMQP::Basic::Reject.new(1_u16, delivery_tag, false)
      end
    end

    class Consumer < Connection
      def initialize(@source : Source, @in : Channel::Buffered(AMQP::Frame),
                     @out : Channel::Buffered(AMQP::Frame), @ack_mode : AckMode, log : Logger)
        @log = log.dup
        @log.progname += " consumer"
        @message_counter = 0_u32
        @message_count = 0_u32
        super(@source.uri, @log)
        set_prefetch
      end

      def on_done(&blk)
        @on_done = blk
      end

      def start
        channel_read_loop
        consume_loop
      end

      private def consume_loop
        consume
        loop do
          Fiber.yield if @out.full?
          frame = AMQP::Frame.decode(@socket)
          @log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::HeaderFrame
            @out.send(frame)
          when AMQP::Basic::Deliver
            @out.send(frame)
          when AMQP::BodyFrame
            @out.send(frame)
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            raise UnexpectedFrame.new(frame) unless @out.closed?
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            break
          else
            raise UnexpectedFrame.new(frame)
          end
        rescue Channel::ClosedError
          @log.debug { "#consume_loop out channel closed" }
        end
      ensure
        @socket.close
      end

      private def channel_read_loop
        spawn(name: "Shovel publisher #{@source.uri.host}#channel_read_loop") do
          loop do
            Fiber.yield if @in.empty?
            frame = @in.receive
            case frame
            when AMQP::Basic::Ack
              @socket.write frame.to_slice
              @message_counter += 1
              if @source.delete_after == DeleteAfter::QueueLength &&
                 @message_count <= @message_counter
                @socket.write AMQP::Connection::Close.new(200_u16,
                  "Shovel done", 0_u16, 0_u16).to_slice
                @on_done.try &.call
              end
            when AMQP::Basic::Nack
              @socket.write frame.to_slice
            when AMQP::Basic::Return
              @socket.write frame.to_slice
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue ex : Channel::ClosedError
            @log.debug { "#channel_read_loop closed" }
            force_close("Shovel stopped")
            break
          end
        end
      end

      private def set_prefetch
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, @source.prefetch, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
      end

      private def consume
        queue_name = @source.queue || ""
        passive = !queue_name.empty?
        @socket.write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, passive,
          false, true, true, false,
          {} of String => AMQP::Field).to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Queue::DeclareOk)
        if @source.exchange
          @socket.write AMQP::Queue::Bind.new(1_u16, 0_u16, frame.queue_name,
            @source.exchange.not_nil!,
            @source.exchange_key || "",
            true,
            {} of String => AMQP::Field).to_slice
        end
        queue = frame.queue_name
        @message_count = frame.message_count
        no_ack = @ack_mode == AckMode::NoAck
        consume = AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "",
          false, no_ack, false, false,
          {} of String => AMQP::Field)
        @socket.write consume.to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Basic::ConsumeOk)
      end
    end
  end
end
