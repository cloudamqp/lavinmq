require "logger"
require "./amqp"
require "./shovel/*"

module AvalancheMQ
  class Shovel
    @pub : Publisher?
    @sub : Consumer?
    @log : Logger
    @state = 0_u8

    getter name, vhost

    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFUALT_RECONNECT_DELAY = 5
    DEFUALT_DELETE_AFTER    = DeleteAfter::Never
    DEFAULT_PREFETCH        = 1000_u16

    def initialize(@source : Source, @destination : Destination, @name : String, @vhost : VHost,
                   @ack_mode = DEFAULT_ACK_MODE, @reconnect_delay = DEFUALT_RECONNECT_DELAY)
      @log = @vhost.log.dup
      @log.progname += " shovel=#{@name}"
      @state = State::Starting
    end

    def self.merge_defaults(config : JSON::Any)
      c = config.as_h
      c["src-delete-after"] ||= JSON::Any.new("never")
      c["ack-mode"] ||= JSON::Any.new("on-confirm")
      c["reconnect-delay"] ||= JSON::Any.new(DEFUALT_RECONNECT_DELAY.to_i64)
      c["src-prefetch-count"] ||= JSON::Any.new(DEFAULT_PREFETCH.to_i64)
      JSON::Any.new(c)
    end

    def state
      @state.to_s
    end

    def run
      @log.info { "Starting" }
      spawn(run_loop, name: "Shovel #{@vhost.name}/#{@name}")
      Fiber.yield
    end

    def run_loop
      loop do
        break if stopped?
        done = Channel(Bool).new
        @state = State::Starting
        @publisher = Publisher.new(@destination, @ack_mode, @log, done)
        @consumer = Consumer.new(@source, @ack_mode, @log, done)
        p = @publisher.not_nil!
        c = @consumer.not_nil!
        c.on_frame do |f|
          p.forward f
        end
        p.on_frame do |f|
          c.forward f
        end
        p.run
        c.run
        @state = State::Running
        continue = done.receive
        done.close
        if continue
          c.close("Shovel failure")
          p.close("Shovel failure")
        else
          delete
          break
        end
      rescue ex
        if ex.is_a? AMQP::FrameDecodeError
          @log.warn { "Shovel failure: #{ex.cause.inspect}" }
        else
          @log.warn { "Shovel failure: #{ex.inspect_with_backtrace}" }
        end
        @consumer.try &.close("Shovel stopped")
        @publisher.try &.close("Shovel stopped")
        done.try &.close
        break if stopped?
        sleep @reconnect_delay.seconds
      end
      @log.info { "Shovel stopped" }
    end

    # Does not trigger reconnect, but a graceful close
    def stop
      @log.info { "Stopping" }
      @state = State::Terminated
      @consumer.try &.close("Shovel stopped")
      @publisher.try &.close("Shovel stopped")
    end

    def delete
      stop
      @vhost.delete_parameter("shovel", @name)
    end

    def stopped?
      @state == State::Terminated
    end

    enum State
      Starting
      Running
      Terminated
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
  end
end
