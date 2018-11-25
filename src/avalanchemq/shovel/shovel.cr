require "logger"
require "./consumer"
require "./publisher"

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
    end

    def state
      @state.to_s
    end

    def run
      @log.info { "Starting" }
      @state = State::Starting
      spawn(run_loop, name: "Shovel #{@vhost.name}/#{@name}")
      Fiber.yield
    end

    def to_json(json)
      {
        name:  @name,
        vhost: @vhost.name,
        state: @state,
      }.to_json(json)
    end

    private def run_loop
      loop do
        break if stopped?
        done = Channel(Bool).new
        @state = State::Starting
        @publisher = Publisher.new(@destination, @ack_mode, @log, done)
        @consumer = Consumer.new(@source, @ack_mode, @log, done)
        p = @publisher.not_nil!
        c = @consumer.not_nil!
        c.on_frame { |f| p.forward f }
        p.on_frame { |f| c.forward f }
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
        case ex
        when AMQP::Error::FrameDecode
          @log.warn { "Shovel failure: #{ex.cause.inspect}" }
        when Connection::UnexpectedFrame
          @log.warn { "Shovel failure: #{ex.inspect}" }
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
