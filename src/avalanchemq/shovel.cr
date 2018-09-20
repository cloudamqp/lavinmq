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
      @channel_a = Channel(AMQP::Frame?).new
      @channel_b = Channel(AMQP::Frame?).new
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
          break if stopped?
          sub = Consumer.new(@source, @channel_b, @channel_a, @ack_mode, @log)
          sub.on_done { delete }
          @state += 1
          sub.start
        rescue ex
          unless stopped?
            @state -= 1
            @log.warn "Shovel consumer failure: #{ex.inspect_with_backtrace}"
          end
          sub.try &.close("Shovel stopped")
          break if stopped?
          @channel_b.send(nil)
          sleep @reconnect_delay.seconds
        end
        @log.info { "Consumer stopped" }
      end
      spawn(name: "Shovel publisher #{@destination.uri.host}") do
        loop do
          break if stopped?
          pub = Publisher.new(@destination, @channel_a, @channel_b, @ack_mode, @log)
          @state += 1
          pub.start
        rescue ex
          unless stopped?
            @state -= 1
            @log.warn "Shovel publisher failure: #{ex.message}"
          end
          pub.try &.close("Shovel stopped")
          break if stopped?
          @channel_a.send(nil)
          sleep @reconnect_delay.seconds
        end
        @log.info { "Publisher stopped" }
      end
      @log.info { "Starting" }
      Fiber.yield
    end

    # Does not trigger reconnect, but a graceful close
    def stop
      @log.info { "Stopping" }
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
  end
end
