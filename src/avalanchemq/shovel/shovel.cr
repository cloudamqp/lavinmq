require "logger"
require "../sortable_json"
require "amqp-client"

module AvalancheMQ
  class Shovel
    include SortableJSON
    @log : Logger
    @state = State::Terminated
    @stop = ::Channel(Exception).new

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

    def details_tuple
      {
        name:  @name,
        vhost: @vhost.name,
        state: @state,
      }
    end

    private def run_loop
      loop do
        @state = State::Starting
        ::AMQP::Client.start(@source.uri) do |c|
          ::AMQP::Client.start(@destination.uri) do |p|
            c.channel do |cch|
              cch.prefetch @source.prefetch
              queue_name, message_count = setup_queue(cch)
              if @source.delete_after == DeleteAfter::QueueLength && message_count == 0
                @state = State::Terminated
                return
              end
              p.channel do |pch|
                pch.confirm_select if @ack_mode == AckMode::OnConfirm
                no_ack = @ack_mode == AckMode::NoAck
                @state = State::Running
                cch.basic_consume(queue_name, no_ack: no_ack, tag: "Shovel") do |msg|
                  shovel(msg, pch, message_count)
                end
                ex = @stop.receive?
                @state = State::Terminated
                raise ex if ex
                return
              end
            end
          end
        end
      rescue ex
        @state = State::Terminated
        @log.error { "Shovel failure: #{ex.inspect_with_backtrace}" }
        sleep @reconnect_delay.seconds
      end
    end

    private def setup_queue(cch)
      name = @source.queue || ""
      q = cch.queue_declare(name)
      if @source.exchange || @source.exchange_key
        cch.queue_bind(q[:queue_name], @source.exchange || "", @source.exchange_key || "")
      end
      { q[:queue_name], q[:message_count] }
    end

    private def shovel(msg, pch, queue_length)
      ex = @destination.exchange || msg.exchange
      rk = @destination.exchange_key || msg.routing_key
      msgid = pch.basic_publish(msg.body_io, ex, rk)
      delete_after_this =
        @source.delete_after == DeleteAfter::QueueLength &&
        msg.delivery_tag == queue_length
      should_multi_ack = msgid % (@source.prefetch / 2).ceil.to_i == 0
      if should_multi_ack || delete_after_this
        case @ack_mode
        when AckMode::OnConfirm
          pch.wait_for_confirm(msgid)
          msg.ack(multiple: true)
        when AckMode::OnPublish
          msg.ack(multiple: true)
        end
      end
      if delete_after_this
        @stop.close
      end
    rescue ex
      @stop.send ex unless @stop.closed?
    end

    # Does not trigger reconnect, but a graceful close
    def stop
      return if stopped?
      @log.info { "Stopping" }
      @stop.close
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
