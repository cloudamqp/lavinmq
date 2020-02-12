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
              queue_name = @source.queue || ""
              q = cch.queue(queue_name)
              if @source.exchange || @source.exchange_key
                q.bind(@source.exchange || "", @source.exchange_key || "")
              end
              msg_count = cch.queue_declare(q.name, passive: true)[:message_count]
              if @source.delete_after == DeleteAfter::QueueLength && msg_count == 0
                @state = State::Terminated
                break
              end
              p.channel do |pch|
                pch.confirm_select if @ack_mode == AckMode::OnConfirm
                @state = State::Running
                no_ack = @ack_mode == AckMode::NoAck
                q.subscribe(no_ack: no_ack, tag: "shovel") do |msg|
                  ex = @destination.exchange || msg.exchange
                  rk = @destination.exchange_key || msg.routing_key
                  msgid = pch.basic_publish(msg.body_io, ex, rk)
                  pch.wait_for_confirm(msgid) if @ack_mode == AckMode::OnConfirm
                  msg.ack unless no_ack
                  if @source.delete_after == DeleteAfter::QueueLength
                    if msg.delivery_tag == msg_count
                      @stop.close
                    end
                  end
                rescue ex
                  if @stop.closed?
                    @log.error { "Shovel failure: #{ex.inspect_with_backtrace}" }
                  else
                    @stop.send ex
                  end
                end
                ex = @stop.receive?
                raise ex if ex
                break
              end
            end
          end
        end
      rescue ex
        @state = State::Terminated
        @log.warn { "Shovel failure: #{ex.inspect_with_backtrace}" }
        sleep @reconnect_delay.seconds
      end
      @state = State::Terminated
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
