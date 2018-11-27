require "../observable"
require "./publisher"
require "./consumer"
require "./queue_upstream"
require "./exchange_upstream"

module AvalancheMQ
  module Federation
    class Upstream
      class Link
        include Observer
        getter connected_at

        @publisher : Publisher?
        @consumer : Consumer?
        @state = 0_u8
        @consumer_available = Channel(Nil).new
        @done = Channel(Nil).new

        def initialize(@upstream : QueueUpstream, @federated_q : Queue, @log : Logger)
          @log.progname += " link=#{@federated_q.name}"
          @federated_q.register_observer(self)
          @consumer_available.send(nil) if @federated_q.immediate_delivery?
        end

        def state
          @state.to_s
        end

        def name
          @federated_q.name
        end

        def on(event, data)
          @log.debug { "event=#{event} data=#{data}" }
          case event
          when :delete, :close
            @upstream.stop_link(@federated_q)
          when :add_consumer
            @consumer_available.send(nil) unless @consumer_available.closed?
          when :rm_consumer
            @upstream.stop_link(@federated_q) unless @federated_q.consumer_count > 0
          end
        end

        def to_json(json)
          {
            upstream:  @upstream.name,
            vhost:     @upstream.vhost.name,
            timestamp: @connected_at.to_s,
            type:      @upstream.is_a?(QueueUpstream) ? "queue" : "exchange",
            uri:       @upstream.uri.to_s,
            resource:  @federated_q.name,
          }
        end

        def run
          @log.info { "Starting" }
          spawn(run_loop, name: "Federation link #{@upstream.vhost.name}/#{@federated_q.name}")
          Fiber.yield
        end

        private def run_loop
          loop do
            break if stopped?
            @state = State::Starting
            if !@federated_q.immediate_delivery?
              @log.debug { "Waiting for consumers" }
              @consumer_available.receive
            end
            @publisher = Publisher.new(@upstream, @federated_q)
            @consumer = Consumer.new(@upstream)
            p = @publisher.not_nil!
            c = @consumer.not_nil!
            c.on_frame { |f| p.forward f }
            p.on_frame { |f| c.forward f }
            p.run
            c.run
            @state = State::Running
            @connected_at = Time.utc_now
            @done.receive
            break
          rescue ex
            @connected_at = nil
            case ex
            when AMQP::Error::FrameDecode, Connection::UnexpectedFrame
              @log.warn { "Federation link failure: #{ex.cause.inspect}" }
            else
              @log.warn { "Federation link: #{ex.inspect_with_backtrace}" }
            end
            @consumer.try &.close("Federation link stopped")
            @publisher.try &.close("SFederation link stopped")
            break if stopped?
            sleep @upstream.reconnect_delay.seconds
          end
          @log.info { "Federation link stopped" }
        ensure
          @done.close
          @consumer_available.close
          @connected_at = nil
        end

        # Does not trigger reconnect, but a graceful close
        def stop
          @log.info { "Stopping" }
          @state = State::Terminated
          @federated_q.unregister_observer(self)
          @consumer.try &.close("Federation link stopped")
          @publisher.try &.close("Federation link stopped")
          @consumer_available.close
          @done.send(nil) unless @done.closed?
        end

        def stopped?
          @state == State::Terminated
        end

        enum State
          Starting
          Running
          Terminated
        end
      end
    end
  end
end
