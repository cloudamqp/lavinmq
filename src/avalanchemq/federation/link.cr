require "../observable"
require "amqp-client"
require "../sortable_json"

module AvalancheMQ
  module Federation
    class Upstream
      abstract class Link
        include Observer
        include SortableJSON
        getter connected_at

        @publisher : Publisher?
        @consumer : Consumer?
        @state = 0_u8
        @consumer_available = Channel(Nil).new
        @done = Channel(Nil).new
        @connected_at : Int64?

        def initialize(@upstream : Upstream, @log : Logger)
        end

        def state
          @state.to_s
        end

        def running?
          @state == State::Running
        end

        def details_tuple
          {
            upstream:  @upstream.name,
            vhost:     @upstream.vhost.name,
            timestamp: @connected_at ? Time.unix_ms(@connected_at.not_nil!) : nil,
            type:      self.is_a?(QueueLink) ? "queue" : "exchange",
            uri:       @upstream.uri.to_s,
            resource:  name,
          }
        end

        def run
          @log.info { "Starting" }
          spawn(run_loop, name: "Federation link #{@upstream.vhost.name}/#{name}")
          Fiber.yield
        end

        def stopped?
          @state == State::Terminated
        end

        # Does not trigger reconnect, but a graceful close
        def stop
          return if stopped?
          @log.info { "Stopping" }
          @state = State::Terminated
          @consumer.try &.close("Federation link stopped")
          @publisher.try &.close("Federation link stopped")
          done!
        end

        private def done!
          select
          when @done.send(nil)
          else
          end
        end

        abstract def name : String
        abstract def on(event : Symbol, data : Object)
        private abstract def run_loop
        private abstract def unregister_observer

        enum State
          Starting
          Running
          Terminated
        end
      end

      class QueueLink < Link
        def initialize(@upstream : Upstream, @federated_q : Queue, @upstream_q : String, @log : Logger)
          @log.progname += " link=#{@federated_q.name}"
          @federated_q.register_observer(self)
          @consumer_available.send(nil) if @federated_q.immediate_delivery?
          super(@upstream, @log)
        end

        def name : String
          @federated_q.name
        end

        def on(event, data)
          @log.debug { "event=#{event} data=#{data}" }
          return if stopped?
          case event
          when :delete, :close
            @upstream.stop_link(@federated_q)
          when :add_consumer
            @consumer_available.send(nil)
          when :rm_consumer
            nil
          else raise "Unexpected event '#{event}'"
          end
        end

        private def unregister_observer
          @federated_q.unregister_observer(self)
        end

        private def setup_queue(c)
          cch = c.channel
          q = begin
            cch.queue_declare(@upstream_q, passive: true)
          rescue ::AMQP::Client::Channel::ClosedException
            cch = c.channel
            cch.queue_declare(@upstream_q, passive: false)
          end
          return {cch, q}
        end

        private def run_loop
          loop do
            break if stopped?
            @state = State::Starting
            if !@federated_q.immediate_delivery?
              @log.debug { "Waiting for consumers" }
              @consumer_available.receive?
              break if stopped?
            end
            ::AMQP::Client.start(@upstream.uri) do |c|
              ::AMQP::Client.start("/tmp/#{name}.sock") do |p|
                cch, q = setup_queue(c)
                cch.prefetch = @upstream.prefetch
                no_ack = @upstream.ack_mode == AckMode::NoAck
                cch.basic_consume(q.name, no_ack: no_ack, tag: "Federation") do |msg|
                  p.send_basic_publish(msg, pch, q[:message_count])
                end
              end
            end
            p = @publisher.not_nil!
            c = @consumer.not_nil!
            c.on_frame { |f| p.forward f }
            p.on_frame { |f| c.forward f }
            p.run
            c.run
            @state = State::Running
            @connected_at = Time.utc.to_unix_ms
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
          @connected_at = nil
        end
      end

      class ExchangeLink < Link
        def initialize(@upstream : Upstream, @federated_ex : Exchange, @upstream_q : String,
                       @upstream_exchange : String, @log : Logger)
          @log.progname += " link=#{@federated_ex.name}"
          @federated_ex.register_observer(self)
          super(@upstream, @log)
        end

        def name : String
          @federated_ex.name
        end

        def on(event, data)
          @log.debug { "event=#{event} data=#{data}" }
          return if stopped?
          case event
          when :delete
            @upstream.stop_link(@federated_ex)
          when :bind
            if c = @consumer
              if b = data.as?(BindingDetails)
                c.bind(@upstream_exchange, @upstream_q, b.routing_key, b.arguments)
              end
            end
          when :unbind
            if c = @consumer
              if b = data.as?(BindingDetails)
                c.unbind(@upstream_exchange, @upstream_q, b.routing_key, b.arguments)
              end
            end
          else raise "Unexpected event '#{event}'"
          end
        end

        def stop
          return if stopped?
          consumer.cleanup_exchange_federation
        end

        private def unregister_observer
          @federated_ex.unregister_observer(self)
        end

        private def run_loop
          loop do
            break if stopped?
            @state = State::Starting
            @publisher = Publisher.new(@upstream, nil, @federated_ex.name)
            @consumer = Consumer.new(@upstream, @upstream_q)
            p = @publisher.not_nil!
            c = @consumer.not_nil!
            c.init_exchange_federation(@upstream_exchange, @federated_ex)
            c.on_frame { |f| p.forward f }
            p.on_frame { |f| c.forward f }
            p.run
            c.run
            @state = State::Running
            @connected_at = Time.utc.to_unix_ms
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
          @connected_at = nil
        end
      end
    end
  end
end
