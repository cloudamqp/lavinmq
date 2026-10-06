require "../observable"
require "../logger"
require "../sortable_json"
require "../rough_time"
require "../amqp/queue/event"
require "../amqp/exchange/event"
require "../endpoint"

module LavinMQ
  module Federation
    class Upstream
      # Moves messages from the upstream to a federated exchange or queue in
      # this broker. The upstream is an Endpoint: another vhost of this broker,
      # in-process, or another broker over AMQP. Downstream, messages are
      # published in-process, with publish confirms when the ack mode is
      # on-confirm, so the upstream message is acked only once it's durable
      # here.
      abstract class Link
        include SortableJSON
        Log = LavinMQ::Log.for "federation.link"

        enum State
          Starting
          Running
          Stopped
          Terminating
          Terminated
          Error
        end

        getter last_changed : Int64?
        getter error : String?
        getter state = State::Stopped
        @metadata : ::Log::Metadata
        @display_uri : String
        # Closed by #stop, wakes every wait of the link
        @stop_signal = ::Channel(Nil).new
        # Set when a downstream publish was nacked (a full reject-publish
        # queue). The message goes back upstream and is redelivered right
        # away, so the next delivery waits a moment instead of spinning.
        @nacked = Atomic(Bool).new(false)
        NACK_BACKOFF = 100.milliseconds
        @upstream_session : Endpoint::Session
        @downstream : Endpoint::LocalSession

        def initialize(@upstream : Upstream)
          @metadata = ::Log::Metadata.new(nil, {vhost: @upstream.vhost.name, upstream: @upstream.name})
          @log = Logger.new(Log, @metadata)
          @display_uri = Endpoint.display_uri(@upstream.uri)
          session_name = "Federation link: #{@upstream.name}/#{name}"
          @upstream_session = Endpoint.session(@upstream.uri, @upstream.vhost, session_name)
          @downstream = Endpoint::LocalSession.new(@upstream.vhost, @upstream.vhost.name, session_name)
        end

        abstract def name : String
        abstract def type : String

        # Runs until the link is up, then returns. Blocks while the link is
        # running and returns when the upstream goes away.
        private abstract def start_link

        def details_tuple
          {
            upstream:       @upstream.name,
            vhost:          @upstream.vhost.name,
            timestamp:      @last_changed.try { |v| Time.unix_ms(v) },
            type:           type,
            uri:            @display_uri,
            resource:       name,
            error:          @error,
            status:         @state.to_s.downcase,
            "consumer-tag": @upstream.consumer_tag,
          }
        end

        def search_match?(value : String) : Bool
          @upstream.name.includes? value
        end

        def search_match?(value : Regex) : Bool
          value === @upstream.name
        end

        def run
          @log.info { "Starting" }
          spawn(run_loop, name: "Federation link #{@upstream.vhost.name}/#{name}")
          Fiber.yield
        end

        # Graceful close of the link without removing any upstream resources.
        # Use on broker shutdown: the upstream queue and exchange must survive
        # a restart so buffered messages aren't lost.
        def stop
          return if stopping?
          set_state(State::Terminating)
          @stop_signal.close
          close_sessions
        end

        # Permanently remove the link, including any resources it created on
        # the upstream. Use when the federation, the federated resource or the
        # upstream itself is deleted.
        def delete
          stop
        end

        def stopping? : Bool
          @state.in?(State::Terminating, State::Terminated)
        end

        private def set_state(state : State)
          @log.debug { "state change from=#{@state} to=#{state}" }
          @last_changed = RoughTime.unix_ms
          @state = state
        end

        private def run_loop
          loop do
            break if stopping?
            set_state(State::Starting)
            begin
              @downstream.open
              start_link
              @error = nil
            rescue ex
              break if stopping?
              @log.info { "Federation link error=#{ex.message}" }
              @error = ex.message
            ensure
              close_sessions
            end
            break if stopping?
            set_state(State::Stopped)
            break unless wait(@upstream.reconnect_delay)
            @log.info { "Federation try reconnect" }
          end
        ensure
          set_state(State::Terminated)
          @log.info { "Terminated" }
        end

        private def close_sessions
          @upstream_session.close
          @downstream.close
        end

        # Sleeps, returning false if the link is stopped meanwhile
        private def wait(span : Time::Span) : Bool
          select
          when @stop_signal.receive?
            false
          when timeout(span)
            !stopping?
          end
        end

        private def declare_queue(session, name, args = AMQ::Protocol::Table.new)
          session.declare_queue(name, passive: true)
        rescue Endpoint::NotFound
          session.declare_queue(name, passive: false, args: args)
        end

        # Publishes an upstream delivery downstream, adding where it came from
        # to its x-received-from header, and settles it upstream according to
        # the ack mode. Returns false if it was published `immediate` and no
        # consumer was ready for it; it's then returned to the upstream queue
        # (unless consumed with no-ack, where it's lost).
        private def federate(msg : Endpoint::Delivery, exchange : String, routing_key : String,
                             received_from : AMQ::Protocol::Table, immediate : Bool) : Bool
          @last_changed = RoughTime.unix_ms
          wait(NACK_BACKOFF) if @nacked.swap(false)
          props = msg.properties
          headers = props.headers || AMQ::Protocol::Table.new
          hops = headers["x-received-from"]?.try(&.as?(Array(AMQ::Protocol::Field))) || Array(AMQ::Protocol::Field).new
          hops << received_from
          headers["x-received-from"] = hops
          props.headers = headers
          upstream = @upstream_session
          tag = msg.tag
          case @upstream.ack_mode
          in AckMode::NoAck
            result = @downstream.publish(exchange, routing_key, props, msg.body, immediate, nil)
            if immediate && !result.routed?
              @log.warn { "No downstream consumer ready, message lost (ack-mode no-ack)" }
              return false
            end
          in AckMode::OnPublish
            result = @downstream.publish(exchange, routing_key, props, msg.body, immediate, nil)
            if immediate && !result.routed?
              upstream.reject(tag, requeue: true)
              return false
            end
            upstream.ack(tag)
          in AckMode::OnConfirm
            # A nack (no consumer ready, reject-publish overflow, or the
            # downstream closing) returns the message upstream.
            result = @downstream.publish(exchange, routing_key, props, msg.body, immediate,
              ->(confirmed : Bool) { settle(upstream, tag, confirmed) })
            return false if immediate && !result.routed?
          end
          true
        end

        private def settle(upstream : Endpoint::Session, tag : UInt64, confirmed : Bool)
          if confirmed
            upstream.ack(tag)
          else
            @nacked.set(true)
            upstream.reject(tag, requeue: true)
          end
        rescue ex
          @log.debug { "Could not settle upstream delivery: #{ex.message}" }
        end
      end

      # Federates a queue: consumes the upstream queue while the downstream
      # queue has consumers, and publishes to it only when one of them is ready
      # to take the message (an `immediate` publish). That way messages stay
      # upstream, available to other consumers, until a consumer here wants
      # them.
      class QueueLink < Link
        include Observer(QueueEvent)

        # Set by the consumer watcher when it ends a consume round because the
        # downstream queue has no consumers left
        @round_ended = false

        def initialize(@upstream : Upstream, @federated_q : AMQP::Queue, @upstream_q : String)
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_q.name})
          @federated_q.register_observer(self)
        end

        def name : String
          @federated_q.name
        end

        def type : String
          "queue"
        end

        def stop
          @federated_q.unregister_observer(self)
          super
        end

        def on(event : QueueEvent, data)
          return if stopping?
          case event
          in .deleted?, .closed?
            @upstream.stop_link(@federated_q)
          in .consumer_added?, .consumer_removed?
            nil
          end
        rescue e
          @log.error { "Could not process event=#{event} error=#{e.inspect_with_backtrace}" }
        end

        private def start_link
          # Connect once up front so a bad upstream is reported right away,
          # even before the downstream queue has consumers.
          open_upstream
          set_state(State::Running)
          loop do
            unless has_consumers?
              @upstream_session.close
              return unless wait_for_consumers
              open_upstream
            end
            consume_round
          end
        end

        private def open_upstream
          @upstream_session.close
          @upstream_session.open
          declare_queue(@upstream_session, @upstream_q)
          @upstream_session.prefetch = @upstream.prefetch
        end

        private def has_consumers? : Bool
          !@federated_q.consumers_empty?
        rescue ::Channel::ClosedError
          false
        end

        # Waits until the downstream queue has consumers. Returns false if the
        # link was stopped (or the queue closed) meanwhile.
        private def wait_for_consumers : Bool
          @log.debug { "Waiting for downstream consumers" }
          until has_consumers?
            select
            when @federated_q.consumers_empty.when_false.receive?
            when @stop_signal.receive?
            end
            return false if stopping? || @federated_q.closed?
          end
          true
        end

        # Consumes the upstream queue until the downstream queue loses its
        # consumers. The round ends by closing the upstream session, which
        # returns every message we hold, rather than cancelling the consumer,
        # which could leave prefetched deliveries unsettled.
        private def consume_round
          @round_ended = false
          done = ::Channel(Nil).new
          spawn(watch_consumers(done), name: "Federation link #{@upstream.vhost.name}/#{name} consumer watch")
          no_ack = @upstream.ack_mode.no_ack?
          received_from = AMQ::Protocol::Table.new({
            "uri"         => @display_uri,
            "queue"       => @upstream_q,
            "redelivered" => false,
          })
          begin
            @upstream_session.consume(@upstream_q, @upstream.consumer_tag, no_ack, false,
              AMQ::Protocol::Table.new) do |msg|
              received_from["redelivered"] = msg.redelivered
              unless federate(msg, "", @federated_q.name, received_from.clone, immediate: true)
                # No consumer here was ready, so the message went back
                # upstream. Take no more until one has room; if they're all
                # gone instead, the consumer watcher ends the round.
                wait_for_capacity
                @nacked.set(false) # the nack meant no consumer, waited for above
              end
            end
          rescue ex
            raise ex unless @round_ended
          ensure
            done.close
          end
        end

        # Ends the consume round when the downstream queue has no consumers
        # left, or when the round ends by itself
        private def watch_consumers(done)
          select
          when @federated_q.consumers_empty.when_true.receive?
            @log.info { "Lost downstream consumers, closing upstream" }
            @round_ended = true
            @upstream_session.close
          when done.receive?
          end
        end

        # Waits until a downstream consumer can take a message. Returns false if
        # there are no consumers left or the link stopped.
        private def wait_for_capacity : Bool
          until @federated_q.immediate_delivery?
            return false if stopping? || !has_consumers?
            channels = @federated_q.consumers.map(&.has_capacity.when_true.as(::Channel(Nil)))
            channels << @stop_signal
            channels << @federated_q.consumers_empty.when_true
            ::Channel.receive_first(channels)
          end
          true
        rescue ::Channel::ClosedError
          !stopping? && has_consumers?
        end
      end

      # Federates an exchange: an internal queue on the upstream, bound to the
      # upstream exchange the way the downstream exchange is bound, collects
      # what the downstream exchange would route, and the link publishes it to
      # the downstream exchange.
      #
      # Upstream topology, kept between restarts and removed by #delete:
      #   upstream exchange --(downstream's bindings)--> x-federation-upstream
      #   exchange named like the queue --> queue "federation: X -> host:vhost:Y"
      class ExchangeLink < Link
        include Observer(ExchangeEvent)

        def initialize(@upstream : Upstream, @federated_ex : AMQP::Exchange, @upstream_q : String,
                       @upstream_exchange : String)
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_ex.name})
        end

        def name : String
          @federated_ex.name
        end

        def type : String
          "exchange"
        end

        def stop
          super
          @federated_ex.unregister_observer(self)
        end

        def delete
          stop
          cleanup
        end

        def on(event : ExchangeEvent, data)
          return if stopping?
          case event
          in .deleted?
            @upstream.stop_link(@federated_ex)
          in .bind?
            b = binding_details(data)
            forward, args = bound_from(b.arguments)
            @upstream_session.bind_exchange(@upstream_q, @upstream_exchange, b.routing_key, args) if forward
          in .unbind?
            b = binding_details(data)
            forward, args = bound_from(b.arguments)
            @upstream_session.unbind_exchange(@upstream_q, @upstream_exchange, b.routing_key, args) if forward
          end
        rescue ex : Endpoint::Error
          # Not connected: the bindings are replayed when the link reconnects
          @log.debug { "Could not mirror event=#{event} to upstream: #{ex.message}" }
        rescue ex
          @log.error { "Could not process event=#{event} error=#{ex.inspect_with_backtrace}" }
        end

        private def binding_details(data) : AMQP::BindingDetails
          data.as?(AMQP::BindingDetails) || raise ArgumentError.new("Expected data to be of type AMQP::BindingDetails")
        end

        private def start_link
          session = @upstream_session
          session.open
          setup(session)
          # A concurrent delete can stop the link while setup was waiting on
          # the upstream; don't go Running, and don't leave a dead observer.
          if stopping?
            @federated_ex.unregister_observer(self)
            return
          end
          session.prefetch = @upstream.prefetch
          set_state(State::Running)
          no_ack = @upstream.ack_mode.no_ack?
          received_from = AMQ::Protocol::Table.new({
            "uri"         => @display_uri,
            "exchange"    => @upstream_exchange,
            "redelivered" => false,
          })
          session.consume(@upstream_q, @upstream.consumer_tag, no_ack, false, AMQ::Protocol::Table.new) do |msg|
            if should_forward?(msg.properties.headers)
              received_from["redelivered"] = msg.redelivered
              federate(msg, @federated_ex.name, msg.routing_key, received_from.clone, immediate: false)
            else
              @log.debug { "Skipping message, max hops reached" }
              session.ack(msg.tag) unless no_ack
            end
          end
        end

        private def setup(session)
          begin
            session.declare_exchange(@upstream_exchange, @federated_ex.type, passive: true)
          rescue Endpoint::NotFound
            session.declare_exchange(@upstream_exchange, @federated_ex.type, passive: false,
              args: @federated_ex.arguments)
          end
          q_args = AMQ::Protocol::Table.new({"x-internal-purpose" => "federation"})
          @upstream.expires.try { |v| q_args["x-expires"] = v }
          @upstream.msg_ttl.try { |v| q_args["x-message-ttl"] = v }
          declare_queue(session, @upstream_q, q_args)
          ex_args = AMQ::Protocol::Table.new({
            "x-downstream-name"  => System.hostname,
            "x-internal-purpose" => "federation",
            "x-max-hops"         => @upstream.max_hops,
          })
          begin
            session.declare_exchange(@upstream_q, "x-federation-upstream", passive: true)
          rescue Endpoint::NotFound
            session.declare_exchange(@upstream_q, "x-federation-upstream", passive: false, args: ex_args)
          end
          session.bind_queue(@upstream_q, @upstream_q, "")
          # Register before copying the bindings: exchanges store a binding
          # before notifying observers, so one made in between is either in the
          # copy or reported to us (binding twice is harmless).
          @federated_ex.register_observer(self)
          @federated_ex.bindings_details.each do |binding|
            forward, args = bound_from(binding.arguments)
            session.bind_exchange(@upstream_q, @upstream_exchange, binding.routing_key, args) if forward
          end
        end

        # Removes the upstream queue and exchange, with a session of its own
        # since the link's is closed by now
        private def cleanup
          session = Endpoint.session(@upstream.uri, @upstream.vhost,
            "Federation link cleanup: #{@upstream.name}/#{name}")
          session.open
          begin
            session.delete_queue(@upstream_q)
            session.delete_exchange(@upstream_q)
          ensure
            session.close
          end
        rescue ex
          @log.warn { "Failed to clean up upstream resources: #{ex.message}" }
        end

        private def should_forward?(headers) : Bool
          return true if headers.nil?
          received_from = headers["x-received-from"]?.try(&.as?(Array(AMQ::Protocol::Field)))
          return true unless received_from
          received_from.size < @upstream.max_hops
        end

        # The arguments to bind upstream with: the downstream binding's own,
        # with this hop added to x-bound-from. The first element is false when
        # the binding has travelled max-hops already and isn't forwarded.
        private def bound_from(arguments : AMQ::Protocol::Table?) : Tuple(Bool, AMQ::Protocol::Table)
          # Arguments may be the binding's own table, which must not change
          arguments = arguments.try(&.clone) || AMQ::Protocol::Table.new
          bound_from = arguments["x-bound-from"]?.try(&.as?(Array(AMQ::Protocol::Field))) || Array(AMQ::Protocol::Field).new
          hops = binding_hops(bound_from)
          return {false, arguments} if hops == 0
          bound_from.unshift AMQ::Protocol::Table.new({
            "vhost":    @upstream.vhost.name,
            "exchange": @federated_ex.name,
            "hops":     hops,
          })
          arguments["x-bound-from"] = bound_from
          {true, arguments}
        end

        # The lowest of the previous hop's count minus one and this upstream's
        # max-hops
        private def binding_hops(bound_from) : Int64
          if prev = bound_from.first?.try(&.as?(AMQ::Protocol::Table))
            if hops = prev["hops"]?.try(&.as?(Int64))
              return {hops - 1, @upstream.max_hops}.min
            end
          end
          @upstream.max_hops
        end
      end
    end
  end
end
