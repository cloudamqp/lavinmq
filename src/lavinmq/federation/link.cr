require "amqp-client"
require "../logger"
require "../sortable_json"

module LavinMQ
  module Federation
    class Upstream
      abstract class Link
        include SortableJSON
        Log = LavinMQ::Log.for "federation.link"
        getter last_changed, error, state

        @last_changed : Int64?
        @state = State::Stopped
        @error : String?
        @scrubbed_uri : String
        @last_unacked : UInt64?
        @upstream_connection : ::AMQP::Client::Connection?
        @upstream_channel : ::AMQP::Client::Channel?
        @metadata : ::Log::Metadata
        @state_changed = Channel(State?).new

        def initialize(@upstream : Upstream)
          @metadata = ::Log::Metadata.new(nil, {vhost: @upstream.vhost.name, upstream: @upstream.name})
          @log = Logger.new(Log, @metadata)
          uri = @upstream.uri
          ui = uri.userinfo
          @scrubbed_uri = ui.nil? ? uri.to_s : uri.to_s.sub("#{ui}@", "")
        end

        def details_tuple
          {
            upstream:       @upstream.name,
            vhost:          @upstream.vhost.name,
            timestamp:      @last_changed.try { |v| Time.unix_ms(v) },
            type:           self.is_a?(QueueLink) ? "queue" : "exchange",
            uri:            @scrubbed_uri,
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

        private def state(state)
          @log.debug { "state change from=#{@state} to=#{state}" }
          @last_changed = RoughTime.unix_ms
          return if @state == state
          @state = state
          loop { @state_changed.try_send?(state) || break }
        end

        # Does not trigger reconnect, but a graceful close
        def terminate
          return if @state.terminated?
          state(State::Terminating)
          @upstream_connection.try &.close
        end

        private def run_loop
          loop do
            break if stop_link?
            state(State::Starting)
            start_link
            break if stop_link?
            state(State::Stopped)
            wait_before_reconnect
            break if stop_link?
            @log.info { "Federation try reconnect" }
          rescue ex
            break if stop_link?
            @log.info { "Federation link state=#{@state} error=#{ex.inspect}" }
            state(State::Stopped)
            @error = ex.message
            wait_before_reconnect
            break if stop_link?
            @log.info { "Federation try reconnect" }
          end
          @log.info { "Federation link stopped" }
        ensure
          state(State::Terminated)
          @state_changed.close
          @log.info { "Terminated" }
        end

        private def wait_before_reconnect
          loop do
            select
            when timeout @upstream.reconnect_delay
              @log.debug { "#wait_before_reconnect timeout after #{@upstream.reconnect_delay}" }
              break
            when event = @state_changed.receive?
              break if stop_link?(event)
              @log.debug { "#wait_before_reconnect @state_changed.received? triggerd " \
                           "@state_changed.closed?=#{@state_changed.closed?}" }
            end
          end
        end

        private def stop_link?(state = @state)
          return false if state.nil?
          state.in?(State::Terminating, State::Terminated)
        end

        private def federate(msg, exchange, routing_key, *, immediate = false)
          @log.debug { "Federating routing_key=#{routing_key} exchange=#{exchange}" }
          @upstream.vhost.publish(
            Message.new(
              RoughTime.unix_ms, exchange, routing_key, msg.properties,
              msg.body_io.bytesize.to_u64, msg.body_io,
            ),
            immediate
          )
        end

        private def try_passive(client, ch = nil, &)
          ch ||= client.channel
          {ch, yield(ch, true)}
        rescue ::AMQP::Client::Channel::ClosedException
          ch = client.channel
          {ch, yield(ch, false)}
        end

        private def received_from_header(msg)
          headers = msg.properties.headers || AMQP::Table.new
          received_from = headers["x-received-from"]?.try(&.as?(Array(AMQP::Field)))
          received_from ||= Array(AMQP::Table).new
          {headers, received_from}
        end

        private def named_uri(uri)
          named_uri = uri.dup
          params = named_uri.query_params
          params["name"] ||= "Federation link: #{@upstream.name}/#{name}"
          named_uri.query = params.to_s
          named_uri
        end

        abstract def name : String
        private abstract def start_link

        private def setup_connection(&)
          return if @state.in?(State::Terminated, State::Terminating)
          @upstream_connection.try &.close
          upstream_uri = named_uri(@upstream.uri)
          params = upstream_uri.query_params
          params["product"] = "LavinMQ"
          params["product_version"] = LavinMQ::VERSION.to_s
          upstream_uri.query = params.to_s
          ::AMQP::Client.start(upstream_uri) do |upstream_connection|
            upstream_connection.on_close do
              next if stop_link?
              state(State::Stopped)
            end
            yield @upstream_connection = upstream_connection
          end
        end

        enum State
          Starting
          Running
          Stopped
          Terminating
          Terminated
          Error
        end
      end

      class QueueLink < Link
        EXCHANGE = ""

        @consumer_available = Channel(Nil).new

        def initialize(@upstream : Upstream, @federated_q : Queue, @upstream_q : String)
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_q.name})

          spawn(monitor_consumers, name: "#{@federated_q.name}: consumer monitor")
        end

        def monitor_consumers
          # We need an initial value
          has_consumer = !@federated_q.consumers_empty.value
          @log.debug { "initial has_consumer = #{has_consumer}" }
          loop do
            if has_consumer
              # Signal
              notify_consumer_available
              # Wait for queue to lose all consumers, or for the link
              # to stop
              loop do
                select
                when @federated_q.consumers_empty.when_true.receive
                  break
                when state = @state_changed.receive?
                  return if stop_link?(state)
                  return if @state_changed.closed? # closed == stop
                end
              end
              @log.info { "Lost consumers, cancel upstream subscriber" }
              has_consumer = false
              # cancel our consumer!
              if channel = @upstream_channel
                begin
                  channel.basic_cancel(@upstream.consumer_tag)
                rescue ex : ::AMQP::Client::Error
                  @log.debug(exception: ex) { "Tried to cancel upstream consumer tag=#{@upstream.consumer_tag}" }
                end
              end
            else
              # Wait for queue get a consumer, or for the link
              # to stop
              loop do
                select
                when @federated_q.consumers_empty.when_false.receive
                  break
                when state = @state_changed.receive?
                  return if stop_link?(state)
                  return if @state_changed.closed?
                end
              end
              # Signaling is done first in the next iteration of the loop when
              # we enter the `has_consumer?` case
              @log.info { "Got consumers, signal to start subscriber" }
              has_consumer = true
            end
          end
        rescue ::Channel::ClosedError
        end

        def name : String
          @federated_q.name
        end

        def terminate
          super
          @consumer_available.close
        end

        private def notify_consumer_available
          select
          when @consumer_available.send nil
          when @federated_q.consumers_empty.when_true.receive
          end
        end

        private def setup_queue(upstream_client)
          try_passive(upstream_client) do |ch, passive|
            ch.queue_declare(@upstream_q, passive: passive)
            ::AMQP::Client::Queue.new(ch, @upstream_q)
          end
        end

        private def consume_upstream_and_federate(queue, no_ack)
          queue.subscribe(tag: @upstream.consumer_tag, no_ack: no_ack, block: true) do |msg|
            @last_changed = RoughTime.unix_ms
            headers, received_from = received_from_header(msg)
            received_from << ::AMQP::Client::Arguments.new({
              "uri"         => @scrubbed_uri,
              "queue"       => queue.name,
              "redelivered" => msg.redelivered,
            })
            headers["x-received-from"] = received_from
            msg.properties.headers = headers

            if federate(msg, EXCHANGE, @federated_q.name, immediate: true)
              msg.ack if @upstream.ack_mode != AckMode::NoAck
            else
              @log.info { "Federate failed, no downstream consumer available" }
              queue.unsubscribe(@upstream.consumer_tag)
              msg.reject(requeue: true)
            end
          end
        end

        private def start_link
          setup_connection do |upstream_connection|
            upstream_channel, q = setup_queue(upstream_connection)
            @upstream_channel = upstream_channel
            upstream_channel.prefetch(count: @upstream.prefetch)
            no_ack = @upstream.ack_mode.no_ack?
            state(State::Running)
            loop do
              @log.debug { "Waiting for consumers" }
              select
              when @consumer_available.receive?
                consume_upstream_and_federate(q, no_ack)
              when @state_changed.receive?
                return if @state_changed.closed?
                return if @upstream_connection.try &.closed?
              end
            end
          end
        end
      end

      class ExchangeLink < Link
        @consumer_ex : ::AMQP::Client::Exchange?
        @bind_cb : Proc(BindingDetails, Nil)?
        @unbind_cb : Proc(BindingDetails, Nil)?
        @deleted_cb : Proc(Nil)?

        def initialize(@upstream : Upstream, @federated_ex : Exchange, @upstream_q : String,
                       @upstream_exchange : String)
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_ex.name})
        end

        def name : String
          @federated_ex.name
        end

        private def should_forward?(headers)
          return true if headers.nil?
          x_received_from = headers["x-received-from"]?.try(&.as?(Array(AMQP::Field)))
          return true unless x_received_from
          x_received_from.size < @upstream.max_hops
        end

        private def on_exchange_bind(b : BindingDetails)
          return if @state.terminated? || @state.terminating?
          @log.debug { "event=bind data=#{b}" }
          updated, args = update_bound_from?(b.arguments)
          if updated
            with_consumer_ex do |ex|
              ex.bind(@upstream_exchange, b.routing_key, args: args)
            end
          end
        rescue e
          @log.error { "Could not process event=bind data=#{b} error=#{e.inspect_with_backtrace}" }
        end

        private def on_exchange_unbind(b : BindingDetails)
          return if @state.terminated? || @state.terminating?
          @log.debug { "event=unbind data=#{b}" }
          updated, args = update_bound_from?(b.arguments)
          if updated
            with_consumer_ex do |ex|
              ex.unbind(@upstream_exchange, b.routing_key, args: args)
            end
          end
        rescue e
          @log.error { "Could not process event=unbind data=#{b} error=#{e.inspect_with_backtrace}" }
        end

        private def on_exchange_deleted
          return if @state.terminated? || @state.terminating?
          @log.debug { "event=deleted" }
          @upstream.stop_link(@federated_ex)
        rescue e
          @log.error { "Could not process event=deleted error=#{e.inspect_with_backtrace}" }
        end

        private def with_consumer_ex(&)
          if ex = @consumer_ex
            yield ex
          else
            @log.warn { "No upstream connection for exchange event" }
          end
        end

        def terminate
          super
          @bind_cb.try { |cb| @federated_ex.off_bind(cb) }
          @unbind_cb.try { |cb| @federated_ex.off_unbind(cb) }
          @deleted_cb.try { |cb| @federated_ex.off_deleted(cb) }
          cleanup
        end

        private def cleanup
          upstream_uri = @upstream.uri.dup
          params = upstream_uri.query_params
          params["name"] ||= "Federation link cleanup: #{@upstream.name}/#{name}"
          params["product"] = "LavinMQ"
          params["product_version"] = LavinMQ::VERSION.to_s
          upstream_uri.query = params.to_s
          ::AMQP::Client.start(upstream_uri) do |c|
            ch = c.channel
            ch.queue_delete(@upstream_q)
            ch.exchange_delete(@upstream_q)
          rescue ex : ::AMQP::Client::Error
            @log.warn { "Failed to clean up upstream resources: #{ex.message}" }
          end
        rescue e
          @log.warn(e) { "cleanup interrupted " }
        end

        private def setup(upstream_client)
          ch, _ = try_passive(upstream_client) do |uch, passive|
            uch.exchange(@upstream_exchange, type: @federated_ex.type,
              args: @federated_ex.arguments, passive: passive)
          end
          args2 = ::AMQP::Client::Arguments.new({
            "x-downstream-name"  => System.hostname,
            "x-internal-purpose" => "federation",
            "x-max-hops"         => @upstream.max_hops,
          })
          q_args = ::AMQP::Client::Arguments.new({"x-internal-purpose" => "federation"})
          if expires = @upstream.expires
            q_args["x-expires"] = expires
          end
          if msg_ttl = @upstream.msg_ttl
            q_args["x-message-ttl"] = msg_ttl
          end
          ch, _ = try_passive(upstream_client, ch) do |uch, passive|
            uch.queue(@upstream_q, args: q_args, passive: passive)
          end
          ch, consumer_ex = try_passive(upstream_client, ch) do |uch, passive|
            ex = uch.exchange(@upstream_q, type: "x-federation-upstream",
              args: args2, passive: passive)
            uch.queue_bind(@upstream_q, @upstream_q, routing_key: "")
            ex
          end
          @bind_cb = @federated_ex.on_bind { |data| on_exchange_bind(data) }
          @unbind_cb = @federated_ex.on_unbind { |data| on_exchange_unbind(data) }
          @deleted_cb = @federated_ex.on_deleted { on_exchange_deleted }
          @federated_ex.bindings_details.each do |binding|
            updated, args = update_bound_from?(binding.arguments)
            if updated
              consumer_ex.bind(@upstream_exchange, binding.routing_key, args: args)
            end
          end
          upstream_q = ch.queue(@upstream_q, args: q_args, passive: true)
          {ch, consumer_ex, upstream_q}
        end

        private def start_link
          setup_connection do |upstream_connection|
            upstream_channel, @consumer_ex, upstream_q = setup(upstream_connection)
            upstream_channel.prefetch(count: @upstream.prefetch)
            no_ack = @upstream.ack_mode.no_ack?
            state(State::Running)
            upstream_q.subscribe(no_ack: no_ack, tag: @upstream.consumer_tag, block: true) do |msg|
              if should_forward?(msg.properties.headers)
                federate_to_downstream_exchange(msg)
              else
                @log.debug { "Skipping message, max hops reached" }
                msg.ack
              end
            end
          ensure
            @consumer_ex = nil
          end
        end

        private def federate_to_downstream_exchange(msg)
          @last_changed = RoughTime.unix_ms
          headers, received_from = received_from_header(msg)
          received_from << ::AMQP::Client::Arguments.new({
            "uri"         => @scrubbed_uri,
            "exchange"    => @upstream_exchange,
            "redelivered" => msg.redelivered,
          })
          headers["x-received-from"] = received_from
          msg.properties.headers = headers
          # Because we publish with immediate false we'll always get a succesful
          # return and never gets a reason to reject and requeue
          federate(msg, @federated_ex.name, msg.routing_key, immediate: false)
          msg.ack unless @upstream.ack_mode.no_ack?
        end

        # This methods returns a tuple where the first element is a boolean
        # indicating whether the arguments were updated, and the second
        # element is the updated arguments.
        # If the arguments were not updated, it means that max hops has been reached
        # and the binding should not be created.
        private def update_bound_from?(arguments : ::AMQP::Client::Arguments?)
          # Arguments may be a reference to the arguments in a binding, and we don't
          # want to be changed, therefore we clone it.
          arguments = arguments.try &.clone || ::AMQP::Client::Arguments.new
          bound_from = arguments["x-bound-from"]?.try(&.as?(Array(AMQP::Field)))
          bound_from ||= Array(AMQP::Field).new
          hops = get_binding_hops(bound_from)
          return {false, arguments} if hops == 0
          bound_from.unshift AMQP::Table.new({
            "vhost":    @upstream.vhost.name,
            "exchange": @federated_ex.name,
            "hops":     hops,
          })
          arguments["x-bound-from"] = bound_from
          {true, arguments}
        end

        # Calculate the number of hops for the binding. It will use the lowest value
        # from the previous hops in the binding or the max hops configured on the current
        # exchange.
        private def get_binding_hops(x_bound_from)
          if prev = x_bound_from.first?.try(&.as?(AMQP::Table))
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
