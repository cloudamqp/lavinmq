require "../observable"
require "amqp-client"
require "../logger"
require "../sortable_json"
require "../amqp/queue/event"
require "../amqp/exchange/event"

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
        @metadata : ::Log::Metadata

        def initialize(@upstream : Upstream)
          @metadata = ::Log::Metadata.new(nil, {vhost: @upstream.vhost.name, upstream: @upstream.name})
          @log = Logger.new(Log, @metadata)
          user = @upstream.vhost.users.direct_user
          vhost = @upstream.vhost.name == "/" ? "" : @upstream.vhost.name
          port = Config.instance.amqp_port
          host = Config.instance.amqp_bind
          url = "amqp://#{user.name}:#{user.plain_text_password}@#{host}:#{port}/#{vhost}"
          @local_uri = URI.parse(url)
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
          @log.debug { "state change #{@state}->#{state}" }
          @last_changed = RoughTime.unix_ms
          @state = state
        end

        # Does not trigger reconnect, but a graceful close
        def terminate
          return if @state.terminated?
          state(State::Terminating)
          @upstream_connection.try &.close
        end

        private def run_loop
          loop do
            break if @state.terminating?
            state(State::Starting)
            start_link
            break if @state.terminating?
            state(State::Stopped)
            sleep @upstream.reconnect_delay.seconds
            @log.info { "Federation try reconnect" }
          rescue ex
            break if @state.terminating?
            @log.info { "Federation link state=#{@state} error=#{ex.inspect}" }
            state(State::Stopped)
            @error = ex.message
            sleep @upstream.reconnect_delay.seconds
            @log.info { "Federation try reconnect" }
          end
          @log.info { "Federation link stopped" }
        ensure
          state(State::Terminated)
          @log.info { "Terminated" }
        end

        private def federate(msg, upstream_ch, exchange, routing_key, immediate = false)
          @log.debug { "Federating routing_key=#{routing_key} exchange=#{exchange}" }
          status = @upstream.vhost.publish(
            Message.new(
              RoughTime.unix_ms, exchange, routing_key, msg.properties,
              msg.body_io.bytesize.to_u64, msg.body_io,
            ),
            immediate
          )

          if status
            ack(msg.delivery_tag, upstream_ch) if @upstream.ack_mode != AckMode::NoAck
          else
            reject(msg.delivery_tag, upstream_ch)
          end
          status
        end

        private def ack(delivery_tag, upstream_ch)
          return unless delivery_tag
          if ch = upstream_ch
            raise "Channel closed when acking" if ch.closed?
            ch.basic_ack(delivery_tag)
          end
        end

        private def reject(delivery_tag, upstream_ch)
          return unless delivery_tag
          if ch = upstream_ch
            raise "Channel closed when rejecting" if ch.closed?
            ch.basic_reject(delivery_tag, true)
          end
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
          return if @state.terminated?
          @upstream_connection.try &.close
          upstream_uri = named_uri(@upstream.uri)
          params = upstream_uri.query_params
          params["product"] = "LavinMQ"
          params["product_version"] = LavinMQ::VERSION.to_s
          upstream_uri.query = params.to_s
          ::AMQP::Client.start(upstream_uri) do |upstream_connection|
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
        include Observer(QueueEvent)
        @consumer_available = Channel(Nil).new(1)
        EXCHANGE = ""

        def initialize(@upstream : Upstream, @federated_q : Queue, @upstream_q : String)
          @federated_q.register_observer(self)
          consumer_available if @federated_q.immediate_delivery?
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_q.name})
        end

        def name : String
          @federated_q.name
        end

        def terminate
          @federated_q.unregister_observer(self)
          super
          @consumer_available.close
        end

        private def consumer_available
          select
          when @consumer_available.send nil
          else
          end
        end

        def on(event : QueueEvent, data)
          return if @state.terminated? || @state.terminating?
          @log.debug { "event=#{event} data=#{data}" }
          case event
          in .deleted?, .closed?
            @upstream.stop_link(@federated_q)
          in .consumer_added?
            consumer_available
          in .consumer_removed?
            nil
          end
        rescue e
          @log.error { "Could not process event=#{event} data=#{data} error=#{e.inspect_with_backtrace}" }
        end

        private def setup_queue(upstream_client)
          try_passive(upstream_client) do |ch, passive|
            ch.queue_declare(@upstream_q, passive: passive)
          end
        end

        private def start_link
          setup_connection do |upstream_connection|
            upstream_channel, q = setup_queue(upstream_connection)
            upstream_channel.prefetch(count: @upstream.prefetch)
            no_ack = @upstream.ack_mode.no_ack?
            state(State::Running)
            unless @federated_q.immediate_delivery?
              @log.debug { "Waiting for consumers" }
              loop do
                select
                when @consumer_available.receive?
                  break
                when timeout(1.second)
                  return if @upstream_connection.try &.closed?
                end
              end
            end
            q_name = q[:queue_name]
            upstream_channel.basic_consume(q_name, no_ack: no_ack, tag: @upstream.consumer_tag, block: true) do |msg|
              @last_changed = RoughTime.unix_ms
              headers, received_from = received_from_header(msg)
              received_from << ::AMQP::Client::Arguments.new({
                "uri"         => @scrubbed_uri,
                "queue"       => q_name,
                "redelivered" => msg.redelivered,
              })
              headers["x-received-from"] = received_from
              msg.properties.headers = headers

              unless federate(msg, upstream_channel.not_nil!, EXCHANGE, @federated_q.name, true)
                raise NoDownstreamConsumerError.new("Federate failed, no downstream consumer available")
              end
            end
          rescue ex : NoDownstreamConsumerError
            @log.warn(ex) { "No downstream consumer active, stopping federation" }
          end
        end
      end

      class ExchangeLink < Link
        include Observer(ExchangeEvent)
        @consumer_ex : ::AMQP::Client::Exchange?

        def initialize(@upstream : Upstream, @federated_ex : Exchange, @upstream_q : String,
                       @upstream_exchange : String)
          super(@upstream)
          @metadata = @metadata.extend({link: @federated_ex.name})
        end

        def name : String
          @federated_ex.name
        end

        def on(event : ExchangeEvent, data)
          return if @state.terminated? || @state.terminating?
          @log.debug { "event=#{event} data=#{data}" }
          case event
          in .deleted?
            @upstream.stop_link(@federated_ex)
          in .bind?
            b = data_as_binding_details(data)
            # Must clone args here, else we'll add stuff to
            # the arguments to the source exchange's binding
            args = b.arguments.clone || ::AMQP::Client::Arguments.new
            updated, args = update_bound_from?(args)
            if updated
              with_consumer_ex do |ex|
                ex.bind(@upstream_exchange, b.routing_key, args: args)
              end
            end
          in .unbind?
            with_consumer_ex do |ex|
              b = data_as_binding_details(data)
              args = b.arguments || ::AMQP::Client::Arguments.new
              ex.unbind(@upstream_exchange, b.routing_key, args: args)
            end
          end
        rescue e
          @log.error { "Could not process event=#{event} data=#{data} error=#{e.inspect_with_backtrace}" }
        end

        private def data_as_binding_details(data) : BindingDetails
          b = data.as?(BindingDetails)
          raise ArgumentError.new("Expected data to be of type BindingDetails") unless b
          b
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
          @federated_ex.unregister_observer(self)
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
            ch.queue_bind(@upstream_q, @upstream_q, routing_key: "")
            ex
          end
          @federated_ex.register_observer(self)
          @federated_ex.bindings_details.each do |binding|
            # Must clone args here, else we'll add stuff to
            # the arguments to the source exchange's binding
            args = binding.arguments.clone || ::AMQP::Client::Arguments.new
            updated, args = update_bound_from?(args)
            if updated
              consumer_ex.bind(@upstream_exchange, binding.routing_key, args: args)
            end
          end
          {ch, consumer_ex}
        end

        private def start_link
          setup_connection do |upstream_connection|
            upstream_channel, @consumer_ex = setup(upstream_connection)
            upstream_channel.prefetch(count: @upstream.prefetch)
            no_ack = @upstream.ack_mode.no_ack?
            state(State::Running)
            upstream_channel.basic_consume(@upstream_q, no_ack: no_ack, tag: @upstream.consumer_tag, block: true) do |msg|
              @last_changed = RoughTime.unix_ms
              headers, received_from = received_from_header(msg)
              received_from << ::AMQP::Client::Arguments.new({
                "uri"         => @scrubbed_uri,
                "exchange"    => @upstream_exchange,
                "redelivered" => msg.redelivered,
              })
              headers["x-received-from"] = received_from
              msg.properties.headers = headers
              federate(msg, upstream_channel.not_nil!, @federated_ex.name, msg.routing_key)
            end
          ensure
            @consumer_ex = nil
          end
        end

        # This methods returns a tuple where the first element is a boolean
        # indicating whether the arguments were updated, and the second
        # element is the updated arguments.
        # If the arguments were not updated, it means that max hops has been reached
        # and the binding should not be created.
        private def update_bound_from?(arguments : ::AMQP::Client::Arguments)
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

    class NoDownstreamConsumerError < Exception
    end
  end
end
