require "../observable"
require "amqp-client"
require "../sortable_json"

module AvalancheMQ
  module Federation
    class Upstream
      abstract class Link
        include Observer
        include SortableJSON
        getter last_changed, error

        @state = 0_u8
        @consumer_available = Channel(Nil).new
        @last_changed : Int64?
        @state = State::Terminated
        @stop = ::Channel(Exception?).new
        @error : String?
        @scrubbed_uri : String

        def initialize(@upstream : Upstream, @log : Logger)
          user = UserStore.instance.direct_user
          vhost = @upstream.vhost.name == "/" ? "" : @upstream.vhost.name
          port = Config.instance.amqp_port
          host = Config.instance.amqp_bind
          url = "amqp://#{user.name}:#{user.plain_text_password}@#{host}:#{port}/#{vhost}"
          @local_uri = URI.parse(url)
          uri = @upstream.uri
          ui = uri.userinfo
          @scrubbed_uri = ui.nil? ? uri.to_s : uri.to_s.sub("#{ui}@", "")
        end

        def state
          @state.to_s
        end

        def running?
          @state == State::Running
        end

        def details_tuple
          {
            upstream:       @upstream.name,
            vhost:          @upstream.vhost.name,
            timestamp:      @last_changed ? Time.unix_ms(@last_changed.not_nil!) : nil,
            type:           self.is_a?(QueueLink) ? "queue" : "exchange",
            uri:            @scrubbed_uri,
            resource:       name,
            error:          @error,
            status:         @state.to_s.downcase,
            "consumer-tag": @upstream.consumer_tag,
          }
        end

        def run
          @log.info { "Starting" }
          @state = State::Starting
          spawn(run_loop, name: "Federation link #{@upstream.vhost.name}/#{name}")
          Fiber.yield
        end

        def terminated?
          @state == State::Terminated
        end

        # Does not trigger reconnect, but a graceful close
        def terminate
          return if terminated?
          @stop.send(nil)
          @log.info { "Terminated" }
        end

        private def run_loop
          loop do
            @state = State::Starting
            start_link
            break
          rescue ex
            @state = State::Stopped
            @last_changed = nil
            @error = ex.message
            @log.error { "Federation link error: #{ex.message}" }
            select
            when @stop.receive?
              break
            when timeout @upstream.reconnect_delay.seconds
              @log.info { "Federation try reconnect" }
            end
          end
          @log.info { "Federation link stopped" }
        ensure
          @stop.close
          @last_changed = nil
          @state = State::Terminated
        end

        private def federate(msg, pch, exchange, routing_key)
          msgid = pch.basic_publish(msg.body_io, exchange, routing_key)
          @log.debug { "Federating msgid=#{msgid} routing_key=#{routing_key}" }
          should_multi_ack = msgid % (@upstream.prefetch / 2).ceil.to_i == 0
          if should_multi_ack
            case @upstream.ack_mode
            when AckMode::OnConfirm
              pch.wait_for_confirm(msgid)
              msg.ack(multiple: true)
            when AckMode::OnPublish
              msg.ack(multiple: true)
            when AckMode::NoAck
              nil
            end
          end
        rescue ex
          @stop.send ex unless @stop.closed?
        end

        private def try_passive(client, ch = nil)
          ch ||= client.channel
          {ch, yield(ch, true)}
        rescue ::AMQP::Client::Channel::ClosedException
          ch = client.channel
          {ch, yield(ch, false)}
        end

        private def received_from_header(msg)
          headers = msg.properties.headers || ::AMQP::Client::Arguments.new
          received_from = headers["x-received-from"]?.try(&.as?(Array(::AMQP::Client::Arguments)))
          received_from ||= Array(::AMQP::Client::Arguments).new(1)
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
        abstract def on(event : Symbol, data : Object)
        private abstract def start_link
        private abstract def unregister_observer

        enum State
          Starting
          Running
          Stopped
          Terminated
          Error
        end
      end

      class QueueLink < Link
        EXCHANGE = ""

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
          return if terminated?
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

        private def setup_queue(upstream_client)
          try_passive(upstream_client) do |ch, passive|
            ch.queue_declare(@upstream_q, passive: passive)
          end
        end

        private def start_link
          return if terminated?
          upstream_uri = named_uri(@upstream.uri)
          local_uri = named_uri(@local_uri)
          ::AMQP::Client.start(upstream_uri) do |c|
            ::AMQP::Client.start(local_uri) do |p|
              cch, q = setup_queue(c)
              cch.prefetch(count: @upstream.prefetch)
              pch = p.channel
              pch.confirm_select if @upstream.ack_mode == AckMode::OnConfirm
              no_ack = @upstream.ack_mode == AckMode::NoAck
              @state = State::Running
              @last_changed = Time.utc.to_unix_ms
              @log.debug { "Running" }
              unless @federated_q.immediate_delivery?
                @log.debug { "Waiting for consumers" }
                select
                when @consumer_available.receive?
                # continue
                when ex = @stop.receive?
                  raise ex unless ex.nil?
                  return
                end
              end
              q_name = q[:queue_name]
              cch.basic_consume(q_name, no_ack: no_ack, tag: @upstream.consumer_tag) do |msg|
                headers, received_from = received_from_header(msg)
                received_from << ::AMQP::Client::Arguments.new({
                  "uri"         => @scrubbed_uri,
                  "queue"       => q_name,
                  "redelivered" => msg.redelivered,
                })
                headers["x-received-from"] = received_from
                msg.properties.headers = headers
                federate(msg, pch, EXCHANGE, @federated_q.name)
              end

              if ex = @stop.receive?
                raise ex
              end
              return
            end
          end
        end
      end

      class ExchangeLink < Link
        @consumer_q : ::AMQP::Client::Queue?

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
          return if terminated?
          case event
          when :delete
            @upstream.stop_link(@federated_ex)
          when :bind
            if q = @consumer_q
              if b = data.as?(BindingDetails)
                args = ::AMQP::Client::Arguments.new(b.arguments)
                q.bind(@upstream_exchange, b.routing_key, args: args)
              end
            end
          when :unbind
            if q = @consumer_q
              if b = data.as?(BindingDetails)
                args = ::AMQP::Client::Arguments.new(b.arguments)
                q.unbind(@upstream_exchange, b.routing_key, args: args)
              end
            end
          else raise "Unexpected event '#{event}'"
          end
        end

        def terminate
          super
          cleanup
        end

        private def cleanup
          upstream_uri = @upstream.uri.dup
          params = upstream_uri.query_params
          params["name"] ||= "Federation link cleanup: #{@upstream.name}/#{name}"
          upstream_uri.query = params.to_s
          ::AMQP::Client.start(upstream_uri) do |c|
            ch = c.channel
            ch.queue_delete(@upstream_q)
          end
        end

        private def unregister_observer
          @federated_ex.unregister_observer(self)
        end

        private def setup(upstream_client)
          args = ::AMQP::Client::Arguments.new(@federated_ex.arguments)
          ch, _ = try_passive(upstream_client) do |uch, passive|
            uch.exchange(@upstream_exchange, type: @federated_ex.type,
              args: args, passive: passive)
          end
          args = ::AMQP::Client::Arguments.new({
            "x-downstream-name"  => System.hostname,
            "x-internal-purpose" => "federation",
            "x-max-hops"         => @upstream.max_hops,
          })
          ch, _ = try_passive(upstream_client, ch) do |uch, passive|
            uch.exchange(@upstream_q, type: "x-federation-upstream",
              args: args, passive: passive)
          end
          q_args = {"x-internal-purpose" => "federation"}
          if expires = @upstream.expires
            q_args["x-expires"] = expires
          end
          if msg_ttl = @upstream.msg_ttl
            q_args["x-message-ttl"] = msg_ttl
          end
          ch, q = try_passive(upstream_client, ch) do |uch, passive|
            uch.queue(@upstream_q, args: ::AMQP::Client::Arguments.new(q_args), passive: passive)
          end
          @federated_ex.bindings_details.each do |binding|
            args = ::AMQP::Client::Arguments.new(binding.arguments)
            q.bind(@upstream_exchange, binding.routing_key, args: args)
          end
          {ch, q}
        end

        private def start_link
          return if terminated?
          upstream_uri = named_uri(@upstream.uri)
          local_uri = named_uri(@local_uri)
          ::AMQP::Client.start(upstream_uri) do |c|
            ::AMQP::Client.start(local_uri) do |p|
              cch, @consumer_q = setup(c)
              cch.prefetch(count: @upstream.prefetch)
              pch = p.channel
              pch.confirm_select if @upstream.ack_mode == AckMode::OnConfirm
              no_ack = @upstream.ack_mode == AckMode::NoAck
              @state = State::Running
              @last_changed = Time.utc.to_unix_ms
              @log.debug { "Running" }

              cch.basic_consume(@upstream_q, no_ack: no_ack, tag: @upstream.consumer_tag) do |msg|
                headers, received_from = received_from_header(msg)
                received_from << ::AMQP::Client::Arguments.new({
                  "uri"         => @scrubbed_uri,
                  "exchange"    => @upstream_exchange,
                  "redelivered" => msg.redelivered,
                })
                headers["x-received-from"] = received_from
                msg.properties.headers = headers
                federate(msg, pch, @federated_ex.name, msg.routing_key)
              end

              if ex = @stop.receive?
                raise ex
              end
              return
            end
          end
        end
      end
    end
  end
end
