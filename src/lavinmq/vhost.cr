require "json"
require "../stdlib/*"
require "sync/shared"
require "sync/exclusive"
require "./logger"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/store"
require "./federation/upstream_store"
require "./sortable_json"
require "./amqp/exchange/*"
require "digest/sha1"
require "./amqp/queue"
require "./schema"
require "./event_type"
require "./stats"
require "./queue_factory"
require "./mqtt/session"

module LavinMQ
  class VHost
    include SortableJSON
    include Stats

    rate_stats({"channel_closed", "channel_created", "connection_closed", "connection_created",
                "queue_declared", "queue_deleted", "ack", "deliver", "deliver_no_ack", "deliver_get", "get", "get_no_ack", "publish", "confirm",
                "redeliver", "reject", "consumer_added", "consumer_removed"})

    getter name, data_dir, operator_policies, policies, parameters, shovels, dir, users

    def flow?
      @flow.get(:acquire)
    end

    def flow=(active : Bool)
      @flow.set(active, :release)
    end

    def closed?
      @closed.get(:acquire)
    end

    @flow = Atomic(Bool).new(true)
    @closed = Atomic(Bool).new(false)
    property max_connections : Int32?
    property max_queues : Int32?

    # Synchronized exchange accessors

    def exchanges_byname?(name : String) : Exchange?
      @exchanges.shared { |h| h[name]? }
    end

    def exchanges_byname(name : String) : Exchange
      @exchanges.shared { |h| h[name] }
    end

    def exchanges_fetch(name : String, default) : Exchange?
      @exchanges.shared { |h| h.fetch(name, default) }
    end

    def exchanges_has_key?(name : String) : Bool
      @exchanges.shared { |h| h.has_key?(name) }
    end

    def exchanges_each_value(& : Exchange ->) : Nil
      @exchanges.shared { |h| h.each_value { |v| yield v } }
    end

    def exchanges_each_value : Iterator(Exchange)
      @exchanges.shared(&.values).each
    end

    def exchanges_size : Int32
      @exchanges.shared(&.size)
    end

    def exchanges_any?(& : {String, Exchange} -> Bool) : Bool
      @exchanges.shared { |h| h.any? { |kv| yield kv } }
    end

    # Synchronized queue accessors

    def queues_byname?(name : String) : Queue?
      @queues.shared { |h| h[name]? }
    end

    def queues_byname(name : String) : Queue
      @queues.shared { |h| h[name] }
    end

    def queues_fetch(name : String, default) : Queue?
      @queues.shared { |h| h.fetch(name, default) }
    end

    def queues_has_key?(name : String) : Bool
      @queues.shared { |h| h.has_key?(name) }
    end

    def queues_each_value(& : Queue ->) : Nil
      @queues.shared { |h| h.each_value { |v| yield v } }
    end

    def queues_each_value : Iterator(Queue)
      @queues.shared(&.values).each
    end

    def queues_size : Int32
      @queues.shared(&.size)
    end

    # Synchronized connection accessors

    def connections_each(& : Client ->) : Nil
      @connections.lock { |a| a.each { |c| yield c } }
    end

    def connections_each : Iterator(Client)
      @connections.lock(&.dup).each
    end

    def connections_size : Int32
      @connections.lock(&.size)
    end

    def queues_values : Array(Queue)
      @queues.shared(&.values)
    end

    # Internal: direct write, acquires exclusive lock.
    # Used by Exchange#init_delayed_queue and tests.
    protected def queues_unsafe_put(name : String, queue : Queue) : Nil
      @queues.lock { |h| h[name] = queue }
    end

    # Internal: direct write, acquires exclusive lock.
    # Used by MQTT::Broker during initialization and tests.
    protected def exchanges_unsafe_put(name : String, exchange : Exchange) : Nil
      @exchanges.lock { |h| h[name] = exchange }
    end

    # Internal: clear all queues. Used only in tests.
    protected def queues_clear : Nil
      @queues.lock(&.clear)
    end

    # Synchronized direct reply consumer accessors

    def direct_reply_consumer?(consumer_tag : String) : Client::Channel?
      @direct_reply_consumers.lock { |h| h[consumer_tag]? }
    end

    def direct_reply_consumer_set(consumer_tag : String, channel : Client::Channel) : Nil
      @direct_reply_consumers.lock { |h| h[consumer_tag] = channel }
    end

    def direct_reply_consumer_delete(consumer_tag : String) : Client::Channel?
      @direct_reply_consumers.lock { |h| h.delete(consumer_tag) }
    end

    def direct_reply_consumer_has_key?(consumer_tag : String) : Bool
      @direct_reply_consumers.lock { |h| h.has_key?(consumer_tag) }
    end

    @exchanges : Sync::Shared(Hash(String, Exchange))
    @queues : Sync::Shared(Hash(String, Queue))
    @direct_reply_consumers : Sync::Exclusive(Hash(String, Client::Channel))
    @shovels : Shovel::Store?
    @upstreams : Federation::UpstreamStore?
    @connections : Sync::Exclusive(Array(Client))
    @definitions_file : File
    @definitions_lock = Mutex.new(:reentrant)
    @definitions_file_path : String
    @definitions_deletes = 0
    Log = LavinMQ::Log.for "vhost"

    def initialize(@name : String, @server_data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?, @description = "", @tags = Array(String).new(0))
      @log = Logger.new(Log, vhost: @name)
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir)
      FileUtils.rm_rf File.join(@data_dir, "transient")
      @definitions_file_path = File.join(@data_dir, "definitions.amqp")
      @definitions_file = File.open(@definitions_file_path, "a+")
      @replicator.try &.register_file(@definitions_file)
      File.write(File.join(@data_dir, ".vhost"), @name)
      @exchanges = Sync::Shared.new(Hash(String, Exchange).new)
      @queues = Sync::Shared.new(Hash(String, Queue).new)
      @connections = Sync::Exclusive.new(Array(Client).new(512))
      @direct_reply_consumers = Sync::Exclusive.new(Hash(String, Client::Channel).new)
      load_limits
      @operator_policies = ParameterStore(OperatorPolicy).new(@data_dir, "operator_policies.json", @replicator, vhost: @name)
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @replicator, vhost: @name)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator, vhost: @name)
      @shovels = Shovel::Store.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
      spawn check_consumer_timeouts_loop, name: "Consumer timeouts loop"
    end

    private def check_consumer_timeouts_loop
      loop do
        sleep Config.instance.consumer_timeout_loop_interval.seconds
        return if closed?
        connections_each do |c|
          c.channels.each_value do |ch|
            ch.check_consumer_timeout
          end
        end
      end
    end

    def max_connections=(value : Int32) : Nil
      value = nil if value < 0
      @max_connections = value
      store_limits
    end

    def max_queues=(value : Int32) : Nil
      value = nil if value < 0
      @max_queues = value
      store_limits
    end

    private def load_limits
      File.open(File.join(@data_dir, "limits.json")) do |f|
        limits = JSON.parse(f)
        @max_queues = limits["max-queues"]?.try &.as_i?
        @max_connections = limits["max-connections"]?.try &.as_i?
        @replicator.try &.register_file(f)
      end
    rescue File::NotFoundError
    end

    private def store_limits
      File.open(File.join(@data_dir, "limits.json"), "w") do |f|
        JSON.build(f) do |json|
          json.object do
            json.field "max-queues", @max_queues if @max_queues
            json.field "max-connections", @max_connections if @max_connections
          end
        end
      end
      @replicator.try &.replace_file(File.join(@data_dir, "limits.json"))
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << ">"
    end

    # Queue#publish can raise RejectPublish which should trigger a Nack. All other confirm scenarios
    # should be Acks, apart from Exceptions.
    # As long as at least one queue reject the publish due to overflow a Nack should be sent,
    # even if other queues accepts the message. Behaviour confirmed with RabbitMQ.
    # True if it also succesfully wrote to one or more queues
    # False if no queue was able to receive the message because they're
    # closed
    # The position of the msg.body_io should be at the start of the body
    # When this method finishes, the position will be the same, start of the body
    def publish(msg : Message, immediate = false,
                visited = Set(LavinMQ::Exchange).new, found_queues = Set(LavinMQ::Queue).new) : Bool
      ex = exchanges_byname?(msg.exchange_name) || return false
      ex.publish(msg, immediate, found_queues, visited)
    ensure
      visited.clear
      found_queues.clear
    end

    def details_tuple
      {
        name:          @name,
        dir:           @dir,
        tracing:       false,
        tags:          @tags,
        description:   @description,
        cluster_state: NamedTuple.new,
      }
    end

    def message_details
      ready = unacked = 0_u64
      ack = confirm = deliver = deliver_no_ack = get = get_no_ack = publish = redeliver = return_unroutable = deliver_get = 0_u64
      queues_each_value do |q|
        ready += q.message_count
        unacked += q.unacked_count
        ack += q.ack_count
        confirm += q.confirm_count
        deliver += q.deliver_count
        deliver_no_ack += q.deliver_no_ack_count
        deliver_get += q.deliver_get_count
        get += q.get_count
        get_no_ack += q.get_no_ack_count
        publish += q.publish_count
        redeliver += q.redeliver_count
        return_unroutable += q.return_unroutable_count
      end
      {
        messages:                ready + unacked,
        messages_unacknowledged: unacked,
        messages_ready:          ready,
        message_stats:           {
          ack:               ack,
          confirm:           confirm,
          deliver:           deliver,
          deliver_no_ack:    deliver_no_ack,
          get:               get,
          get_no_ack:        get_no_ack,
          deliver_get:       deliver_get,
          publish:           publish,
          redeliver:         redeliver,
          return_unroutable: return_unroutable,
        },
      }
    end

    def declare_queue(name, durable, auto_delete, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, durable, false,
        auto_delete, false, arguments)
      @log.info { "Created queue: #{name} (durable=#{durable} auto_delete=#{auto_delete} arguments=#{arguments})" }
    end

    def delete_queue(name)
      apply AMQP::Frame::Queue::Delete.new(0_u16, 0_u16, name, false, false, false)
      @log.debug { "Deleted queue: #{name}" }
    end

    def declare_exchange(name, type, durable, auto_delete, internal = false,
                         arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, name, type, false, durable,
        auto_delete, internal, false, arguments)
      @log.info { "Created exchange: #{name} (type=#{type} durable=#{durable} auto_delete=#{auto_delete} arguments=#{arguments})" }
    end

    def delete_exchange(name)
      apply AMQP::Frame::Exchange::Delete.new(0_u16, 0_u16, name, false, false)
      @log.info { "Deleted exchange: #{name}" }
    end

    def bind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    def bind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    def unbind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Unbind.new(0_u16, 0_u16, destination, source,
        routing_key, arguments)
    end

    def unbind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Unbind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def apply(f, loading = false) : Bool
      should_compact = false
      @definitions_lock.synchronize do
        case f
        when AMQP::Frame::Exchange::Declare
          @exchanges.lock do |exchanges|
            return false if exchanges.has_key? f.exchange_name
            e = exchanges[f.exchange_name] =
              make_exchange(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
            apply_policies([e] of Exchange) unless loading
            store_definition(f) if !loading && f.durable
            nil
          end
        when AMQP::Frame::Exchange::Delete
          @exchanges.lock do |exchanges|
            if x = exchanges.delete f.exchange_name
              exchanges.each_value do |ex|
                ex.bindings_details.each do |binding|
                  next unless binding.destination == x
                  ex.unbind(x, binding.routing_key, binding.arguments)
                end
              end
              x.delete
              should_compact = track_dirty_definition(f, !loading && x.durable?)
            else
              return false
            end
            nil
          end
        when AMQP::Frame::Exchange::Bind
          @exchanges.lock do |exchanges|
            src = exchanges[f.source]? || return false
            dst = exchanges[f.destination]? || return false
            return false unless src.bind(dst, f.routing_key, f.arguments)
            store_definition(f) if !loading && src.durable? && dst.durable?
            nil
          end
        when AMQP::Frame::Exchange::Unbind
          @exchanges.lock do |exchanges|
            src = exchanges[f.source]? || return false
            dst = exchanges[f.destination]? || return false
            return false unless src.unbind(dst, f.routing_key, f.arguments)
            should_compact = track_dirty_definition(f, !loading && src.durable? && dst.durable?)
            nil
          end
        when AMQP::Frame::Queue::Declare
          @queues.lock do |queues|
            return false if queues.has_key? f.queue_name
            q = queues[f.queue_name] = QueueFactory.make(self, f)
            apply_policies([q] of Queue) unless loading
            store_definition(f) if !loading && f.durable && !f.exclusive
            event_tick(EventType::QueueDeclared) unless loading
            nil
          end
        when AMQP::Frame::Queue::Delete
          # Lock order: always @exchanges before @queues to prevent deadlocks
          @exchanges.lock do |exchanges|
            @queues.lock do |queues|
              if q = queues.delete(f.queue_name)
                exchanges.each_value do |ex|
                  ex.bindings_details.each do |binding|
                    next unless binding.destination == q
                    ex.unbind(q, binding.routing_key, binding.arguments)
                  end
                end
                should_compact = track_dirty_definition(f, !loading && q.durable? && !q.exclusive?)
                event_tick(EventType::QueueDeleted) unless loading
                q.delete
              else
                return false
              end
              nil
            end
            nil
          end
        when AMQP::Frame::Queue::Bind
          @exchanges.shared do |exchanges|
            @queues.shared do |queues|
              x = exchanges[f.exchange_name]? || return false
              q = queues[f.queue_name]? || return false
              return false unless x.bind(q, f.routing_key, f.arguments)
              store_definition(f) if !loading && x.durable? && q.durable? && !q.exclusive?
              nil
            end
            nil
          end
        when AMQP::Frame::Queue::Unbind
          @exchanges.shared do |exchanges|
            @queues.shared do |queues|
              x = exchanges[f.exchange_name]? || return false
              q = queues[f.queue_name]? || return false
              return false unless x.unbind(q, f.routing_key, f.arguments)
              should_compact = track_dirty_definition(f, !loading && x.durable? && q.durable? && !q.exclusive?)
              nil
            end
            nil
          end
        else raise "Cannot apply frame #{f.class} in vhost #{@name}"
        end
      end
      # Compact outside the hash locks to avoid deadlock
      compact! if should_compact
      true
    end

    def queue_bindings(queue : Queue) : Iterator(BindingDetails)
      default_binding = BindingDetails.new("", name, BindingKey.new(queue.name), queue)
      bindings = exchanges_each_value.flat_map do |ex|
        ex.bindings_details.select { |binding| binding.destination == queue }
      end
      {default_binding}.each.chain(bindings)
    end

    def add_operator_policy(name : String, pattern : String, apply_to : String,
                            definition : Hash(String, JSON::Any), priority : Int8) : OperatorPolicy
      op = OperatorPolicy.new(name, @name, Regex.new(pattern),
        Policy::Target.parse(apply_to), definition, priority)
      @operator_policies.create(op)
      spawn apply_policies, name: "ApplyPolicies (after add) OperatingPolicy #{@name}"
      @log.info { "OperatorPolicy=#{name} Created" }
      op
    end

    def add_policy(name : String, pattern : String, apply_to : String,
                   definition : Hash(String, JSON::Any), priority : Int8) : Policy
      p = Policy.new(name, @name, Regex.new(pattern), Policy::Target.parse(apply_to),
        definition, priority)
      @policies.create(p)
      spawn apply_policies, name: "ApplyPolicies (after add) #{@name}"
      @log.info { "Policy=#{name} Created" }
      p
    end

    def delete_operator_policy(name)
      @operator_policies.delete(name)
      spawn apply_policies, name: "ApplyPolicies (after delete) #{@name}"
      @log.info { "OperatorPolicy=#{name} Deleted" }
    end

    def delete_policy(name)
      @policies.delete(name)
      spawn apply_policies, name: "ApplyPolicies (after delete) #{@name}"
      @log.info { "Policy=#{name} Deleted" }
    end

    def add_connection(client : Client)
      event_tick(EventType::ConnectionCreated)
      @connections.lock { |a| a << client }
    end

    def rm_connection(client : Client)
      event_tick(EventType::ConnectionClosed)
      @connections.lock { |a| a.delete client }
    end

    SHOVEL                  = "shovel"
    FEDERATION_UPSTREAM     = "federation-upstream"
    FEDERATION_UPSTREAM_SET = "federation-upstream-set"

    def add_parameter(p : Parameter)
      @log.debug { "Add parameter #{p.name}" }
      @parameters.create(p)
      apply_parameters(p)
      spawn apply_policies, name: "ApplyPolicies (add parameter) #{@name}"
    end

    def delete_parameter(component_name, parameter_name)
      @log.debug { "Delete parameter #{parameter_name}" }
      @parameters.delete({component_name, parameter_name})
      case component_name
      when SHOVEL
        shovels.delete(parameter_name)
      when FEDERATION_UPSTREAM
        upstreams.delete_upstream(parameter_name)
      when FEDERATION_UPSTREAM_SET
        upstreams.delete_upstream_set(parameter_name)
      else
        @log.warn { "No action when deleting parameter #{component_name}" }
      end
    end

    def stop_shovels
      shovels.each_value &.terminate
    end

    def stop_upstream_links
      upstreams.stop_all
    end

    private def close_connections(reason)
      # Snapshot connections under lock, then close outside lock
      conns = @connections.lock(&.dup)
      WaitGroup.wait do |wg|
        to_close = Channel(Client).new
        fiber_count = 0
        conns.each do |client|
          select
          when to_close.send client
          else # spawn another fiber closing channels
            fiber_id = fiber_count &+= 1
            @log.trace { "spawning close conn fiber #{fiber_id} " }
            client_inner = client
            wg.spawn do
              client_inner.close(reason)
              while client_to_close = to_close.receive?
                client_to_close.close(reason)
              end
              @log.trace { "exiting close conn fiber #{fiber_id} " }
            end
            Fiber.yield
          end
        end
        to_close.close
      end
    end

    def close(reason = "Broker shutdown")
      @closed.set(true, :release)
      stop_shovels
      stop_upstream_links

      @log.info { "Closing connections" }
      close_done = Channel(Nil).new

      spawn do
        close_connections reason
        @log.debug { "Close sent to all connections" }
        close_done.close
      end

      select
      when close_done.receive?
        @log.info { "All connections closed gracefully" }
      when timeout 15.seconds
        @log.warn { "Timeout waiting for connections to close. #{connections_size} left that will be forced closed." }
      end
      close_done.close
      # then force close the remaining (close tcp socket)
      connections_each &.force_close
      Fiber.yield # yield so that Client read_loops can shutdown
      queues_each_value &.close
      Fiber.yield
      @definitions_file.close
      FileUtils.rm_rf File.join(@data_dir, "transient")
    end

    def delete
      close(reason: "VHost deleted")
      Fiber.yield
      FileUtils.rm_rf @data_dir
    end

    private def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      resources = if resources
                    resources.each
                  else
                    Iterator.chain({queues_each_value, exchanges_each_value})
                  end
      policies = @policies.values.sort_by!(&.priority).reverse
      operator_policies = @operator_policies.values.sort_by!(&.priority).reverse
      resources.each do |resource|
        policy = policies.find &.match?(resource)
        operator_policy = operator_policies.find &.match?(resource)
        resource.apply_policy(policy, operator_policy)
      end
    rescue ex : TypeCastError
      @log.error { "Invalid policy. #{ex.message}" }
    end

    private def apply_parameters(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        case p.component_name
        when SHOVEL
          shovels.create(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM
          upstreams.create_upstream(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM_SET
          upstreams.create_upstream_set(p.parameter_name, p.value)
        else
          @log.warn { "No action when applying parameter #{p.component_name}" }
        end
      end
    end

    private def load!
      load_definitions!
      has_parameters = !@parameters.empty?
      has_policies = !@policies.empty? || !@operator_policies.empty?
      if has_parameters || has_policies
        spawn(name: "Load parameters") do
          sleep 10.milliseconds
          next if closed?
          apply_parameters
          apply_policies
        end
      end
      Fiber.yield
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def load_definitions!
      exchanges = Hash(String, AMQP::Frame::Exchange::Declare).new
      queues = Hash(String, AMQP::Frame::Queue::Declare).new
      queue_bindings = Hash(String, Array(AMQP::Frame::Queue::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Queue::Bind).new }
      exchange_bindings = Hash(String, Array(AMQP::Frame::Exchange::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Exchange::Bind).new }
      should_compact = false
      io = @definitions_file
      if io.size.zero?
        load_default_definitions
        compact!
        return
      end

      @log.info { "Loading definitions" }
      @definitions_lock.synchronize do
        @log.debug { "Verifying schema" }
        SchemaVersion.verify(io, :definition)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        loop do
          f = stream.next_frame
          @log.trace { "Reading frame #{f.inspect}" }
          case f
          when AMQP::Frame::Exchange::Declare
            exchanges[f.exchange_name] = f
          when AMQP::Frame::Exchange::Delete
            exchanges.delete f.exchange_name
            exchange_bindings.delete f.exchange_name
            should_compact = true
          when AMQP::Frame::Exchange::Bind
            exchange_bindings[f.destination] << f
          when AMQP::Frame::Exchange::Unbind
            exchange_bindings[f.destination].reject! do |b|
              b.source == f.source &&
                b.routing_key == f.routing_key &&
                b.arguments == f.arguments
            end
            should_compact = true
          when AMQP::Frame::Queue::Declare
            queues[f.queue_name] = f
          when AMQP::Frame::Queue::Delete
            queues.delete f.queue_name
            queue_bindings.delete f.queue_name
            should_compact = true
          when AMQP::Frame::Queue::Bind
            queue_bindings[f.queue_name] << f
          when AMQP::Frame::Queue::Unbind
            queue_bindings[f.queue_name].reject! do |b|
              b.exchange_name == f.exchange_name &&
                b.routing_key == f.routing_key &&
                b.arguments == f.arguments
            end
            should_compact = true
          else
            raise "Cannot apply frame #{f.class} in vhost #{@name}"
          end
        rescue ex : IO::EOFError
          break
        end # loop
      end   # synchronize

      # apply definitions
      @log.info { "Applying #{exchanges.size} exchanges" }
      exchanges.each_value &->self.load_apply(AMQP::Frame)
      @log.info { "Applying #{queues.size} queues" }
      queues.each_value &->self.load_apply(AMQP::Frame)
      @log.info { "Applying #{exchange_bindings.each_value.sum(0, &.size)} exchange bindings" }
      exchange_bindings.each_value &.each(&->self.load_apply(AMQP::Frame))
      @log.info { "Applying #{queue_bindings.each_value.sum(0, &.size)} queue bindings" }
      queue_bindings.each_value &.each(&->self.load_apply(AMQP::Frame))

      @log.info { "Definitions loaded" }
      compact! if should_compact
    end

    protected def load_apply(frame : AMQP::Frame)
      apply frame, loading: true
    rescue ex : LavinMQ::Error
      @log.error(exception: ex) { "Failed to apply frame #{frame.inspect}" }
    end

    private def load_default_definitions
      @log.info { "Loading default definitions" }
      @exchanges.lock do |exchanges|
        exchanges[""] = AMQP::DefaultExchange.new(self, "", true, false, false)
        exchanges["amq.direct"] = AMQP::DirectExchange.new(self, "amq.direct", true, false, false)
        exchanges["amq.fanout"] = AMQP::FanoutExchange.new(self, "amq.fanout", true, false, false)
        exchanges["amq.topic"] = AMQP::TopicExchange.new(self, "amq.topic", true, false, false)
        exchanges["amq.headers"] = AMQP::HeadersExchange.new(self, "amq.headers", true, false, false)
        exchanges["amq.match"] = AMQP::HeadersExchange.new(self, "amq.match", true, false, false)
      end
    end

    # Called from apply() which already holds @exchanges.lock and @queues.lock
    private def compact!(exchanges : Hash(String, Exchange), queues : Hash(String, Queue))
      @definitions_lock.synchronize do
        do_compact!(exchanges, queues)
      end
    end

    # Called during load when no other fibers are active
    private def compact!
      @definitions_lock.synchronize do
        @exchanges.lock do |exchanges|
          @queues.lock do |queues|
            do_compact!(exchanges, queues)
          end
        end
      end
    end

    private def do_compact!(exchanges : Hash(String, Exchange), queues : Hash(String, Queue))
      @log.info { "Compacting definitions" }
      io = File.open("#{@definitions_file_path}.tmp", "a+")
      SchemaVersion.prefix(io, :definition)
      exchanges.each_value.select(&.durable?).each do |e|
        f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
          false, e.durable?, e.auto_delete?, e.internal?,
          false, e.arguments)
        io.write_bytes f
      end
      queues.each_value.select(&.durable?).each do |q|
        f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable?, q.exclusive?,
          q.auto_delete?, false, q.arguments)
        io.write_bytes f
      end
      exchanges.each_value.select(&.durable?).each do |e|
        e.bindings_details.each do |binding|
          args = binding.arguments || AMQP::Table.new
          frame = case binding.destination
                  when Queue
                    AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, binding.destination.name, e.name,
                      binding.routing_key, false, args)
                  when Exchange
                    AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, binding.destination.name, e.name,
                      binding.routing_key, false, args)
                  end
          if f = frame
            io.write_bytes f
          end
        end
      end
      io.fsync
      File.rename io.path, @definitions_file_path
      @replicator.try &.replace_file @definitions_file_path
      @definitions_file.close
      @definitions_file = io
    end

    private def store_definition(frame)
      @log.debug { "Storing definition: #{frame.inspect}" }
      bytes = frame.to_slice
      @definitions_file.write bytes
      @replicator.try &.append @definitions_file_path, bytes
      @definitions_file.fsync
    end

    # Writes a dirty (delete/unbind) definition and returns true if compaction is needed
    private def track_dirty_definition(frame, should_store : Bool) : Bool
      return false unless should_store
      store_definition(frame)
      (@definitions_deletes += 1) >= Config.instance.max_deleted_definitions
    end

    private def make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        if name.empty?
          AMQP::DefaultExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        else
          AMQP::DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        end
      when "fanout"
        AMQP::FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        AMQP::TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        AMQP::HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "x-delayed-message", "x-delayed-exchange"
        arguments = arguments.clone
        type = arguments.delete("x-delayed-type")
        raise Error::ExchangeTypeError.new("Missing required argument 'x-delayed-type'") unless type
        arguments["x-delayed-exchange"] = true
        make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      when "x-federation-upstream"
        AMQP::FederationExchange.new(vhost, name, arguments)
      when "x-consistent-hash"
        AMQP::ConsistentHashExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      else raise Error::ExchangeTypeError.new("invalid exchange type '#{type}'")
      end
    end

    def upstreams
      @upstreams.not_nil!
    end

    def shovels
      @shovels.not_nil!
    end

    def event_tick(event_type)
      case event_type
      in EventType::ChannelClosed        then @channel_closed_count.add(1, :relaxed)
      in EventType::ChannelCreated       then @channel_created_count.add(1, :relaxed)
      in EventType::ConnectionClosed     then @connection_closed_count.add(1, :relaxed)
      in EventType::ConnectionCreated    then @connection_created_count.add(1, :relaxed)
      in EventType::QueueDeclared        then @queue_declared_count.add(1, :relaxed)
      in EventType::QueueDeleted         then @queue_deleted_count.add(1, :relaxed)
      in EventType::ClientAck            then @ack_count.add(1, :relaxed)
      in EventType::ClientPublish        then @publish_count.add(1, :relaxed)
      in EventType::ClientPublishConfirm then @confirm_count.add(1, :relaxed)
      in EventType::ClientRedeliver      then @redeliver_count.add(1, :relaxed)
      in EventType::ClientReject         then @reject_count.add(1, :relaxed)
      in EventType::ConsumerAdded        then @consumer_added_count.add(1, :relaxed)
      in EventType::ConsumerRemoved      then @consumer_removed_count.add(1, :relaxed)
      in EventType::ClientGet
        @get_count.add(1, :relaxed)
        @deliver_get_count.add(1, :relaxed)
      in EventType::ClientGetNoAck
        @get_no_ack_count.add(1, :relaxed)
        @deliver_get_count.add(1, :relaxed)
      in EventType::ClientDeliver
        @deliver_count.add(1, :relaxed)
        @deliver_get_count.add(1, :relaxed)
      in EventType::ClientDeliverNoAck
        @deliver_no_ack_count.add(1, :relaxed)
        @deliver_get_count.add(1, :relaxed)
      end
    end

    def sync : Nil
      {% if flag?(:linux) %}
        ret = LibC.syncfs(@definitions_file.fd)
        raise IO::Error.from_errno("syncfs") if ret != 0
      {% else %}
        LibC.sync
      {% end %}
    end
  end
end
