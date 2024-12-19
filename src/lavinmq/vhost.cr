require "json"
require "../stdlib/*"
require "./logger"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/shovel_store"
require "./federation/upstream_store"
require "./sortable_json"
require "./exchange"
require "digest/sha1"
require "./amqp/queue"
require "./schema"
require "./event_type"
require "./stats"
require "./queue_factory"

module LavinMQ
  class VHost
    include SortableJSON
    include Stats

    rate_stats({"channel_closed", "channel_created", "connection_closed", "connection_created",
                "queue_declared", "queue_deleted", "ack", "deliver", "deliver_get", "get", "publish", "confirm",
                "redeliver", "reject", "consumer_added", "consumer_removed"})

    getter name, exchanges, queues, data_dir, operator_policies, policies, parameters, shovels,
      direct_reply_consumers, connections, dir, users
    property? flow = true
    getter? closed = false
    property max_connections : Int32?
    property max_queues : Int32?

    @exchanges = Hash(String, Exchange).new
    @queues = Hash(String, Queue).new
    @direct_reply_consumers = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    @connections = Array(Client).new(512)
    @definitions_file : File
    @definitions_lock = Mutex.new(:reentrant)
    @definitions_file_path : String
    @definitions_deletes = 0
    Log = LavinMQ::Log.for "vhost"

    def initialize(@name : String, @server_data_dir : String, @users : UserStore, @replicator : Clustering::Replicator, @description = "", @tags = Array(String).new(0))
      @log = Logger.new(Log, vhost: @name)
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir)
      @definitions_file_path = File.join(@data_dir, "definitions.amqp")
      @definitions_file = File.open(@definitions_file_path, "a+")
      @replicator.register_file(@definitions_file)
      File.write(File.join(@data_dir, ".vhost"), @name)
      load_limits
      @operator_policies = ParameterStore(OperatorPolicy).new(@data_dir, "operator_policies.json", @replicator, vhost: @name)
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @replicator, vhost: @name)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator, vhost: @name)
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
      spawn check_consumer_timeouts_loop, name: "Consumer timeouts loop"
    end

    private def check_consumer_timeouts_loop
      loop do
        sleep Config.instance.consumer_timeout_loop_interval.seconds
        return if @closed
        @connections.each do |c|
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
                visited = Set(Exchange).new, found_queues = Set(Queue).new) : Bool
      ex = @exchanges[msg.exchange_name]? || return false
      published_queue_count = ex.publish(msg, immediate, found_queues, visited)
      !published_queue_count.zero?
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
      ack = confirm = deliver = get = get_no_ack = publish = redeliver = return_unroutable = deliver_get = 0_u64
      @queues.each_value do |q|
        ready += q.message_count
        unacked += q.unacked_count
        ack += q.ack_count
        confirm += q.confirm_count
        deliver += q.deliver_count
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
      @log.info { "Deleted queue: #{name}" }
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
      @definitions_lock.synchronize do
        case f
        when AMQP::Frame::Exchange::Declare
          return false if @exchanges.has_key? f.exchange_name
          e = @exchanges[f.exchange_name] =
            make_exchange(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
          apply_policies([e] of Exchange) unless loading
          store_definition(f) if !loading && f.durable
        when AMQP::Frame::Exchange::Delete
          if x = @exchanges.delete f.exchange_name
            @exchanges.each_value do |ex|
              ex.bindings_details.each do |binding|
                next unless binding.destination == x
                ex.unbind(x, binding.routing_key, binding.arguments)
              end
            end
            x.delete
            store_definition(f, dirty: true) if !loading && x.durable?
          else
            return false
          end
        when AMQP::Frame::Exchange::Bind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.bind(dst, f.routing_key, f.arguments)
          store_definition(f) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Exchange::Unbind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.unbind(dst, f.routing_key, f.arguments)
          store_definition(f, dirty: true) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Queue::Declare
          return false if @queues.has_key? f.queue_name
          q = @queues[f.queue_name] = QueueFactory.make(self, f)
          apply_policies([q] of Queue) unless loading
          store_definition(f) if !loading && f.durable && !f.exclusive
          event_tick(EventType::QueueDeclared) unless loading
        when AMQP::Frame::Queue::Delete
          if q = @queues.delete(f.queue_name)
            @exchanges.each_value do |ex|
              ex.bindings_details.each do |binding|
                next unless binding.destination == q
                ex.unbind(q, binding.routing_key, binding.arguments)
              end
            end
            store_definition(f, dirty: true) if !loading && q.durable? && !q.exclusive?
            event_tick(EventType::QueueDeleted) unless loading
            q.delete
          else
            return false
          end
        when AMQP::Frame::Queue::Bind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.bind(q, f.routing_key, f.arguments)
          store_definition(f) if !loading && x.durable? && q.durable? && !q.exclusive?
        when AMQP::Frame::Queue::Unbind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.unbind(q, f.routing_key, f.arguments)
          store_definition(f, dirty: true) if !loading && x.durable? && q.durable? && !q.exclusive?
        else raise "Cannot apply frame #{f.class} in vhost #{@name}"
        end
        true
      end
    end

    def queue_bindings(queue : Queue) : Iterator(BindingDetails)
      default_binding = BindingDetails.new("", name, BindingKey.new(queue.name), queue)
      bindings = @exchanges.each_value.flat_map do |ex|
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
      @connections << client
    end

    def rm_connection(client : Client)
      event_tick(EventType::ConnectionClosed)
      @connections.delete client
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
      WaitGroup.wait do |wg|
        to_close = Channel(Client).new
        fiber_count = 0
        @connections.each do |client|
          select
          when to_close.send client
          else
            fiber_id = fiber_count &+= 1
            @log.trace { "spawning close conn fiber #{fiber_id} " }
            wg.spawn do
              client.close(reason)
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
      @closed = true
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
        @log.warn { "Timeout waiting for connections to close. #{@connections.size} left that will be forced closed." }
      end
      close_done.close
      # then force close the remaining (close tcp socket)
      @connections.each &.force_close
      Fiber.yield # yield so that Client read_loops can shutdown
      @queues.each_value &.close
      Fiber.yield
      @definitions_file.close
    end

    def delete
      close(reason: "VHost deleted")
      Fiber.yield
      FileUtils.rm_rf @data_dir
    end

    private def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      itr = if resources
              resources.each
            else
              Iterator.chain({@queues.each_value, @exchanges.each_value})
            end
      policies = @policies.values.sort_by!(&.priority).reverse
      operator_policies = @operator_policies.values.sort_by!(&.priority).reverse
      itr.each do |r|
        policy = policies.find &.match?(r)
        operator_policy = operator_policies.find &.match?(r)
        r.apply_policy(policy, operator_policy)
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
      spawn(name: "Load parameters") do
        sleep 10.milliseconds
        next if @closed
        apply_parameters
        apply_policies
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
        return
      end
      if etcd = @replicator.etcd
        load_definitions_from_etcd(etcd)
        return
      end
      @log.info { "Loading definitions" }
      @definitions_lock.synchronize do
        @log.debug { "Verifying schema" }
        SchemaVersion.verify(io, :definition)
        loop do
          AMQP::Frame.from_io(io, IO::ByteFormat::SystemEndian) do |f|
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
          end # Frame.from_io
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
      raise Error::InvalidDefinitions.new("Invalid frame: #{frame.inspect}")
    end

    private def load_default_definitions
      @log.info { "Loading default definitions" }
      declare_exchange("", "direct", true, false)
      declare_exchange("amq.direct", "direct", true, false)
      declare_exchange("amq.fanout", "fanout", true, false)
      declare_exchange("amq.topic", "topic", true, false)
      declare_exchange("amq.headers", "headers", true, false)
      declare_exchange("amq.match", "headers", true, false)
    end

    private def load_definitions_from_etcd(etcd : Etcd)
      etcd.get_prefix(etcd_path("queues")).each do |key, value|
        queue_name = ""
        key.split('/') { |s| queue_name = URI.decode_www_form(s) } # get last split value without allocation
        json = JSON.parse(value)
        @queues[queue_name] = QueueFactory.make(self, json)
      end

      etcd.get_prefix(etcd_path("exchanges")).each do |key, value|
        exchange_name = ""
        key.split('/') { |s| exchange_name = URI.decode_www_form(s) } # get last split value without allocation
        json = JSON.parse(value)
        @exchanges[exchange_name] =
          make_exchange(self, exchange_name, json["type"].as_s, true, json["auto_delete"].as_bool, json["internal"].as_bool, json["arguments"].as_h)
      end

      etcd.get_prefix(etcd_path("queue-bindings")).each do |key, value|
        _, _, _, queue_name, exchange_name, routing_key, _ = split_etcd_path(key)
        json = JSON.parse(value)
        x = @exchanges[exchange_name]
        q = @queues[queue_name]
        x.bind(q, routing_key, json["arguments"].to_h)
      end

      etcd.get_prefix(etcd_path("exchange-bindings")).each do |key, value|
        _, _, _, destination, source, routing_key, _ = split_etcd_path(key)
        json = JSON.parse(value)
        src = @exchanges[source]
        dst = @queues[destination]
        src.bind(dst, routing_key, json["arguments"].to_h)
      end
    end

    private def split_etcd_path(path) : Array(String)
      path.split('/').map! { |p| URI.decode_www_form p }
    end

    private def compact!
      @definitions_lock.synchronize do
        @log.info { "Compacting definitions" }
        io = File.open("#{@definitions_file_path}.tmp", "a+")
        SchemaVersion.prefix(io, :definition)
        @exchanges.each_value.select(&.durable?).each do |e|
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable?, e.auto_delete?, e.internal?,
            false, e.arguments)
          io.write_bytes f
        end
        @queues.each_value.select(&.durable?).each do |q|
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable?, q.exclusive?,
            q.auto_delete?, false, q.arguments)
          io.write_bytes f
        end
        @exchanges.each_value.select(&.durable?).each do |e|
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
        @replicator.replace_file @definitions_file_path
        @definitions_file.close
        @definitions_file = io
      end
    end

    private def store_definition(frame, dirty = false)
      @log.debug { "Storing definition: #{frame.inspect}" }
      bytes = frame.to_slice
      @definitions_file.write bytes
      @replicator.append @definitions_file_path, bytes
      if etcd = @replicator.etcd
        store_definition_in_etcd(frame, etcd)
      else
        @definitions_file.fsync
      end
      if dirty
        if (@definitions_deletes += 1) >= Config.instance.max_deleted_definitions
          compact!
          @definitions_deletes = 0
        end
      end
    end

    private def store_definition_in_etcd(f, etcd)
      case f
      when AMQP::Frame::Exchange::Declare
        etcd.put(etcd_path("exchanges", f.exchange_name), {
          type:        f.exchange_type,
          auto_delete: f.auto_delete,
          internal:    f.internal,
          arguments:   f.arguments,
        }.to_json)
      when AMQP::Frame::Exchange::Delete
        etcd.del(etcd_path("exchanges", f.exchange_name))
        etcd.del_prefix(etcd_path("exchange-bindings", f.exchange_name))
      when AMQP::Frame::Exchange::Bind
        args_hash = f.arguments.hash(Crystal::Hasher.new(0, 0)).result
        etcd.put(etcd_path("exchanges", f.destination, "bindings", f.source, f.routing_key, args_hash),
          {arguments: f.arguments}.to_json)
      when AMQP::Frame::Exchange::Unbind
        args_hash = f.arguments.hash(Crystal::Hasher.new(0, 0)).result
        etcd.del(etcd_path("exchanges", f.destination, "bindings", f.source, f.routing_key, args_hash))
      when AMQP::Frame::Queue::Declare
        etcd.put(etcd_path("queues", f.queue_name), {
          arguments: f.arguments,
        }.to_json)
      when AMQP::Frame::Queue::Delete
        etcd.del(etcd_path("queues", f.queue_name))
        etcd.del_prefix(etcd_path("queue-bindings", f.queue_name))
      when AMQP::Frame::Queue::Bind
        args_hash = f.arguments.hash(Crystal::Hasher.new(0, 0)).result
        etcd.put(etcd_path("queues", f.queue_name, "bindings", f.exchange_name, f.routing_key, args_hash),
          {
            arguments: f.arguments,
          }.to_json)
      when AMQP::Frame::Queue::Unbind
        args_hash = f.arguments.hash(Crystal::Hasher.new(0, 0)).result
        etcd.del(etcd_path("queues", f.queue_name, "bindings", f.exchange_name, f.routing_key, args_hash))
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
    end

    private def etcd_path(*args)
      String.build do |str|
        str << Config.instance.clustering_etcd_prefix
        str << "/vhosts/"
        URI.encode_www_form @name, str
        args.each do |a|
          str << "/"
          URI.encode_www_form a.to_s, str
        end
      end
    end

    private def make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        if name.empty?
          DefaultExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        else
          DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        end
      when "fanout"
        FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "x-delayed-message", "x-delayed-exchange"
        arguments = arguments.clone
        type = arguments.delete("x-delayed-type")
        raise Error::ExchangeTypeError.new("Missing required argument 'x-delayed-type'") unless type
        arguments["x-delayed-exchange"] = true
        make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      when "x-federation-upstream"
        FederationExchange.new(vhost, name, arguments)
      when "x-consistent-hash"
        ConsistentHashExchange.new(vhost, name, durable, auto_delete, internal, arguments)
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
      in EventType::ChannelClosed        then @channel_closed_count += 1
      in EventType::ChannelCreated       then @channel_created_count += 1
      in EventType::ConnectionClosed     then @connection_closed_count += 1
      in EventType::ConnectionCreated    then @connection_created_count += 1
      in EventType::QueueDeclared        then @queue_declared_count += 1
      in EventType::QueueDeleted         then @queue_deleted_count += 1
      in EventType::ClientAck            then @ack_count += 1
      in EventType::ClientPublish        then @publish_count += 1
      in EventType::ClientPublishConfirm then @confirm_count += 1
      in EventType::ClientRedeliver      then @redeliver_count += 1
      in EventType::ClientReject         then @reject_count += 1
      in EventType::ConsumerAdded        then @consumer_added_count += 1
      in EventType::ConsumerRemoved      then @consumer_removed_count += 1
      in EventType::ClientGet            then @get_count += 1
      in EventType::ClientDeliver
        @deliver_count += 1
        @deliver_get_count += 1
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
