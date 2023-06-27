require "benchmark"
require "json"
require "../stdlib/*"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/shovel_store"
require "./federation/upstream_store"
require "./sortable_json"
require "./exchange"
require "digest/sha1"
require "./queue"
require "./schema"
require "./event_type"
require "./stats"

module LavinMQ
  class VHost
    include SortableJSON
    include Stats

    rate_stats({"channel_closed", "channel_created", "connection_closed", "connection_created",
                "queue_declared", "queue_deleted", "ack", "deliver", "get", "publish", "confirm",
                "redeliver", "reject", "consumer_added", "consumer_removed"})

    getter name, exchanges, queues, data_dir, operator_policies, policies, parameters, shovels,
      direct_reply_consumers, connections, dir, gc_runs, gc_timing, log, users
    property? flow = true
    getter? closed = false
    property max_connections : Int32?
    property max_queues : Int32?

    @gc_loop = Channel(Nil).new(1)
    @exchanges = Hash(String, Exchange).new
    @queues = Hash(String, Queue).new
    @direct_reply_consumers = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    @connections = Array(Client).new(512)
    @gc_runs = 0
    @gc_timing = Hash(String, Float64).new { |h, k| h[k] = 0 }
    @log : Log
    @definitions_file : File
    @definitions_lock = Mutex.new(:reentrant)

    def initialize(@name : String, @server_data_dir : String, @users : UserStore)
      @log = Log.for "vhost[name=#{@name}]"
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir)
      @definitions_file = File.open(File.join(@data_dir, "definitions.amqp"), "a+")
      @data_dir_fd = LibC.dirfd(Dir.new(@data_dir).@dir)
      File.write(File.join(@data_dir, ".vhost"), @name)
      load_limits
      @operator_policies = ParameterStore(OperatorPolicy).new(@data_dir, "operator_policies.json", @log)
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
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
    def publish(msg : Message, immediate = false,
                visited = Set(Exchange).new, found_queues = Set(Queue).new) : Bool
      ex = @exchanges[msg.exchange_name]? || return false
      ex.publish_in_count += 1
      properties = msg.properties
      headers = properties.headers
      find_all_queues(ex, msg.routing_key, headers, visited, found_queues)
      headers.try(&.delete("BCC"))
      # @log.debug { "publish queues#found=#{found_queues.size}" }
      if found_queues.empty?
        ex.unroutable_count += 1
        return false
      end
      return false if immediate && !found_queues.any? &.immediate_delivery?
      ok = 0
      found_queues.each do |q|
        if q.publish(msg)
          ex.publish_out_count += 1
          ok += 1
        end
      end
      ok > 0
    ensure
      visited.clear
      found_queues.clear
    end

    private def find_all_queues(ex : Exchange, routing_key : String,
                                headers : AMQP::Table?,
                                visited : Set(Exchange),
                                queues : Set(Queue)) : Nil
      ex.queue_matches(routing_key, headers) do |q|
        next if q.closed?
        queues.add? q
      end

      ex.exchange_matches(routing_key, headers) do |e2e|
        visited.add(ex)
        if visited.add?(e2e)
          find_all_queues(e2e, routing_key, headers, visited, queues)
        end
      end

      find_cc_queues(ex, headers, visited, queues)

      if queues.empty? && ex.alternate_exchange
        if ae = @exchanges[ex.alternate_exchange]?
          visited.add(ex)
          if visited.add?(ae)
            find_all_queues(ae, routing_key, headers, visited, queues)
          end
        end
      end
    end

    private def find_cc_queues(ex, headers, visited, queues)
      if cc = headers.try(&.fetch("CC", nil))
        if cc = cc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          cc.each do |cc_rk|
            if cc_rk = cc_rk.as?(String)
              find_all_queues(ex, cc_rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("CC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("CC header not a string array")
        end
      end

      if bcc = headers.try(&.fetch("BCC", nil))
        if bcc = bcc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          hdrs.delete "BCC"
          bcc.each do |bcc_rk|
            if bcc_rk = bcc_rk.as?(String)
              find_all_queues(ex, bcc_rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("BCC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("BCC header not a string array")
        end
      end
    end

    def details_tuple
      {
        name:          @name,
        dir:           @dir,
        tracing:       false,
        cluster_state: NamedTuple.new,
      }
    end

    def message_details
      ready = unacked = 0_u64
      ack = confirm = deliver = get = get_no_ack = publish = redeliver = return_unroutable = 0_u64
      @queues.each_value do |q|
        ready += q.message_count
        unacked += q.unacked_count
        ack += q.ack_count
        confirm += q.confirm_count
        deliver += q.deliver_count
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
          publish:           publish,
          redeliver:         redeliver,
          return_unroutable: return_unroutable,
        },
      }
    end

    def declare_queue(name, durable, auto_delete, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, durable, false,
        auto_delete, false, arguments)
      @log.info { "queue=#{name} (durable: #{durable}, auto_delete=#{auto_delete}, arguments: #{arguments}) Created" }
    end

    def delete_queue(name)
      apply AMQP::Frame::Queue::Delete.new(0_u16, 0_u16, name, false, false, false)
    end

    def declare_exchange(name, type, durable, auto_delete, internal = false,
                         arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, name, type, false, durable,
        auto_delete, internal, false, arguments)
      @log.info { "exchange=#{name} Created" }
    end

    def delete_exchange(name)
      apply AMQP::Frame::Exchange::Delete.new(0_u16, 0_u16, name, false, false)
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
            make_exchange(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments.to_h)
          apply_policies([e] of Exchange) unless loading
          store_definition(f) if !loading && f.durable
        when AMQP::Frame::Exchange::Delete
          if x = @exchanges.delete f.exchange_name
            @exchanges.each_value do |ex|
              ex.exchange_bindings.each do |binding_args, destinations|
                ex.unbind(x, *binding_args) if destinations.includes?(x)
              end
            end
            x.delete
            store_definition(f) if !loading && x.durable
          else
            return false
          end
        when AMQP::Frame::Exchange::Bind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          src.bind(dst, f.routing_key, f.arguments.to_h)
          store_definition(f) if !loading && src.durable && dst.durable
        when AMQP::Frame::Exchange::Unbind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          src.unbind(dst, f.routing_key, f.arguments.to_h)
          store_definition(f) if !loading && src.durable && dst.durable
        when AMQP::Frame::Queue::Declare
          return false if @queues.has_key? f.queue_name
          q = @queues[f.queue_name] = QueueFactory.make(self, f)
          apply_policies([q] of Queue) unless loading
          store_definition(f) if !loading && f.durable && !f.exclusive
          event_tick(EventType::QueueDeclared) unless loading
        when AMQP::Frame::Queue::Delete
          if q = @queues.delete(f.queue_name)
            @exchanges.each_value do |ex|
              ex.queue_bindings.each do |binding_args, destinations|
                ex.unbind(q, *binding_args) if destinations.includes?(q)
              end
            end
            store_definition(f) if !loading && q.durable && !q.exclusive
            event_tick(EventType::QueueDeleted) unless loading
            q.delete
          else
            return false
          end
        when AMQP::Frame::Queue::Bind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          x.bind(q, f.routing_key, f.arguments.to_h)
          store_definition(f) if !loading && x.durable && q.durable && !q.exclusive
        when AMQP::Frame::Queue::Unbind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          x.unbind(q, f.routing_key, f.arguments.to_h)
          store_definition(f) if !loading && x.durable && q.durable && !q.exclusive
        else raise "Cannot apply frame #{f.class} in vhost #{@name}"
        end
        true
      end
    end

    alias QueueBinding = Tuple(BindingKey, Exchange)

    def queue_bindings(queue : Queue)
      iterators = @exchanges.each_value.map do |ex|
        ex.queue_bindings.each.select do |(_binding_args, destinations)|
          destinations.includes?(queue)
        end.map { |(binding_args, _destinations)| {ex, binding_args} }
      end
      Iterator(QueueBinding).chain(iterators)
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

    def close(reason = "Broker shutdown")
      @closed = true
      stop_shovels
      stop_upstream_links
      Fiber.yield
      @log.debug { "Closing connections" }
      @connections.each &.close(reason)
      # wait up to 10s for clients to gracefully close
      100.times do
        break if @connections.empty?
        sleep 0.1
      end
      # then force close the remaining (close tcp socket)
      @connections.each &.force_close
      Fiber.yield # yield so that Client read_loops can shutdown
      @queues.each_value &.close
      Fiber.yield
      compact!
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
        sleep 0.01
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
        compact!
      else
        @definitions_lock.synchronize do
          SchemaVersion.verify(io, :definition)
          loop do
            AMQP::Frame.from_io(io, IO::ByteFormat::SystemEndian) do |f|
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
              else raise "Cannot apply frame #{f.class} in vhost #{@name}"
              end
            end # Frame.from_io
          rescue ex : IO::EOFError
            break
          end # loop
        end   # synchronize
      end     # if

      # apply definitions
      exchanges.each_value { |f| apply f, loading: true }
      queues.each_value { |f| apply f, loading: true }
      exchange_bindings.each_value { |fs| fs.each { |f| apply f, loading: true } }
      queue_bindings.each_value { |fs| fs.each { |f| apply f, loading: true } }

      compact! if should_compact
    end

    private def load_default_definitions
      @log.info { "Loading default definitions" }
      @exchanges[""] = DefaultExchange.new(self, "", true, false, false)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", true, false, false)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", true, false, false)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", true, false, false)
      @exchanges["amq.headers"] = HeadersExchange.new(self, "amq.headers", true, false, false)
      @exchanges["amq.match"] = HeadersExchange.new(self, "amq.match", true, false, false)
    end

    private def compact!(include_transient = false)
      @definitions_lock.synchronize do
        @log.info { "Compacting definitions" }
        tmp_path = File.join(@data_dir, "definitions.amqp.tmp")
        io = File.open(tmp_path, "a+")
        SchemaVersion.prefix(io, :definition)
        @exchanges.each_value do |e|
          next if !include_transient && !e.durable
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable, e.auto_delete, e.internal,
            false, AMQP::Table.new(e.arguments))
          io.write_bytes f
        end
        @queues.each_value do |q|
          next if !include_transient && !q.durable
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
            q.auto_delete, false, AMQP::Table.new(q.arguments))
          io.write_bytes f
        end
        @exchanges.each_value do |e|
          next if !include_transient && !e.durable
          e.queue_bindings.each do |bt, queues|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            queues.each do |q|
              f = AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, q.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
          e.exchange_bindings.each do |bt, exchanges|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            exchanges.each do |ex|
              f = AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, ex.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
        end
        io.fsync
        File.rename tmp_path, File.join(@data_dir, "definitions.amqp")
        @definitions_file.close
        @definitions_file = io
      end
    end

    private def store_definition(frame)
      @log.debug { "Storing definition: #{frame.inspect}" }
      @definitions_file.write_bytes frame
      @definitions_file.fsync
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
      when "x-delayed-message"
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

    def purge_queues_and_close_consumers(backup_data : Bool, suffix : String)
      if backup_data
        backup_dir = File.join(@server_data_dir, "#{@dir}_#{suffix}")
        @log.info { "vhost=#{@name} reset backup=#{backup_dir}" }
        FileUtils.cp_r data_dir, backup_dir
      end
      @queues.each_value do |queue|
        purged_msgs = queue.purge_and_close_consumers
        @log.info { "vhost=#{@name} queue=#{queue.name} action=purge_and_close_consumers " \
                    "purged_messages=#{purged_msgs}" }
      end
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
      in EventType::ClientDeliver        then @deliver_count += 1
      in EventType::ClientGet            then @get_count += 1
      in EventType::ClientPublish        then @publish_count += 1
      in EventType::ClientPublishConfirm then @confirm_count += 1
      in EventType::ClientRedeliver      then @redeliver_count += 1
      in EventType::ClientReject         then @reject_count += 1
      in EventType::ConsumerAdded        then @consumer_added_count += 1
      in EventType::ConsumerRemoved      then @consumer_removed_count += 1
      end
    end

    def sync
      {% if flag?(:linux) %}
        ret = LibC.syncfs(@data_dir_fd)
        raise IO::Error.from_errno("syncfs") if ret != 0
      {% else %}
        LibC.sync
      {% end %}
    end
  end
end
