require "json"
require "../stdlib/*"
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
require "./connection_store"
require "./direct_reply_consumer_store"
require "./definitions_store"
require "./amqp/channel"

module LavinMQ
  class VHost
    include SortableJSON
    include Stats

    rate_stats({"channel_closed", "channel_created", "connection_closed", "connection_created",
                "queue_declared", "queue_deleted", "ack", "deliver", "deliver_no_ack", "deliver_get", "get", "get_no_ack", "publish", "confirm",
                "redeliver", "reject", "consumer_added", "consumer_removed", "recv_oct", "send_oct"})

    getter name, data_dir, operator_policies, policies, parameters, shovels, dir, users
    getter closed = BoolChannel.new(true)
    property max_connections : Int32?
    property max_queues : Int32?

    @flow = true
    @direct_reply_consumers = DirectReplyConsumerStore.new
    @definitions : DefinitionsStore?
    @shovels : Shovel::Store?
    @upstreams : Federation::UpstreamStore?
    @connections = ConnectionStore.new
    @pending_acks = Hash(AMQP::Channel, UInt64).new
    @pending_acks_lock = Mutex.new(:unchecked)
    @confirm_requested = ::Channel(Nil).new(1)

    # Bool accessors (later become Atomic)
    def flow? : Bool
      @flow
    end

    def flow=(active : Bool)
      @flow = active
    end

    def closed? : Bool
      @closed.value
    end

    # Exchange accessors

    def exchange?(name : String) : Exchange?
      definitions.exchange?(name)
    end

    def exchange(name : String) : Exchange
      definitions.exchange(name)
    end

    def exchange_exists?(name : String) : Bool
      definitions.exchange_exists?(name)
    end

    def each_exchange(& : Exchange ->) : Nil
      definitions.each_exchange { |v| yield v }
    end

    def exchanges : Array(Exchange)
      definitions.exchanges
    end

    def exchanges_size : Int32
      definitions.exchanges_size
    end

    def exchanges_any?(& : {String, Exchange} -> Bool) : Bool
      definitions.exchanges_any? { |kv| yield kv }
    end

    def register_exchange(exchange : Exchange) : Nil
      definitions.register_exchange(exchange)
    end

    # Queue accessors

    def queue?(name : String) : Queue?
      definitions.queue?(name)
    end

    def queue(name : String) : Queue
      definitions.queue(name)
    end

    def queue_exists?(name : String) : Bool
      definitions.queue_exists?(name)
    end

    def each_queue(& : Queue ->) : Nil
      definitions.each_queue { |v| yield v }
    end

    def queues : Array(Queue)
      definitions.queues
    end

    def queues_size : Int32
      definitions.queues_size
    end

    def register_queue(queue : Queue) : Nil
      definitions.register_queue(queue)
    end

    def queues_clear : Nil
      definitions.queues_clear
    end

    # Connection accessors

    def each_connection(& : Client ->) : Nil
      @connections.each { |c| yield c }
    end

    def connections : Array(Client)
      @connections.to_a
    end

    def connections_size : Int32
      @connections.size
    end

    def add_connection(client : Client)
      event_tick(EventType::ConnectionCreated)
      @connections.add client
    end

    def rm_connection(client : Client)
      event_tick(EventType::ConnectionClosed)
      @connections.delete client
    end

    # Direct reply consumer accessors

    def direct_reply_consumer?(consumer_tag : String) : Client::Channel?
      @direct_reply_consumers[consumer_tag]?
    end

    def direct_reply_consumer_set(consumer_tag : String, channel : Client::Channel) : Nil
      @direct_reply_consumers[consumer_tag] = channel
    end

    def direct_reply_consumer_delete(consumer_tag : String) : Client::Channel?
      @direct_reply_consumers.delete(consumer_tag)
    end

    def direct_reply_consumer_has_key?(consumer_tag : String) : Bool
      @direct_reply_consumers.has_key?(consumer_tag)
    end

    Log = LavinMQ::Log.for "vhost"

    def initialize(@name : String, @server_data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?, @description = "", @tags = Array(String).new(0))
      @log = Logger.new(Log, vhost: @name)
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir)
      FileUtils.rm_rf File.join(@data_dir, "transient")
      File.write(File.join(@data_dir, ".vhost"), @name)
      load_limits
      @operator_policies = ParameterStore(OperatorPolicy).new(@data_dir, "operator_policies.json", @replicator, vhost: @name)
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @replicator, vhost: @name)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator, vhost: @name)
      @shovels = Shovel::Store.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      @definitions = DefinitionsStore.new(self, @data_dir, @replicator, @log)
      load!
      spawn check_consumer_timeouts_loop, name: "Consumer timeouts loop"
      spawn publish_confirm_loop, name: "Publish confirm loop"
    end

    private def check_consumer_timeouts_loop
      loop do
        select
        when timeout Config.instance.consumer_timeout_loop_interval.seconds
        when closed.when_true.receive?
          return
        end
        each_connection do |c|
          c.each_channel do |ch|
            ch.check_consumer_timeout
          end
        end
      end
    end

    def enqueue_ack(channel : AMQP::Channel, msgid : UInt64)
      @pending_acks_lock.synchronize do
        @pending_acks[channel] = msgid
      end
      @confirm_requested.try_send nil
    rescue ::Channel::ClosedError
    end

    private def publish_confirm_loop
      loop do
        @confirm_requested.receive

        # Activity detected, start batching
        deadline = Time.instant + Config.instance.publish_confirm_interval.milliseconds
        idle_timeout_interval = Config.instance.publish_confirm_idle_timeout.milliseconds
        loop do
          remaining = deadline - Time.instant
          break if remaining <= Time::Span::ZERO
          idle_timeout = remaining < idle_timeout_interval ? remaining : idle_timeout_interval
          select
          when @confirm_requested.receive
            # Keep batching as long as new publishes arrive
          when timeout idle_timeout
            break
          end
        end

        drain_pending_acks
      rescue ::Channel::ClosedError
        # @confirm_requested is closed; flush anything that was persisted
        # but not yet confirmed before exiting.
        drain_pending_acks
        return
      end
    end

    private def drain_pending_acks
      acks = @pending_acks_lock.synchronize do
        if @pending_acks.empty?
          nil
        else
          current = @pending_acks
          @pending_acks = Hash(AMQP::Channel, UInt64).new
          current
        end
      end
      return unless acks

      begin
        sync
      rescue ex
        @log.error(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end

      acks.each do |channel, msgid|
        channel.do_confirm_ack(msgid, multiple: true)
      rescue ex
        # If the channel is closed before we can ack, just ignore it
        @log.debug { "Failed to ack message on channel #{channel}: #{ex.message}" }
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
      ex = exchange?(msg.exchange_name) || return false
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
      each_queue do |q|
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

    def apply(f, loading = false) : Bool
      definitions.apply(f, loading)
    end

    def queue_bindings(queue : Queue) : Array(BindingDetails)
      definitions.queue_bindings(queue)
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
      return if @closed.swap(true)
      @confirm_requested.close
      stop_shovels
      stop_upstream_links

      @log.info { "Closing connections" }
      close_done = Channel(Nil).new

      spawn do
        @connections.close_all(reason, @log)
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
      each_connection &.force_close
      Fiber.yield # yield so that Client read_loops can shutdown
      each_queue &.close
      each_exchange &.close
      Fiber.yield
      definitions.close
      FileUtils.rm_rf File.join(@data_dir, "transient")
    end

    def delete
      close(reason: "VHost deleted")
      Fiber.yield
      FileUtils.rm_rf @data_dir
    end

    def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      resources ||= (queues.map(&.as(Queue | Exchange)) + exchanges.map(&.as(Queue | Exchange)))
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
      definitions.load!
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
      closed.set(false)
      Fiber.yield
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

    def add_recv_bytes(bytes : UInt64) : Nil
      @recv_oct_count.add(bytes, :relaxed)
    end

    def add_send_bytes(bytes : UInt64) : Nil
      @send_oct_count.add(bytes, :relaxed)
    end

    def sync : Nil
      definitions.sync
    end

    private def definitions : DefinitionsStore
      @definitions.not_nil!
    end
  end
end
