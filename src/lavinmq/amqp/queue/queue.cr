require "../../filesystem"
require "digest/sha1"
require "sync/exclusive"
require "../../logger"
require "../../segment_position"
require "../../policy"
require "../../sortable_json"
require "../../client/channel/consumer"
require "../../message"
require "../../error"
require "./state"
require "../../message_store"
require "../../unacked_message"
require "../../deduplication"
require "../../bool_channel"
require "../argument_target"
require "../argument"
require "../argument/dead_lettering"
require "../../queue_stats"

module LavinMQ::AMQP
  class Queue
    include PolicyTarget
    include SortableJSON
    include QueueStats

    include ArgumentTarget
    include Argument::DeadLettering

    VALIDATOR_INT_ZERO  = ArgumentValidator::IntValidator.new(min_value: 0)
    VALIDATOR_INT_ONE   = ArgumentValidator::IntValidator.new(min_value: 1)
    VALIDATOR_INT32_ONE = ArgumentValidator::IntValidator.new(min_value: 1, max_value: Int32::MAX)
    VALIDATOR_STRING    = ArgumentValidator::StringValidator.new
    VALIDATOR_BOOL      = ArgumentValidator::BoolValidator.new

    DEFAULT_DELAYED_RETRY_DELIVERY_LIMIT = 20_i64
    DELAYED_RETRY_MAX_DELAY_MS           = UInt32::MAX.to_i64
    DELAYED_RETRY_DRAIN_REDELAY_MS       = 1000_i64

    add_argument_validator "x-expires", VALIDATOR_INT_ONE
    add_argument_validator "x-max-length", VALIDATOR_INT_ZERO
    add_argument_validator "x-max-length-bytes", VALIDATOR_INT_ZERO
    add_argument_validator "x-message-ttl", VALIDATOR_INT_ZERO
    add_argument_validator "x-overflow", VALIDATOR_STRING
    add_argument_validator "x-delivery-limit", VALIDATOR_INT_ZERO
    add_argument_validator "x-consumer-timeout", VALIDATOR_INT_ZERO
    add_argument_validator "x-single-active-consumer", VALIDATOR_BOOL
    add_argument_validator "x-message-deduplication", VALIDATOR_BOOL
    add_argument_validator "x-cache-size", VALIDATOR_INT_ZERO
    add_argument_validator "x-cache-ttl", VALIDATOR_INT_ZERO
    add_argument_validator "x-deduplication-header", VALIDATOR_STRING
    add_argument_validator "x-delayed-retry-min", VALIDATOR_INT_ONE
    add_argument_validator "x-delayed-retry-max", VALIDATOR_INT_ONE
    add_argument_validator "x-delayed-retry-multiplier", VALIDATOR_INT32_ONE

    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    # Settings derived from the queue's arguments and its policies. A new
    # instance is built off to the side every time they are (re)applied and
    # published with a single reference store, so readers never see a policy
    # half-applied, limits missing while a policy is re-applied, or a torn
    # value. A published instance is never mutated.
    class Settings
      property max_length : Int64?
      property max_length_bytes : Int64?
      property message_ttl : Int64?
      property expires : Int64?
      property delivery_limit : Int64?
      property? reject_on_overflow = false
      property consumer_timeout : UInt64?
      property dlx : String?
      property dlrk : String?
      property max_age : (Time::Span | Time::MonthSpan)?
      property delayed_retry_min : Int64?
      property delayed_retry_max : Int64?
      property delayed_retry_multiplier : Int32?
      # Set when the policy tightened limits that existing messages must be
      # checked against
      property? enforce_limits = false
      getter effective_args = Array(String).new

      def initialize(@consumer_timeout = nil)
      end

      # Deliveries are counted for x-delivery-limit and x-delayed-retry-min
      def counts_deliveries? : Bool
        !!(@delivery_limit || @delayed_retry_min)
      end
    end

    # Published with release and read with acquire ordering, so that a reader
    # on another thread sees the fields as they were when it was published
    @settings = Atomic(Settings).new(Settings.new(Config.instance.consumer_timeout))
    # Built by clear_policy_arguments/apply_policy_argument, published by
    # commit_policy_arguments
    @staged_settings = Settings.new
    @delayed_retry_queue : DelayedRetryQueue?
    @logged_retry_policy_conflict : String?
    @retry_queue_lock = Mutex.new
    @exclusive_consumer = false
    @deliveries = Hash(SegmentPosition, Int32).new
    # Mutated, and decided on (exclusivity, auto-delete), under @consumers_lock
    @consumers = Array(Client::Channel::Consumer).new
    @consumers_lock = Mutex.new
    # @consumers.size, readable without the lock (e.g. on the delivery path)
    @consumer_count = Atomic(Int32).new(0)
    # Set under @consumers_lock when the queue is going away (closed,
    # auto-deleted or expired), so no consumer can be added after that decision
    @consumers_closed = false
    @message_ttl_change = ::Channel(Nil).new

    # Leaf lock: basic.get, acks/closes of any channel and stats all touch
    # the list, and Channel#close holds its @unack_lock meanwhile. The Deque
    # is only reachable through the lock.
    @basic_get_unacked = Sync::Exclusive(Deque(UnackedMessage)).new(Deque(UnackedMessage).new, :unchecked)

    # Consumer accessors

    def consumers : Array(Client::Channel::Consumer)
      @consumers_lock.synchronize { @consumers.dup }
    end

    def consumers_size : Int32
      @consumer_count.get(:relaxed)
    end

    def consumers_empty? : Bool
      consumers_size.zero?
    end

    # BasicGet unacked accessors

    def basic_get_unacked_push(msg : UnackedMessage) : Nil
      @basic_get_unacked.lock(&.push(msg))
    end

    def basic_get_unacked_reject!(& : UnackedMessage -> Bool) : Nil
      @basic_get_unacked.lock(&.reject! { |u| yield u })
    end

    def basic_get_unacked_size : Int32
      @basic_get_unacked.lock(&.size)
    end

    @msg_store_lock = Mutex.new(:reentrant)
    @msg_store : MessageStore
    getter deliver_loop_wg = WaitGroup.new

    getter paused = BoolChannel.new(false)

    def settings : Settings
      @settings.get(:acquire)
    end

    def consumer_timeout : UInt64?
      settings.consumer_timeout
    end

    getter consumers_empty = BoolChannel.new(true)
    @queue_expiration_ttl_change = ::Channel(Nil).new

    # Idle fiber management
    @message_expire_fiber_active = Atomic(Bool).new(false)
    @queue_expire_fiber_active = Atomic(Bool).new(false)
    @policy_limits_fiber_active = Atomic(Bool).new(false)
    @policy_limits_pending = Atomic(Bool).new(false)

    def message_expire_fiber_active?
      @message_expire_fiber_active.get(:relaxed)
    end

    private EXPIRE_FIBER_IDLE_THRESHOLD = 30.seconds

    # The expire loop wakes on deadlines rounded up to this many ms, so
    # messages published close together expire in one batch instead of
    # one wakeup per millisecond. Messages are never expired early.
    EXPIRE_WAKEUP_GRANULARITY_MS = 10_i64

    # Time until the expire loop should wake up for a message expiring at
    # *expire_at* (unix ms)
    def self.time_to_expiration_wakeup(expire_at : Int64) : Time::Span
      now = RoughTime.unix_ms
      return Time::Span.zero if expire_at <= now
      g = EXPIRE_WAKEUP_GRANULARITY_MS
      wake_at = (expire_at + g - 1) // g * g
      (wake_at - now).milliseconds
    end

    getter? internal = false

    private def queue_expire_loop
      @vhost.closed.when_false.receive?
      loop do
        break if @closed || !settings.expires
        select
        when @consumers_empty.when_true.receive
        when @queue_expiration_ttl_change.receive
          next
        end
        break unless ttl = settings.expires
        @log.debug { "Queue expires in #{ttl}ms" }
        select
        when @queue_expiration_ttl_change.receive
        when @consumers_empty.when_false.receive
        when timeout ttl.milliseconds
          # A consumer added as the TTL fired keeps the queue alive
          next unless expire_queue
          close
          break
        end
      end
    rescue ::Channel::ClosedError
    end

    private def message_expire_loop
      @vhost.closed.when_false.receive?
      loop do
        break unless wait_for_consumers_empty
        break unless wait_for_messages
        next unless consumers_empty?
        break unless wait_for_message_expiration
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    rescue ::Channel::ClosedError
    ensure
      @message_expire_fiber_active.set(false, :release)
      ensure_expire_fiber # restart if msg arrived during teardown
    end

    # The wait_for_* helpers below return false once the fiber has been idle
    # long enough to stop, and true when the loop should keep going.

    private def wait_for_consumers_empty : Bool
      select
      when @consumers_empty.when_true.receive
        @log.debug { "Consumers empty" }
        true
      when timeout EXPIRE_FIBER_IDLE_THRESHOLD
        @log.debug { "Idle timeout while waiting for consumers empty, stopping fiber" }
        false
      end
    end

    private def wait_for_messages : Bool
      select
      when @msg_store.empty.when_false.receive
        @log.debug { "Message store not empty" }
        true
      when timeout EXPIRE_FIBER_IDLE_THRESHOLD
        @log.debug { "Idle timeout while waiting for messages, stopping fiber" }
        false
      end
    end

    private def wait_for_message_expiration : Bool
      if ttl = time_to_message_expiration
        @log.debug { "Next message TTL: #{ttl}" }
        select
        when @message_ttl_change.receive
          @log.debug { "Message TTL changed" }
        when @msg_store.empty.when_true.receive # might be empty now (from basic get)
          @log.debug { "Message store is empty" }
        when @consumers_empty.when_false.receive
          @log.debug { "Got consumers" }
        when timeout ttl
          @log.debug { "Message TTL reached" }
          expire_messages
        end
        true
      else
        # first message in queue should not be expired
        # wait for empty queue or TTL change
        select
        when @message_ttl_change.receive
          @log.debug { "Message TTL changed" }
          true
        when @msg_store.empty.when_true.receive
          @log.debug { "Msg store is empty" }
          true
        when timeout EXPIRE_FIBER_IDLE_THRESHOLD
          @log.debug { "Idle timeout while no messages need expiring, stopping fiber" }
          false
        end
      end
    end

    getter name, arguments, vhost
    getter? auto_delete, exclusive
    # The connection that declared this exclusive queue, told when the queue is deleted
    property exclusive_owner : AMQP::Client?
    getter? closed = false
    getter state = QueueState::Running
    getter empty : BoolChannel
    getter single_active_consumer : Client::Channel::Consumer? = nil
    getter single_active_consumer_change = ::Channel(Client::Channel::Consumer).new
    @single_active_consumer_queue = false
    @data_dir : String
    Log = LavinMQ::Log.for "queue"
    @metadata : ::Log::Metadata
    @deduper : Deduplication::Deduper?

    protected def initialize(@vhost : VHost, @name : String,
                             @exclusive : Bool = false, @auto_delete : Bool = false,
                             @arguments : AMQP::Table = AMQP::Table.new)
      @data_dir = make_data_dir
      @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
      @log = Logger.new(Log, @metadata)
      dotqueue_file = File.join(@data_dir, ".queue")
      fresh = !File.exists?(dotqueue_file)
      File.open(dotqueue_file, "w") { |f| f.sync = true; f.print @name }
      if durable?
        @vhost.replicator.try &.replace_file(dotqueue_file)
      end
      @msg_store = init_msg_store(@data_dir)
      @empty = @msg_store.empty
      @dead_letter = Argument::DeadLettering::DeadLetterer.new(@vhost, @name, @log)
      begin
        start
      rescue ex
        fresh ? delete : close
        raise ex
      end
    end

    private def start : Bool
      if @msg_store.closed
        !close
      else
        if File.exists?(File.join(@data_dir, ".paused")) # Migrate '.paused' files to 'paused'
          FileSystem.durable_rename(File.join(@data_dir, ".paused"), File.join(@data_dir, "paused"))
        end
        if File.exists?(File.join(@data_dir, "paused"))
          @state = QueueState::Paused
          @paused.set(true)
        end
        handle_arguments
        recover_retry_queue
        configure_delayed_retry unless awaiting_policy?
        ensure_queue_expire_fiber
        start_message_expire_loop if should_start_expire_fiber?
        true
      end
    end

    def restart! : Bool
      return false unless @closed
      reset_queue_state
      @msg_store = init_msg_store(@data_dir)
      @empty = @msg_store.empty
      start.tap do |started|
        reapply_policy if started
      end
    end

    private def start_message_expire_loop
      return if @message_expire_fiber_active.swap(true)
      @log.debug { "Starting message expire loop" }
      spawn message_expire_loop, name: "Queue#message_expire_loop #{@vhost.name}/#{@name}"
    end

    private def ensure_queue_expire_fiber
      return if @closed || !settings.expires
      return if @queue_expire_fiber_active.swap(true)
      spawn(name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}") do
        queue_expire_loop
      rescue ex
        @log.error(ex) { "queue_expire_loop failed" }
      ensure
        @queue_expire_fiber_active.set(false, :release)
        ensure_queue_expire_fiber
      end
    end

    private def schedule_policy_limits
      @policy_limits_pending.set(true, :release)
      ensure_policy_limits_fiber
    end

    private def ensure_policy_limits_fiber
      return if @closed || !@policy_limits_pending.get(:acquire)
      return if @policy_limits_fiber_active.swap(true)
      spawn apply_policy_limits, name: "Queue#apply_policy_limits #{@vhost.name}/#{@name}"
    end

    private def apply_policy_limits
      @vhost.closed.when_false.receive?
      while !@closed && @policy_limits_pending.swap(false)
        @msg_store_lock.synchronize do
          break if @closed
          # Read the current limits after acquiring the lock. Policy churn while
          # this pass yields requests another pass, without spawning more fibers.
          # A failed operation must not skip the other limit check or discard
          # another policy update that arrived during this pass.
          begin
            drop_overflow
          rescue ex
            @log.error(ex) { "drop_overflow failed" }
          end
          begin
            drop_redelivered
          rescue ex
            @log.error(ex) { "drop_redelivered failed" }
          end
        end
      end
    rescue ::Channel::ClosedError
    ensure
      @policy_limits_fiber_active.set(false, :release)
      ensure_policy_limits_fiber
    end

    # Ensure the expire fiber is running if there are messages that need expiring
    private def ensure_expire_fiber
      if !@closed && !@message_expire_fiber_active.get(:acquire)
        start_message_expire_loop if should_start_expire_fiber?
      end
    end

    # Check if we need the expire fiber running
    private def should_start_expire_fiber? : Bool
      return false if @msg_store.size == 0 # No messages to expire
      return false unless consumers_empty? # Expire loop can't run with consumers present; rm_consumer will restart it
      return true if settings.message_ttl  # Queue-level TTL means all messages need expiring

      # Check if first message has TTL (including expiration: "0" for immediate expiry)
      @msg_store_lock.synchronize do
        @msg_store.first?.try { |env| !env.message.ttl.nil? } || false
      end
    end

    private def reset_queue_state
      @closed = false
      @consumers_closed = false
      @state = QueueState::Running
      @message_expire_fiber_active.set(false, :release)

      # Recreate channels that were closed
      @queue_expiration_ttl_change = ::Channel(Nil).new
      @message_ttl_change = ::Channel(Nil).new
      @paused = BoolChannel.new(false)
      @consumers_empty = BoolChannel.new(true)
      @single_active_consumer_change = ::Channel(Client::Channel::Consumer).new
      @deliver_loop_wg = WaitGroup.new
      reset_queue_stats
    end

    # own method so that it can be overriden in other queue implementations
    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.replicator : nil
      MessageStore.new(data_dir, replicator, durable?, metadata: @metadata, persister: @vhost.persister)
    end

    private def make_data_dir : String
      data_dir = if durable?
                   File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
                 else
                   File.join(@vhost.data_dir, "transient", Digest::SHA1.hexdigest @name)
                 end
      if Dir.exists? data_dir
        # delete left over files from transient queues
        unless durable?
          FileUtils.rm_r data_dir
          Dir.mkdir_p data_dir
        end
      elsif durable?
        FileSystem.mkdir_p data_dir
      else
        Dir.mkdir_p data_dir
      end
      data_dir
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << " @vhost=" << @vhost.name << ">"
    end

    def self.generate_name
      "amq.gen-#{Random::Secure.urlsafe_base64(24)}"
    end

    def bindings
      @vhost.queue_bindings(self)
    end

    def redeclare
      @queue_expiration_ttl_change.try_send? nil
    end

    def has_exclusive_consumer?
      @exclusive_consumer
    end

    # A queue with an exclusive consumer refuses all other consumers, and an
    # exclusive consumer is refused while the queue has any consumers
    private def in_exclusive_use?(new_consumer_exclusive : Bool) : Bool
      @exclusive_consumer || (new_consumer_exclusive && !@consumers.empty?)
    end

    private def apply_policy_argument(key : String, value : JSON::Any) : Bool # ameba:disable Metrics/CyclomaticComplexity
      @log.debug { "Applying policy #{key}: #{value}" }
      settings = @staged_settings
      case key
      when "max-length"
        unless settings.max_length.try &.< value.as_i64
          settings.max_length = value.as_i64
          settings.effective_args.delete("x-max-length")
          settings.enforce_limits = true
          return true
        end
      when "max-length-bytes"
        unless settings.max_length_bytes.try &.< value.as_i64
          settings.max_length_bytes = value.as_i64
          settings.effective_args.delete("x-max-length-bytes")
          settings.enforce_limits = true
          return true
        end
      when "message-ttl"
        unless settings.message_ttl.try &.< value.as_i64
          settings.message_ttl = value.as_i64
          settings.effective_args.delete("x-message-ttl")
          return true
        end
      when "expires"
        unless settings.expires.try &.< value.as_i64
          settings.expires = value.as_i64
          settings.effective_args.delete("x-expires")
          return true
        end
      when "overflow"
        overflow = value.as_s
        if overflow.in?("reject-publish", "drop-head")
          settings.reject_on_overflow = overflow == "reject-publish"
          settings.effective_args.delete("x-overflow")
          return true
        end
      when "dead-letter-exchange"
        if settings.dlx.nil?
          settings.dlx = value.as_s
          settings.effective_args.delete("x-dead-letter-exchange")
          return true
        end
      when "dead-letter-routing-key"
        if settings.dlrk.nil?
          settings.dlrk = value.as_s
          settings.effective_args.delete("x-dead-letter-routing-key")
          return true
        end
      when "delivery-limit"
        unless settings.delivery_limit.try &.< value.as_i64
          settings.delivery_limit = value.as_i64
          settings.effective_args.delete("x-delivery-limit")
          settings.enforce_limits = true
          return true
        end
      when "delayed-retry-min", "delayed-retry-multiplier", "delayed-retry-max"
        return apply_delayed_retry_policy(key, value.as_i64)
      when "federation-upstream"
        @vhost.upstreams.try &.link(value.as_s, self)
        return true
      when "federation-upstream-set"
        @vhost.upstreams.try &.link_set(value.as_s, self)
        return true
      when "consumer-timeout"
        unless settings.consumer_timeout.try &.< value.as_i64
          settings.consumer_timeout = value.as_i64.to_u64
          settings.effective_args.delete("x-consumer-timeout")
          return true
        end
      end
      false
    end

    private def apply_delayed_retry_policy(key : String, value : Int64) : Bool
      if reason = delayed_retry_policy_conflict
        if key == "delayed-retry-min" && @logged_retry_policy_conflict != reason
          @logged_retry_policy_conflict = reason
          @log.warn { "Policy #{key} ignored reason=#{reason}" }
        end
        return false
      end
      if value < 1
        @log.warn { "Policy #{key} ignored reason=value_below_1 value=#{value}" }
        return false
      end
      settings = @staged_settings
      case key
      when "delayed-retry-min"
        return false if settings.delayed_retry_min.try &.< value
        settings.delayed_retry_min = value
      when "delayed-retry-multiplier"
        if value > Int32::MAX
          @log.warn { "Policy #{key} ignored reason=value_too_large value=#{value}" }
          return false
        end
        return false if settings.delayed_retry_multiplier.try &.< value
        settings.delayed_retry_multiplier = value.to_i32
      when "delayed-retry-max"
        return false if settings.delayed_retry_max.try &.< value
        settings.delayed_retry_max = value
      end
      settings.effective_args.delete("x-#{key}")
      true
    end

    private def delayed_retry_policy_conflict : String?
      if @deduper
        "message_deduplication"
      elsif "amq.retry-#{@name}".bytesize > DelayedQueue::MAX_NAME_LENGTH
        "retry_queue_name_too_long"
      end
    end

    private def clear_policy_arguments
      @staged_settings = settings_from_arguments
      @vhost.upstreams.try &.stop_link(self)
    end

    private def handle_arguments
      @policy_lock.synchronize do
        @staged_settings = settings_from_arguments
        commit_policy_arguments
      end
    end

    # Publish the staged settings, then act on them
    private def commit_policy_arguments
      settings = @staged_settings
      resolve_delayed_retry(settings)
      @dead_letter.set_target(settings.dlx, settings.dlrk)
      @settings.set(settings, :release)
      @queue_expiration_ttl_change.try_send? nil
      ensure_queue_expire_fiber
      @message_ttl_change.try_send? nil
      ensure_expire_fiber if settings.message_ttl
      schedule_policy_limits if settings.enforce_limits?
    end

    private def settings_from_arguments : Settings # ameba:disable Metrics/CyclomaticComplexity
      settings = Settings.new
      effective_args = settings.effective_args
      settings.dlx = parse_header("x-dead-letter-exchange", String)
      effective_args << "x-dead-letter-exchange" if settings.dlx
      settings.dlrk = parse_header("x-dead-letter-routing-key", String)
      effective_args << "x-dead-letter-routing-key" if settings.dlrk
      settings.expires = parse_header("x-expires", Int).try &.to_i64
      effective_args << "x-expires" if settings.expires
      settings.max_length = parse_header("x-max-length", Int).try &.to_i64
      effective_args << "x-max-length" if settings.max_length
      settings.max_length_bytes = parse_header("x-max-length-bytes", Int).try &.to_i64
      effective_args << "x-max-length-bytes" if settings.max_length_bytes
      settings.message_ttl = parse_header("x-message-ttl", Int).try &.to_i64
      effective_args << "x-message-ttl" if settings.message_ttl
      settings.delivery_limit = parse_header("x-delivery-limit", Int).try &.to_i64
      effective_args << "x-delivery-limit" if settings.delivery_limit
      overflow = parse_header("x-overflow", String)
      settings.reject_on_overflow = overflow == "reject-publish"
      effective_args << "x-overflow" if settings.reject_on_overflow? || overflow == "drop-head"
      @single_active_consumer_queue = parse_header("x-single-active-consumer", Bool) == true
      effective_args << "x-single-active-consumer" if @single_active_consumer_queue
      settings.consumer_timeout = parse_header("x-consumer-timeout", Int).try &.to_u64
      effective_args << "x-consumer-timeout" if settings.consumer_timeout
      if parse_header("x-message-deduplication", Bool)
        effective_args << "x-message-deduplication"
        size = parse_header("x-cache-size", Int).try(&.to_u32)
        effective_args << "x-cache-size" if size
        ttl = parse_header("x-cache-ttl", Int).try(&.to_u32)
        effective_args << "x-cache-ttl" if ttl
        header_key = parse_header("x-deduplication-header", String)
        effective_args << "x-deduplication-header" if header_key
        @deduper ||= begin
          cache = Deduplication::MemoryCache(AMQ::Protocol::Field).new(size)
          Deduplication::Deduper.new(cache, ttl, header_key)
        end
      end
      settings.delayed_retry_min = parse_header("x-delayed-retry-min", Int).try(&.to_i64)
      if settings.delayed_retry_min
        if @deduper
          raise LavinMQ::Error::PreconditionFailed.new(
            "x-delayed-retry-min cannot be combined with x-message-deduplication")
        end
        effective_args << "x-delayed-retry-min"
      end
      settings.delayed_retry_multiplier = parse_header("x-delayed-retry-multiplier", Int).try(&.to_i32)
      effective_args << "x-delayed-retry-multiplier" if settings.delayed_retry_multiplier
      settings.delayed_retry_max = parse_header("x-delayed-retry-max", Int).try(&.to_i64)
      effective_args << "x-delayed-retry-max" if settings.delayed_retry_max
      settings
    end

    private macro parse_header(header, type)
      if value = @arguments["{{ header.id }}"]?
        value.as?({{ type }}) || raise LavinMQ::Error::PreconditionFailed.new("{{ header.id }} header not a {{ type.id }}")
      end
    end

    def after_policy_applied
      @logged_retry_policy_conflict = nil unless Policy.merge_definitions(policy, operator_policy).has_key?("delayed-retry-min")
      configure_delayed_retry
    rescue ex
      @log.error(ex) { "Failed to configure retry queue" }
    end

    # The default delivery limit is resolved after the policy is applied, so a
    # policy delivery-limit also applies to queues relying on the default
    private def resolve_delayed_retry(settings : Settings) : Nil
      if settings.delayed_retry_min
        settings.delivery_limit ||= DEFAULT_DELAYED_RETRY_DELIVERY_LIMIT
      else
        settings.delayed_retry_multiplier = nil
        settings.delayed_retry_max = nil
        settings.effective_args.reject! &.in?("x-delayed-retry-multiplier", "x-delayed-retry-max")
        policy_args = effective_policy_args
        if policy_args.any? &.in?("delayed-retry-multiplier", "delayed-retry-max")
          @effective_policy_args.set(policy_args.reject(&.in?("delayed-retry-multiplier", "delayed-retry-max")), :release)
        end
      end
    end

    private def configure_delayed_retry : Nil
      if settings.delayed_retry_min
        @retry_queue_lock.synchronize { create_retry_queue.resume_retries }
      else
        @retry_queue_lock.synchronize { @delayed_retry_queue }.try &.drain
      end
    end

    private def awaiting_policy? : Bool
      settings.delayed_retry_min.nil? && !@vhost.policies.empty?
    end

    private def recover_retry_queue : Nil
      return if internal? || !durable? || @delayed_retry_queue
      q_name = "amq.retry-#{@name}"
      return if q_name.bytesize > DelayedQueue::MAX_NAME_LENGTH
      return unless Dir.exists?(File.join(@vhost.data_dir, Digest::SHA1.hexdigest(q_name)))
      @retry_queue_lock.synchronize { create_retry_queue }
    end

    protected def retry_queue_drained(retry_queue : DelayedRetryQueue) : Nil
      @retry_queue_lock.synchronize do
        return unless @delayed_retry_queue.same?(retry_queue)
        return unless retry_queue.draining? && retry_queue.empty?
        @delayed_retry_queue = nil
        @log.info { "Retry queue drained, deleting it" }
        retry_queue.delete
      end
    end

    # Recreates the retry queue if it was deleted or closed itself on a store error.
    private def active_retry_queue : DelayedRetryQueue?
      @retry_queue_lock.synchronize do
        if retry_queue = @delayed_retry_queue
          next retry_queue unless retry_queue.closed?
          @delayed_retry_queue = nil
        end
        next if @deleted || @closed
        @log.info { "Recreating retry queue" }
        create_retry_queue
      end
    rescue ex
      @log.error(ex) { "Failed to recreate retry queue, requeuing instantly" }
      nil
    end

    # Only call while holding @retry_queue_lock
    private def create_retry_queue : DelayedRetryQueue
      if queue = @delayed_retry_queue
        return queue
      end
      queue = DelayedRetryQueue.create(@vhost, self)
      @delayed_retry_queue = queue
      @vhost.register_queue(queue)
      queue
    end

    # The message keeps its original timestamp while delayed, so x-message-ttl
    # keeps applying to the message's total age; the delayed store expires at
    # timestamp + x-delay, so the age since publish is folded into the delay
    private def route_to_retry_queue(sp : SegmentPosition, msg : BytesMessage, retry_queue : DelayedRetryQueue, base_delay : Int64) : Bool
      delivery_count = @msg_store_lock.synchronize { @deliveries.fetch(sp, 1) }
      delay_ms = calculate_retry_delay(base_delay, delivery_count)
      props = msg.properties
      h = props.headers || AMQP::Table.new
      h["x-delay"] = delay_from(msg.timestamp, delay_ms)
      h["x-delivery-count"] = delivery_count
      props.headers = h
      retry_msg = Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        props, msg.bodysize, IO::Memory.new(msg.body))
      return false unless retry_queue.delay(retry_msg)
      delete_message(sp)
      true
    end

    protected def delay_from(timestamp : Int64, backoff : Int64) : UInt32
      age = RoughTime.unix_ms - timestamp
      age = 0_i64 if age < 0
      Math.min(backoff + age, DELAYED_RETRY_MAX_DELAY_MS).to_u32
    end

    protected def retry_delay_for(delivery_count : Int32) : Int64
      calculate_retry_delay(settings.delayed_retry_min || DELAYED_RETRY_DRAIN_REDELAY_MS, delivery_count)
    end

    # Delay for attempt n: base × n, or base × multiplier^(n-1) with a multiplier,
    # clamped at x-delayed-retry-max and the UInt32 range of x-delay. The cap
    # never ends retries; termination is governed by x-delivery-limit only.
    private def calculate_retry_delay(base_delay : Int64, delivery_count : Int32) : Int64
      settings = self.settings
      cap = Math.min(settings.delayed_retry_max || DELAYED_RETRY_MAX_DELAY_MS, DELAYED_RETRY_MAX_DELAY_MS)
      delay = Math.min(base_delay, cap)
      if mult = settings.delayed_retry_multiplier
        (delivery_count - 1).times do
          if delay > cap // mult
            delay = cap
            break
          end
          delay *= mult
        end
      elsif delivery_count > cap // delay
        delay = cap
      else
        delay *= delivery_count
      end
      Math.min(delay, cap)
    end

    def immediate_delivery?
      @consumers_lock.synchronize do
        @consumers.any? &.accepts?
      end
    end

    def message_count
      @msg_store.size.to_u32
    end

    def empty? : Bool
      @msg_store.empty?
    end

    def consumer_count
      consumers_size.to_u32
    end

    def state_match?(states : Array(QueueState)) : Bool
      states.includes?(@state)
    end

    def pause!
      return unless @state.running?
      @state = QueueState::Paused
      @log.debug { "Paused" }
      @paused.set(true)
      File.touch(File.join(@data_dir, "paused"))
    end

    def resume!
      return unless @state.paused?
      @state = QueueState::Running
      @log.debug { "Resuming" }
      @paused.set(false)
      File.delete(File.join(@data_dir, "paused"))
    end

    def close : Bool
      return false if @closed
      @closed = true
      @state = QueueState::Closed
      # Before yielding or deleting, so a redeclared queue can't get this link
      @vhost.upstreams.try &.stop_link(self)
      @queue_expiration_ttl_change.close
      @message_ttl_change.close
      @paused.close
      @consumers_empty.close
      @consumers_lock.synchronize do
        @consumers_closed = true
        @consumers.each &.cancel
        @consumers.clear
        @consumer_count.set(0, :relaxed)
        @exclusive_consumer = false
        @has_priority_consumers = false
      end
      Fiber.yield           # Let deliver_loop fibers start and react to closed channels
      @deliver_loop_wg.wait # Wait for all deliver loops to exit before closing mmap:s
      @msg_store_lock.synchronize do
        @msg_store.close
        @deliveries.clear
      end
      @basic_get_unacked.lock(&.clear)
      @deduper = nil
      # TODO: When closing due to ReadError, queue is deleted if exclusive
      delete if !durable? || @exclusive
      @retry_queue_lock.synchronize { @delayed_retry_queue.try &.close }
      Fiber.yield
      @log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      @retry_queue_lock.synchronize do
        if retry_queue = @delayed_retry_queue
          @delayed_retry_queue = nil
          retry_queue.delete
        end
      end
      close
      @state = QueueState::Deleted
      @msg_store_lock.synchronize do
        @msg_store.delete
      end
      if durable?
        @vhost.replicator.try do |r|
          dotqueue_file = File.join(@data_dir, ".queue")
          r.delete_file(dotqueue_file)
        end
      end
      @vhost.delete_queue(@name)
      @log.info { "(messages=#{message_count}) Deleted" }
      @exclusive_owner.try &.exclusive_queue_deleted(self)
      true
    end

    def details_tuple
      stats = queue_stats_details
      {
        name:                         @name,
        durable:                      durable?,
        exclusive:                    @exclusive,
        auto_delete:                  @auto_delete,
        arguments:                    @arguments,
        consumers:                    consumers_size,
        vhost:                        @vhost.name,
        messages:                     @msg_store.size + stats[:messages_unacknowledged],
        total_bytes:                  @msg_store.bytesize + stats[:message_bytes_unacknowledged],
        messages_persistent:          durable? ? @msg_store.size + stats[:messages_unacknowledged] : 0,
        ready:                        @msg_store.size, # Deprecated, to be removed in next major version
        messages_ready:               @msg_store.size,
        ready_bytes:                  @msg_store.bytesize, # Deprecated, to be removed in next major version
        message_bytes_ready:          @msg_store.bytesize,
        ready_avg_bytes:              @msg_store.avg_bytesize,
        unacked:                      stats[:unacked], # Deprecated, to be removed in next major version
        messages_unacknowledged:      stats[:messages_unacknowledged],
        unacked_bytes:                stats[:unacked_bytes], # Deprecated, to be removed in next major version
        message_bytes_unacknowledged: stats[:message_bytes_unacknowledged],
        unacked_avg_bytes:            stats[:unacked_avg_bytes],
        operator_policy:              operator_policy.try &.name,
        policy:                       policy.try &.name,
        exclusive_consumer_tag:       exclusive_consumer_tag,
        single_active_consumer_tag:   @single_active_consumer.try &.tag,
        state:                        @state,
        effective_policy_definition:  Policy.merge_definitions(policy, operator_policy),
        message_stats:                current_stats_details,
        effective_arguments:          settings.effective_args,
        effective_policy_arguments:   effective_policy_args,
        internal:                     internal?,
      }
    end

    private def exclusive_consumer_tag : String?
      return unless @exclusive_consumer
      @consumers_lock.synchronize { @consumers.find(&.exclusive?).try(&.tag) }
    end

    enum PublishResult
      Ok       # message accepted and stored
      Dropped  # message was a duplicate
      Overflow # rejected by max-length / max-length-bytes (overflow=reject-publish)
      Closed   # queue closed, the message wasn't stored
    end

    class Closed < Exception; end

    def publish(msg : Message) : PublishResult
      strip_delivery_count(msg)
      publish_internal(msg, nil, delivery_count: nil)
    end

    # On retry-enabled queues the x-delivery-count header carries the retry
    # budget through the retry queue round trip, so it must not be settable
    # from outside: not by publishers and not inherited from an upstream
    # queue's dead-lettering. The retry queue itself bypasses this by
    # republishing through #publish_internal.
    protected def strip_delivery_count(msg : Message) : Nil
      return unless settings.delayed_retry_min
      if (headers = msg.properties.headers) && headers.has_key?("x-delivery-count")
        headers.delete("x-delivery-count")
        msg.properties.headers = headers
      end
    end

    protected def publish_internal(msg : Message, dlx_tasks : Argument::DeadLettering::Tasks?, *, delivery_count : Int32?) : PublishResult
      return PublishResult::Closed if @closed
      if d = @deduper
        if d.duplicate?(msg)
          @dedup_count.add(1, :relaxed)
          return PublishResult::Dropped
        end
        d.add(msg)
      end
      return PublishResult::Overflow if reject_on_overflow?(msg)
      was_empty = false
      pushed = false
      @msg_store_lock.synchronize do
        was_empty = @msg_store.empty?
        sp = @msg_store.push(msg)
        pushed = true
        record_delivery_count(sp, delivery_count)
        drop_overflow(dlx_tasks)
      end
      @publish_count.add(1, :relaxed)
      ensure_consumers_deliver_loops if was_empty

      # Record activity if message has TTL (needs expiration)
      if settings.message_ttl || msg.properties.expiration
        ensure_expire_fiber
      end

      PublishResult::Ok
    rescue MessageStore::ClosedError
      # A racing delete closed the store. If push hadn't stored the message it's
      # not stored (avoids an HTTP 500); if it had, a later ClosedError from the
      # post-publish expire-fiber check still means the publish succeeded.
      pushed ? PublishResult::Ok : PublishResult::Closed
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    private def reject_on_overflow?(msg) : Bool
      settings = self.settings
      return false unless settings.reject_on_overflow?
      if ml = settings.max_length
        if @msg_store.size >= ml
          @log.debug { "Overflow reject message msg=#{msg}" }
          return true
        end
      end

      if mlb = settings.max_length_bytes
        if @msg_store.bytesize + msg.bytesize >= mlb
          @log.debug { " Overflow reject message msg=#{msg}" }
          return true
        end
      end
      false
    end

    private def ensure_consumers_deliver_loops : Nil
      @consumers_lock.synchronize do
        @consumers.each do |consumer|
          consumer.ensure_deliver_loop
        end
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def drop_overflow(dlx_tasks : Argument::DeadLettering::Tasks? = nil) : Nil
      settings = self.settings
      ml = settings.max_length
      mlb = settings.max_length_bytes
      return unless ml || mlb
      # Special case when a limit is set to 0 and a consumer accepts, the messages
      # should be delivered instantly
      return if ((ml == 0) || (mlb == 0)) && immediate_delivery?

      counter = 0
      if ml
        @msg_store_lock.synchronize do
          while @msg_store.size > ml
            env = @msg_store.shift? || break
            @log.debug { "Overflow drop head sp=#{env.segment_position}" }
            expire_msg(env, :maxlen, dlx_tasks)
            counter &+= 1
            if counter >= 16 * 1024
              Fiber.yield
              counter = 0
            end
          end
        end
      end

      if mlb
        @msg_store_lock.synchronize do
          while @msg_store.bytesize > mlb
            env = @msg_store.shift? || break
            @log.debug { "Overflow drop head sp=#{env.segment_position}" }
            expire_msg(env, :maxlenbytes, dlx_tasks)
            counter &+= 1
            if counter >= 16 * 1024
              Fiber.yield
              counter = 0
            end
          end
        end
      end
    end

    private def drop_redelivered : Nil
      counter = 0
      if limit = settings.delivery_limit
        @msg_store_lock.synchronize do
          loop do
            env = @msg_store.first? || break
            delivery_count = @deliveries.fetch(env.segment_position, 0) || break
            break unless delivery_count > limit
            env = @msg_store.shift? || break
            @log.debug { "Over delivery limit, drop sp=#{env.segment_position}" }
            expire_msg(env, :delivery_limit)
            counter &+= 1
            if counter >= 16 * 1024
              Fiber.yield
              counter = 0
            end
          end
        end
      end
    end

    private def time_to_message_expiration : Time::Span?
      # The message is a view into its segment, which a purge or delete on
      # another thread can unmap as soon as the lock is released
      expire_at = @msg_store_lock.synchronize do
        env = @msg_store.first? || return
        @log.debug { "Checking if message #{env.message} has to be expired" }
        expire_at(env.message)
      end
      if expire_at
        Queue.time_to_expiration_wakeup(expire_at)
      end
    end

    private def has_expired?(msg : BytesMessage, requeue = false) : Bool
      return false if zero_ttl?(msg) && !requeue && !consumers_empty?
      if expire_at = expire_at(msg)
        expire_at <= RoughTime.unix_ms
      else
        false
      end
    end

    private def zero_ttl?(msg) : Bool
      msg.ttl == 0 || settings.message_ttl == 0
    end

    private def expire_at(msg : BytesMessage) : Int64?
      if ttl = settings.message_ttl
        ttl = (mttl = msg.ttl) ? Math.min(ttl, mttl) : ttl
        msg.timestamp + ttl
      elsif ttl = msg.ttl
        msg.timestamp + ttl
      end
    end

    private def expire_messages : Nil
      i = 0
      @msg_store_lock.synchronize do
        loop do
          env = @msg_store.first? || break
          msg = env.message
          @log.debug { "Checking if next message #{msg} has expired" }
          if has_expired?(msg)
            # shift it out from the msgs store, first time was just a peek
            env = @msg_store.shift? || break
            expire_msg(env, :expired)
            i += 1
          else
            break
          end
        end
      end
      @log.info { "Expired #{i} messages" } if i > 0
    end

    private def expire_msg(env : Envelope, reason : Symbol, dlx_tasks : Argument::DeadLettering::Tasks? = nil)
      expire_msg(env.segment_position, reason, dlx_tasks)
    end

    private def expire_msg(sp : SegmentPosition, reason : Symbol, dlx_tasks : Argument::DeadLettering::Tasks? = nil)
      dlx = sp.has_dlx? || !@dead_letter.dlx.nil?
      @vhost.count_dead_lettered(reason, dlx: dlx)
      if dlx
        @log.debug { "Expiring #{sp} now due to #{reason}" }
        # The message is published into other queues outside @msg_store_lock,
        # while a concurrent purge or queue/vhost delete can delete or close
        # its segment, so the segment is leased until the routed callback has
        # run. That can be after #route returns, when an outer dead-lettering
        # chain publishes the queued copies (see DeadLetterer#route).
        env = begin
          @msg_store_lock.synchronize { @msg_store.envelope(sp).lease }
        rescue KeyError | MessageStore::ClosedError
          # The segment (or whole store) is already gone: a racing purge or
          # delete removed the message, so there is nothing left to route.
          return
        end
        routed = false
        begin
          @dead_letter.route(env.message, reason, dlx_tasks) do
            routed = true
            begin
              delete_message sp
            ensure
              env.release
            end
          end
        rescue ex
          env.release unless routed
          raise ex
        end
      else
        delete_message sp
      end
    end

    private def expire_queue : Bool
      @log.debug { "Trying to expire queue" }
      @consumers_lock.synchronize do
        return false unless @consumers.empty?
        @consumers_closed = true
      end
      @log.debug { "Queue expired" }
      @vhost.delete_queue(@name)
      true
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      return false if !@state.running? && (@state.paused? && !force)
      @queue_expiration_ttl_change.try_send? nil
      @deliver_get_count.add(1, :relaxed)
      no_ack ? @get_no_ack_count.add(1, :relaxed) : @get_count.add(1, :relaxed)
      get(no_ack) do |env|
        yield env
      end.tap { ensure_expire_fiber }
    rescue ClosedError | MessageStore::ClosedError
      # The queue was closed/deleted concurrently with the get (its @closed flag
      # or the message store). Report no message available (Basic.GetEmpty)
      # rather than letting the error escape into the connection's read loop.
      false
    end

    # If nil is returned it means that the delivery limit is reached
    def consume_get(no_ack, & : Envelope -> Nil) : Bool
      get(no_ack) do |env|
        yield env
        if env.redelivered
          @redeliver_count.add(1, :relaxed)
        else
          no_ack ? @deliver_no_ack_count.add(1, :relaxed) : @deliver_count.add(1, :relaxed)
          @deliver_get_count.add(1, :relaxed)
        end
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(no_ack : Bool, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      settings = self.settings
      loop do # retry if msg expired or deliver limit hit
        # The message can be acked or purged, and its segment deleted, while
        # the delivery is suspended in a socket write
        @msg_store.shift_with_lease?(@msg_store_lock) do |env|
          if has_expired?(env.message) # guarantee to not deliver expired messages
            expire_msg(env, :expired)
            next
          end
          if settings.counts_deliveries? && !no_ack
            env = with_delivery_count_header(env, settings)
          end
          sp = env.segment_position
          if no_ack
            begin
              yield env # deliver the message
            rescue ex   # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              raise ex
            end
            delete_message(sp)
          else
            @unacked_count.add(1, :relaxed)
            @unacked_bytesize.add(sp.bytesize, :relaxed)
            yield env # deliver the message
            # requeuing of failed delivery is up to the consumer
          end
          # Signal expire loop to recalculate wait time for next message
          @message_ttl_change.try_send? nil
          return true
        end || break
      end
      false
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def unacked_messages : Array(UnackedMessage)
      result = consumers.select(AMQP::Consumer).flat_map do |c|
        c.unacked_messages.compact_map do |u|
          next unless u.queue == self
          if consumer = u.consumer
            UnackedMessage.new(c.channel, u.tag, u.delivered_at, consumer.tag)
          end
        end
      end
      @basic_get_unacked.lock { |unacked| result.concat(unacked) }
    end

    private def with_delivery_count_header(env, settings : Settings) : Envelope
      sp = env.segment_position
      headers = env.message.properties.headers || AMQP::Table.new
      # Acks and rejects on other threads update @deliveries too
      delivery_count = @msg_store_lock.synchronize do
        count = @deliveries[sp]? || delivery_count_from_header(headers, settings) || 0
        @deliveries[sp] = count + 1
        count
      end
      headers["x-delivery-count"] = delivery_count if delivery_count > 0 # x-delivery-count not included in first delivery
      env.message.properties.headers = headers
      env
    end

    protected def counts_deliveries? : Bool
      settings.counts_deliveries?
    end

    private def record_delivery_count(sp : SegmentPosition, delivery_count : Int32?) : Nil
      return unless delivery_count && counts_deliveries?
      @deliveries[sp] = delivery_count
    end

    # See #strip_delivery_count for why the header can be trusted here
    private def delivery_count_from_header(headers, settings : Settings) : Int32?
      return unless settings.delayed_retry_min
      headers["x-delivery-count"]?.try(&.as?(Int)).try(&.to_i32)
    end

    def ack(sp : SegmentPosition) : Nil
      return if @closed
      @log.debug { "Acking #{sp}" }
      @ack_count.add(1, :relaxed)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(sp.bytesize, :relaxed)
      delete_message(sp)
    end

    protected def delete_message(sp : SegmentPosition) : Nil
      # Close tears down @msg_store under @msg_store_lock; a dead-letter routed
      # callback (or any other in-flight delete) racing with close would hit
      # MessageStore::ClosedError on @msg_store.delete. The store is gone
      # anyway, so the per-sp bookkeeping is a no-op.
      return if closed?
      {% unless flag?(:release) %}
        @log.debug { "Deleting: #{sp}" }
      {% end %}
      @msg_store_lock.synchronize do
        @deliveries.delete(sp) if settings.counts_deliveries?
        @msg_store.delete(sp)
      end
    end

    # Consumer-initiated reject (basic.reject and basic.nack): delays via the retry queue if configured
    def reject(sp : SegmentPosition, requeue : Bool)
      return if @closed
      @log.debug { "Rejecting #{sp}, requeue: #{requeue}" }
      @reject_count.add(1, :relaxed)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(sp.bytesize, :relaxed)
      if requeue
        redeliver_message(sp, route_to_retry: true)
      else
        expire_msg(sp, :rejected)
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Broker-initiated requeue (channel close, basic.recover): always instant, never via the retry queue
    def requeue(sp : SegmentPosition)
      return if @deleted || @closed
      @log.debug { "Requeuing #{sp}" }
      @reject_count.add(1, :relaxed)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(sp.bytesize, :relaxed)
      redeliver_message(sp, route_to_retry: false)
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    private def redeliver_message(sp : SegmentPosition, route_to_retry : Bool)
      # The message is a view into its segment, only valid while the lock is held
      expired = @msg_store_lock.synchronize { has_expired?(@msg_store[sp], requeue: true) }
      if expired # guarantee to not deliver expired messages
        return expire_msg(sp, :expired)
      end
      settings = self.settings
      if delivery_limit = settings.delivery_limit
        if @msg_store_lock.synchronize { @deliveries.fetch(sp, 0) } > delivery_limit
          return expire_msg(sp, :delivery_limit)
        end
      end
      if route_to_retry && (delayed_retry_min = settings.delayed_retry_min)
        if retry_queue = active_retry_queue
          # DelayedRetryQueue#delay writes the message to its own store before
          # returning, so the lease only has to cover the call
          env = @msg_store_lock.synchronize { @msg_store.envelope(sp).lease }
          begin
            return if route_to_retry_queue(sp, env.message, retry_queue, delayed_retry_min)
          ensure
            env.release
          end
        end
      end
      was_empty = false
      @msg_store_lock.synchronize do
        was_empty = @msg_store.empty?
        @msg_store.requeue(sp)
      end
      drop_overflow
      ensure_consumers_deliver_loops if was_empty
      ensure_expire_fiber
    end

    # Adds the consumer unless the queue is in exclusive use, checked atomically
    # with the add. Returns false if the consumer was refused for that reason,
    # raises ClosedError if the queue is closed or about to be deleted.
    def add_consumer(consumer : Client::Channel::Consumer) : Bool
      raise ClosedError.new if @closed
      consumers_size = @consumers_lock.synchronize do
        raise ClosedError.new if @consumers_closed
        return false if in_exclusive_use?(consumer.exclusive?)
        was_empty = @consumers.empty?
        @consumers << consumer
        @consumer_count.set(@consumers.size, :relaxed)
        @exclusive_consumer = true if consumer.exclusive?
        @has_priority_consumers = true unless consumer.priority.zero?
        if was_empty
          @single_active_consumer = consumer if @single_active_consumer_queue
          notify_consumers_empty(false)
        end
        @consumers.size
      end
      consumer.ensure_deliver_loop unless @msg_store.empty?
      @log.debug { "Adding consumer (now #{consumers_size})" }
      @vhost.event_tick(EventType::ConsumerAdded)
      true
    end

    getter? has_priority_consumers = false

    def rm_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      now_empty = delete_queue = false
      @consumers_lock.synchronize do
        deleted = @consumers.delete consumer
        @consumer_count.set(@consumers.size, :relaxed)
        @has_priority_consumers = @consumers.any? { |c| !c.priority.zero? }
        if deleted
          @exclusive_consumer = false if consumer.exclusive?
          @log.debug { "Removing consumer with #{consumer.unacked} unacked messages (#{@consumers.size} consumers left)" }
          if @single_active_consumer == consumer
            @single_active_consumer = @consumers.first?
            if new_consumer = @single_active_consumer
              while @single_active_consumer_change.try_send? new_consumer
              end
            end
          end
          @vhost.event_tick(EventType::ConsumerRemoved)
        end
        # Decide while holding the lock, so that a consumer added concurrently
        # either keeps the queue or is refused because it's being deleted
        if @consumers.empty?
          if @auto_delete
            delete_queue = !@consumers_closed
            @consumers_closed = true
          else
            now_empty = true
            notify_consumers_empty(true)
          end
        end
      end
      if delete_queue
        delete
      elsif now_empty
        # Check if fiber needs to restart for message expiration
        ensure_expire_fiber
      end
    end

    private def notify_consumers_empty(is_empty)
      @consumers_empty.set(is_empty)
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      return 0_u32 if @closed
      delete_count = @msg_store_lock.synchronize do
        # Checked under the lock, so that a message published or shifted for
        # delivery between the check and the purge isn't purged uncounted
        if unacked_count == 0 && max_count >= @msg_store.size
          # If there's no unacked and we're purging all messages, we can purge faster by deleting files
          count = @msg_store.size
          @msg_store.purge_all
          count
        else
          @msg_store.purge(max_count)
        end
      end
      @log.info { "Purged #{delete_count} messages" }
      # Signal expire loop to recalculate wait time for next message, and
      # restart it if it had stopped while the (now purged) head message had no TTL
      if delete_count > 0
        @message_ttl_change.try_send? nil
        ensure_expire_fiber
      end
      delete_count += purge_delayed(max_count - delete_count)
      delete_count
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Delayed messages would otherwise be republished after their backoff,
    # resurrecting messages the user just purged
    private def purge_delayed(max_count) : UInt32
      retry_queue = @delayed_retry_queue
      return 0_u32 unless retry_queue && max_count > 0
      retry_queue.purge(max_count)
    rescue ex : MessageStore::Error
      @log.error(ex) { "Failed to purge retry queue" }
      0_u32
    end

    def match?(frame)
      durable? == frame.durable &&
        @exclusive == frame.exclusive &&
        @auto_delete == frame.auto_delete &&
        @arguments == frame.arguments
    end

    def match?(durable, exclusive, auto_delete, arguments)
      durable? == durable &&
        @exclusive == exclusive &&
        @auto_delete == auto_delete &&
        @arguments == arguments
    end

    def in_use?
      !(empty? && consumers_empty?)
    end

    def to_json(json : JSON::Builder, consumer_limit : Int32 = -1)
      json.object do
        details_tuple.each do |k, v|
          next if k == :message_stats # rewritten below with the rate-history log
          json.field(k, v) unless v.nil?
        end
        json.field("message_stats") do
          json.object do
            stats_details.each do |k, v|
              json.field(k, v) unless v.nil?
            end
          end
        end
        json.field("consumer_details") do
          json.array do
            @consumers_lock.synchronize do
              @consumers.each do |c|
                c.to_json(json)
                consumer_limit -= 1
                break if consumer_limit.zero?
              end
            end
          end
        end
      end
    end

    # Used for when channel recovers without requeue
    # eg. redelivers messages it already has unacked
    def read(sp : SegmentPosition) : Envelope
      @msg_store_lock.synchronize { @msg_store.envelope(sp, redelivered: true) }
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    def durable?
      false
    end

    class Error < Exception; end

    class ReadError < Exception; end

    class ClosedError < Error; end
  end
end

# Required after the class: DelayedQueue and its children subclass Queue, so a top require would fail on the not-yet-defined superclass.
require "./delayed_queue"
