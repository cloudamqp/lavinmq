require "digest/sha1"
require "../../logger"
require "../../segment_position"
require "../../policy"
require "../../observable"
require "../../stats"
require "../../sortable_json"
require "../../client/channel/consumer"
require "../../message"
require "../../error"
require "../../queue"
require "./state"
require "./event"
require "../../message_store"
require "../../unacked_message"
require "../../deduplication"
require "../../bool_channel"
require "../argument_target"
require "../argument"

module LavinMQ::AMQP
  class Queue < LavinMQ::Queue
    include PolicyTarget
    include Observable(QueueEvent)
    include Stats
    include SortableJSON

    include ArgumentTarget
    include Argument::DeadLettering

    VALIDATOR_INT_ZERO = ArgumentValidator::IntValidator.new(min_value: 0)
    VALIDATOR_INT_ONE  = ArgumentValidator::IntValidator.new(min_value: 1)
    VALIDATOR_STRING   = ArgumentValidator::StringValidator.new
    VALIDATOR_BOOL     = ArgumentValidator::BoolValidator.new

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

    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    @message_ttl : Int64?
    @max_length : Int64?
    @max_length_bytes : Int64?
    @expires : Int64?
    @delivery_limit : Int64?
    @reject_on_overflow = false
    @exclusive_consumer = false
    @deliveries = Hash(SegmentPosition, Int32).new
    @consumers = Array(Client::Channel::Consumer).new
    @consumers_lock = Mutex.new
    @message_ttl_change = ::Channel(Nil).new
    @drop_overflow_channel = ::Channel(Nil).new

    getter basic_get_unacked = Deque(UnackedMessage).new
    @unacked_count = Atomic(UInt32).new(0u32)
    @unacked_bytesize = Atomic(UInt64).new(0u64)

    def unacked_count
      @unacked_count.get(:relaxed)
    end

    def unacked_bytesize
      @unacked_bytesize.get(:relaxed)
    end

    @msg_store_lock = Mutex.new(:reentrant)
    @msg_store : MessageStore
    getter deliver_loop_wg = WaitGroup.new

    getter paused = BoolChannel.new(false)

    getter consumer_timeout : UInt64? = Config.instance.consumer_timeout

    getter consumers_empty = BoolChannel.new(true)
    @queue_expiration_ttl_change = ::Channel(Nil).new
    @effective_args = Array(String).new

    getter? internal = false

    private def queue_expire_loop
      loop do
        break unless @expires
        @consumers_empty.when_true.receive
        break unless ttl = @expires
        @log.debug { "Queue expires in #{ttl}ms" }
        select
        when @queue_expiration_ttl_change.receive
        when @consumers_empty.when_false.receive
        when timeout ttl.milliseconds
          expire_queue
          close
          break
        end
      end
    rescue ::Channel::ClosedError
    end

    private def message_expire_loop
      loop do
        @msg_store.empty.when_false.receive
        @log.debug { "message_expire_loop=\"Message store not empty\"" }
        if @consumers.empty?
          if ttl = time_to_message_expiration
            @log.debug { "message_expire_loop=\"Next message\" ttl=\"#{ttl}\"" }
            select
            when @message_ttl_change.receive
              @log.debug { "message_expire_loop=\"Message TTL changed\" ttl=\"#{ttl}\" consumers=0" }
            when @drop_overflow_channel.receive
              @log.debug { "message_expire_loop=\"Drop overflow\" ttl=\"#{ttl}\" consumers=0" }
              drop_overflow
            when @msg_store.empty.when_true.receive
              @log.debug { "message_expire_loop=\"Message store is empty\" ttl=\"#{ttl}\" consumers=0" }
            when @consumers_empty.when_false.receive
              @log.debug { "message_expire_loop=\"Got consumers\" ttl=\"#{ttl}\" consumers=0" }
            when timeout ttl
              @log.debug { "message_expire_loop=\"Message TTL reached\" ttl=\"#{ttl}\" consumers=0" }
              expire_messages
              drop_overflow
            end
          else
            select
            when @message_ttl_change.receive
              @log.debug { "message_expire_loop=\"Message TTL changed\" ttl=\"nil\" consumer=0" }
            when @drop_overflow_channel.receive
              @log.debug { "message_expire_loop=\"Drop overflow\" ttl=\"nil\" consumer=0" }
              drop_overflow
            when @msg_store.empty.when_true.receive
              @log.debug { "message_expire_loop=\"Msg store is empty\" ttl=\"nil\" consumer=0" }
            end
          end
        else
          select
          when @drop_overflow_channel.receive
            @log.debug { "message_expire_loop=\"Drop overflow while having consumers\"" }
            drop_overflow
          when @consumers_empty.when_true.receive
            @log.debug { "message_expire_loop=\"Lost consumers\"" }
          end
        end
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    rescue ::Channel::ClosedError
    end

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(
      {"ack", "deliver", "deliver_no_ack", "deliver_get", "confirm", "get", "get_no_ack", "publish", "redeliver", "reject", "return_unroutable", "dedup"},
      {"message_count", "unacked_count"})

    getter name, arguments, vhost, consumers
    getter? auto_delete, exclusive
    getter policy : Policy?
    getter operator_policy : OperatorPolicy?
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
      File.open(File.join(@data_dir, ".queue"), "w") { |f| f.sync = true; f.print @name }
      @msg_store = init_msg_store(@data_dir)
      @empty = @msg_store.empty
      @dead_letter = Argument::DeadLettering::DeadLetterer.new(@vhost, @name, @log)
      start
    end

    private def start : Bool
      if @msg_store.closed
        !close
      else
        if File.exists?(File.join(@data_dir, ".paused")) # Migrate '.paused' files to 'paused'
          File.rename(File.join(@data_dir, ".paused"), File.join(@data_dir, "paused"))
        end
        if File.exists?(File.join(@data_dir, "paused"))
          @state = QueueState::Paused
          @paused.set(true)
        end
        handle_arguments
        spawn queue_expire_loop, name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}" if @expires
        spawn message_expire_loop, name: "Queue#message_expire_loop #{@vhost.name}/#{@name}"
        true
      end
    end

    def restart! : Bool
      return false unless @closed
      reset_queue_state
      @msg_store = init_msg_store(@data_dir)
      @empty = @msg_store.empty
      start
    end

    private def reset_queue_state
      @closed = false
      @state = QueueState::Running
      # Recreate channels that were closed
      @queue_expiration_ttl_change = ::Channel(Nil).new
      @message_ttl_change = ::Channel(Nil).new
      @drop_overflow_channel = ::Channel(Nil).new
      @paused = BoolChannel.new(false)
      @consumers_empty = BoolChannel.new(true)
      @single_active_consumer_change = ::Channel(Client::Channel::Consumer).new
      @deliver_loop_wg = WaitGroup.new
      @unacked_count.set(0u32, :relaxed)
      @unacked_bytesize.set(0u64, :relaxed)
    end

    # own method so that it can be overriden in other queue implementations
    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.@replicator : nil
      MessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
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

    private def apply_policy_argument(key : String, value : JSON::Any) : Bool # ameba:disable Metrics/CyclomaticComplexity
      @log.debug { "Applying policy #{key}: #{value}" }
      case key
      when "max-length"
        unless @max_length.try &.< value.as_i64
          @max_length = value.as_i64
          @effective_args.delete("x-max-length")
          drop_overflow
          return true
        end
      when "max-length-bytes"
        unless @max_length_bytes.try &.< value.as_i64
          @max_length_bytes = value.as_i64
          @effective_args.delete("x-max-length-bytes")
          drop_overflow
          return true
        end
      when "message-ttl"
        unless @message_ttl.try &.< value.as_i64
          @message_ttl = value.as_i64
          @message_ttl_change.try_send? nil
          @effective_args.delete("x-message-ttl")
          return true
        end
      when "expires"
        unless @expires.try &.< value.as_i64
          @expires = value.as_i64
          spawn queue_expire_loop, name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}"
          @queue_expiration_ttl_change.try_send? nil
          @effective_args.delete("x-expires")
          return true
        end
      when "overflow"
        overflow = value.as_s
        if overflow.in?("reject-publish", "drop-head")
          @reject_on_overflow = overflow == "reject-publish"
          @effective_args.delete("x-overflow")
          return true
        end
      when "dead-letter-exchange"
        if @dead_letter.dlx.nil?
          @dead_letter.dlx ||= value.as_s
          @effective_args.delete("x-dead-letter-exchange")
          return true
        end
      when "dead-letter-routing-key"
        if @dead_letter.dlrk.nil?
          @dead_letter.dlrk ||= value.as_s
          @effective_args.delete("x-dead-letter-routing-key")
          return true
        end
      when "delivery-limit"
        unless @delivery_limit.try &.< value.as_i64
          @delivery_limit = value.as_i64
          @effective_args.delete("x-delivery-limit")
          drop_redelivered
          return true
        end
      when "federation-upstream"
        @vhost.upstreams.try &.link(value.as_s, self)
        return true
      when "federation-upstream-set"
        @vhost.upstreams.try &.link_set(value.as_s, self)
        return true
      when "consumer-timeout"
        unless @consumer_timeout.try &.< value.as_i64
          @consumer_timeout = value.as_i64.to_u64
          @effective_args.delete("x-consumer-timeout")
          return true
        end
      end
      false
    end

    private def clear_policy_arguments
      handle_arguments
      @vhost.upstreams.try &.stop_link(self)
    end

    private def handle_arguments # ameba:disable Metrics/CyclomaticComplexity
      @effective_args = Array(String).new
      @dead_letter.dlx = parse_header("x-dead-letter-exchange", String)
      @effective_args << "x-dead-letter-exchange" if @dead_letter.dlx
      @dead_letter.dlrk = parse_header("x-dead-letter-routing-key", String)
      @effective_args << "x-dead-letter-routing-key" if @dead_letter.dlrk
      @expires = parse_header("x-expires", Int).try &.to_i64
      @effective_args << "x-expires" if @expires
      @queue_expiration_ttl_change.try_send? nil
      @max_length = parse_header("x-max-length", Int).try &.to_i64
      @effective_args << "x-max-length" if @max_length
      @max_length_bytes = parse_header("x-max-length-bytes", Int).try &.to_i64
      @effective_args << "x-max-length-bytes" if @max_length_bytes
      @message_ttl = parse_header("x-message-ttl", Int).try &.to_i64
      @effective_args << "x-message-ttl" if @message_ttl
      @message_ttl_change.try_send? nil
      @delivery_limit = parse_header("x-delivery-limit", Int).try &.to_i64
      @effective_args << "x-delivery-limit" if @delivery_limit
      overflow = parse_header("x-overflow", String)
      @reject_on_overflow = overflow == "reject-publish"
      @effective_args << "x-overflow" if @reject_on_overflow || overflow == "drop-head"
      @single_active_consumer_queue = parse_header("x-single-active-consumer", Bool) == true
      @effective_args << "x-single-active-consumer" if @single_active_consumer_queue
      @consumer_timeout = parse_header("x-consumer-timeout", Int).try &.to_u64
      @effective_args << "x-consumer-timeout" if @consumer_timeout
      if parse_header("x-message-deduplication", Bool)
        @effective_args << "x-message-deduplication"
        size = parse_header("x-cache-size", Int).try(&.to_u32)
        @effective_args << "x-cache-size" if size
        ttl = parse_header("x-cache-ttl", Int).try(&.to_u32)
        @effective_args << "x-cache-ttl" if ttl
        header_key = parse_header("x-deduplication-header", String)
        @effective_args << "x-deduplication-header" if header_key
        @deduper ||= begin
          cache = Deduplication::MemoryCache(AMQ::Protocol::Field).new(size)
          Deduplication::Deduper.new(cache, ttl, header_key)
        end
      end
    end

    private macro parse_header(header, type)
      if value = @arguments["{{ header.id }}"]?
        value.as?({{ type }}) || raise LavinMQ::Error::PreconditionFailed.new("{{ header.id }} header not a {{ type.id }}")
      end
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
      @consumers.size.to_u32
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
      @queue_expiration_ttl_change.close
      @message_ttl_change.close
      @drop_overflow_channel.close
      @paused.close
      @consumers_empty.close
      @consumers_lock.synchronize do
        @consumers.each &.cancel
        @consumers.clear
      end
      Fiber.yield           # Let deliver_loop fibers start and react to closed channels
      @deliver_loop_wg.wait # Wait for all deliver loops to exit before closing mmap:s
      @msg_store_lock.synchronize do
        @msg_store.close
      end
      @deliveries.clear
      @basic_get_unacked.clear
      @deduper = nil
      # TODO: When closing due to ReadError, queue is deleted if exclusive
      delete if !durable? || @exclusive
      Fiber.yield
      notify_observers(QueueEvent::Closed)
      @log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      @state = QueueState::Deleted
      @msg_store_lock.synchronize do
        @msg_store.delete
      end
      @vhost.delete_queue(@name)
      @log.info { "(messages=#{message_count}) Deleted" }
      notify_observers(QueueEvent::Deleted)
      true
    end

    def details_tuple
      unacked_count = unacked_count()
      unacked_bytesize = unacked_bytesize()
      unacked_avg_bytes = unacked_count.zero? ? 0u64 : unacked_bytesize//unacked_count
      {
        name:                         @name,
        durable:                      durable?,
        exclusive:                    @exclusive,
        auto_delete:                  @auto_delete,
        arguments:                    @arguments,
        consumers:                    @consumers.size,
        vhost:                        @vhost.name,
        messages:                     @msg_store.size + unacked_count,
        total_bytes:                  @msg_store.bytesize + unacked_bytesize,
        messages_persistent:          durable? ? @msg_store.size + unacked_count : 0,
        ready:                        @msg_store.size, # Deprecated, to be removed in next major version
        messages_ready:               @msg_store.size,
        ready_bytes:                  @msg_store.bytesize, # Deprecated, to be removed in next major version
        message_bytes_ready:          @msg_store.bytesize,
        ready_avg_bytes:              @msg_store.avg_bytesize,
        unacked:                      unacked_count, # Deprecated, to be removed in next major version
        messages_unacknowledged:      unacked_count,
        unacked_bytes:                unacked_bytesize, # Deprecated, to be removed in next major version
        message_bytes_unacknowledged: unacked_bytesize,
        unacked_avg_bytes:            unacked_avg_bytes,
        operator_policy:              @operator_policy.try &.name,
        policy:                       @policy.try &.name,
        exclusive_consumer_tag:       @exclusive ? @consumers.first?.try(&.tag) : nil,
        single_active_consumer_tag:   @single_active_consumer.try &.tag,
        state:                        @state,
        effective_policy_definition:  Policy.merge_definitions(@policy, @operator_policy),
        message_stats:                current_stats_details,
        effective_arguments:          @effective_args,
        effective_policy_arguments:   effective_policy_args,
        internal:                     internal?,
      }
    end

    class RejectOverFlow < Exception; end

    class Closed < Exception; end

    def publish(msg : Message) : Bool
      return false if @deleted || @state.closed?
      if d = @deduper
        if d.duplicate?(msg)
          @dedup_count.add(1, :relaxed)
          return false
        end
        d.add(msg)
      end
      reject_on_overflow(msg)
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      signal_drop_overflow
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    private def reject_on_overflow(msg) : Nil
      return unless @reject_on_overflow
      if ml = @max_length
        if @msg_store.size >= ml
          @log.debug { "Overflow reject message msg=#{msg}" }
          raise RejectOverFlow.new
        end
      end

      if mlb = @max_length_bytes
        if @msg_store.bytesize + msg.bytesize >= mlb
          @log.debug { " Overflow reject message msg=#{msg}" }
          raise RejectOverFlow.new
        end
      end
    end

    private def signal_drop_overflow : Nil
      @drop_overflow_channel.try_send?(nil) if (@max_length || @max_length_bytes) && !immediate_delivery?
    end

    private def drop_overflow : Nil
      counter = 0

      if ml = @max_length
        loop do
          env = @msg_store_lock.synchronize do
            if @msg_store.size > ml
              @msg_store.shift?
            end
          end
          break unless env
          expire_msg(env, :maxlen)
          counter &+= 1
          if counter >= 16 * 1024
            Fiber.yield
            counter = 0
          end
        end
      end

      if mlb = @max_length_bytes
        loop do
          env = @msg_store_lock.synchronize do
            if @msg_store.bytesize > mlb
              @msg_store.shift?
            end
          end
          break unless env
          @log.debug { "Overflow drop head sp=#{env.segment_position}" }
          expire_msg(env, :maxlenbytes)
          counter &+= 1
          if counter >= 16 * 1024
            Fiber.yield
            counter = 0
          end
        end
      end
    end

    private def drop_redelivered : Nil
      counter = 0
      if limit = @delivery_limit
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
      env = @msg_store_lock.synchronize { @msg_store.first? } || return
      @log.debug { "Checking if message #{env.message} has to be expired" }
      if expire_at = expire_at(env.message)
        expire_in = expire_at - RoughTime.unix_ms
        if expire_in > 0
          expire_in.milliseconds
        else
          Time::Span.zero
        end
      end
    end

    private def has_expired?(sp : SegmentPosition, requeue = false) : Bool
      msg = @msg_store_lock.synchronize { @msg_store[sp] }
      has_expired?(msg, requeue)
    end

    private def has_expired?(msg : BytesMessage, requeue = false) : Bool
      return false if zero_ttl?(msg) && !requeue && !@consumers.empty?
      if expire_at = expire_at(msg)
        expire_at <= RoughTime.unix_ms
      else
        false
      end
    end

    private def zero_ttl?(msg) : Bool
      msg.ttl == 0 || @message_ttl == 0
    end

    private def expire_at(msg : BytesMessage) : Int64?
      if ttl = @message_ttl
        ttl = (mttl = msg.ttl) ? Math.min(ttl, mttl) : ttl
        (msg.timestamp + ttl) // 100 * 100
      elsif ttl = msg.ttl
        (msg.timestamp + ttl) // 100 * 100
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

    private def expire_msg(sp : SegmentPosition, reason : Symbol)
      if sp.has_dlx? || @dead_letter.dlx
        msg = @msg_store_lock.synchronize { @msg_store[sp] }
        env = Envelope.new(sp, msg, false)
        expire_msg(env, reason)
      else
        delete_message sp
      end
    end

    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }

      @dead_letter.route(msg, reason)

      delete_message sp
    end

    private def expire_queue : Bool
      @log.debug { "Trying to expire queue" }
      return false unless @consumers.empty?
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
      end
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
      loop do # retry if msg expired or deliver limit hit
        env = @msg_store_lock.synchronize { @msg_store.shift? } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end
        if @delivery_limit && !no_ack
          env = with_delivery_count_header(env) || next
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
      end
      false
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def unacked_messages
      unacked_messages = consumers.each.select(AMQP::Consumer).flat_map do |c|
        c.unacked_messages.each.compact_map do |u|
          next unless u.queue == self
          if consumer = u.consumer
            UnackedMessage.new(c.channel, u.tag, u.delivered_at, consumer.tag)
          end
        end
      end
      unacked_messages.chain(self.basic_get_unacked.each)
    end

    private def with_delivery_count_header(env) : Envelope?
      if @delivery_limit
        sp = env.segment_position
        headers = env.message.properties.headers || AMQP::Table.new
        delivery_count = @deliveries.fetch(sp, 0)
        headers["x-delivery-count"] = delivery_count if delivery_count > 0 # x-delivery-count not included in first delivery
        @deliveries[sp] = delivery_count + 1
        env.message.properties.headers = headers
      end
      env
    end

    def ack(sp : SegmentPosition) : Nil
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @ack_count.add(1, :relaxed)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(sp.bytesize, :relaxed)
      delete_message(sp)
    end

    protected def delete_message(sp : SegmentPosition) : Nil
      {% unless flag?(:release) %}
        @log.debug { "Deleting: #{sp}" }
      {% end %}
      @deliveries.delete(sp) if @delivery_limit
      @msg_store_lock.synchronize do
        @msg_store.delete(sp)
      end
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted || @closed
      @log.debug { "Rejecting #{sp}, requeue: #{requeue}" }
      @reject_count.add(1, :relaxed)
      @unacked_count.sub(1, :relaxed)
      @unacked_bytesize.sub(sp.bytesize, :relaxed)
      if requeue
        if has_expired?(sp, requeue: true) # guarantee to not deliver expired messages
          expire_msg(sp, :expired)
        else
          if delivery_limit = @delivery_limit
            if @deliveries.fetch(sp, 0) > delivery_limit
              return expire_msg(sp, :delivery_limit)
            end
          end
          @msg_store_lock.synchronize do
            @msg_store.requeue(sp)
          end
          signal_drop_overflow
        end
      else
        expire_msg(sp, :rejected)
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @consumers_lock.synchronize do
        was_empty = @consumers.empty?
        @consumers << consumer
        if was_empty
          @single_active_consumer = consumer if @single_active_consumer_queue
          notify_consumers_empty(false)
        end
      end
      @exclusive_consumer = true if consumer.exclusive?
      @has_priority_consumers = true unless consumer.priority.zero?
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      @vhost.event_tick(EventType::ConsumerAdded)
      notify_observers(QueueEvent::ConsumerAdded, consumer)
    end

    getter? has_priority_consumers = false

    def rm_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @consumers_lock.synchronize do
        deleted = @consumers.delete consumer
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
          notify_observers(QueueEvent::ConsumerRemoved, consumer)
        end
      end
      if @consumers.empty?
        if @auto_delete
          delete
        else
          notify_consumers_empty(true)
        end
      end
    end

    private def notify_consumers_empty(is_empty)
      @consumers_empty.set(is_empty)
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      if unacked_count == 0 && max_count >= message_count
        # If there's no unacked and we're purging all messages, we can purge faster by deleting files
        delete_count = message_count
        @msg_store_lock.synchronize { @msg_store.purge_all }
      else
        delete_count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
      end
      @log.info { "Purged #{delete_count} messages" }
      # Signal expire loop to recalculate wait time for next message
      if delete_count > 0
        @message_ttl_change.try_send? nil
      end
      delete_count
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
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
      !(empty? && @consumers.empty?)
    end

    def to_json(json : JSON::Builder, consumer_limit : Int32 = -1)
      json.object do
        details_tuple.each do |k, v|
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
      msg = @msg_store_lock.synchronize { @msg_store[sp] }
      msg_sp = SegmentPosition.make(sp.segment, sp.position, msg)
      Envelope.new(msg_sp, msg, redelivered: true)
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
