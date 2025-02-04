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
require "./message_store"
require "../../unacked_message"
require "../../deduplication"

module LavinMQ::AMQP
  class Queue < LavinMQ::Queue
    include PolicyTarget
    include Observable(QueueEvent)
    include Stats
    include SortableJSON

    @message_ttl : Int64?
    @max_length : Int64?
    @max_length_bytes : Int64?
    @expires : Int64?
    @delivery_limit : Int64?
    @dlx : String?
    @dlrk : String?
    @reject_on_overflow = false
    @exclusive_consumer = false
    @deliveries = Hash(SegmentPosition, Int32).new
    @consumers = Array(Client::Channel::Consumer).new
    @consumers_lock = Mutex.new
    @message_ttl_change = ::Channel(Nil).new

    getter basic_get_unacked = Deque(UnackedMessage).new
    getter unacked_count = 0u32
    getter unacked_bytesize = 0u64
    @unacked_lock = Mutex.new(:unchecked)

    @msg_store_lock = Mutex.new(:reentrant)
    @msg_store : MessageStore

    getter? paused = false
    getter paused_change = ::Channel(Bool).new

    getter consumer_timeout : UInt64? = Config.instance.consumer_timeout

    @consumers_empty_change = ::Channel(Bool).new
    @queue_expiration_ttl_change = ::Channel(Nil).new

    private def queue_expire_loop
      loop do
        break unless ttl = @expires
        if @consumers.empty?
          @log.debug { "Queue expires in #{ttl}" }
          select
          when @queue_expiration_ttl_change.receive
          when @consumers_empty_change.receive
          when timeout ttl.milliseconds
            expire_queue
            close
            break
          end
        else
          select
          when @queue_expiration_ttl_change.receive
          when @consumers_empty_change.receive
          end
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    private def message_expire_loop
      loop do
        if @msg_store.empty?
          @msg_store.empty_change.receive
        else
          if @consumers.empty?
            if ttl = time_to_message_expiration
              select
              when @message_ttl_change.receive
              when @msg_store.empty_change.receive # might be empty now (from basic get)
              when @consumers_empty_change.receive
              when timeout ttl
                expire_messages
              end
            else
              # first message in queue should not be expired
              # wait for empty queue or TTL change
              select
              when @message_ttl_change.receive
              when @msg_store.empty_change.receive
              end
            end
          else
            @consumers_empty_change.receive
          end
        end
      rescue ::Channel::ClosedError
        break
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(
      {"ack", "deliver", "deliver_get", "confirm", "get", "get_no_ack", "publish", "redeliver", "reject", "return_unroutable", "dedup"},
      {"message_count", "unacked_count"})

    getter name, arguments, vhost, consumers
    getter? auto_delete, exclusive
    getter policy : Policy?
    getter operator_policy : OperatorPolicy?
    getter? closed = false
    getter state = QueueState::Running
    getter empty_change : ::Channel(Bool)
    getter single_active_consumer : Client::Channel::Consumer? = nil
    getter single_active_consumer_change = ::Channel(Client::Channel::Consumer).new
    @single_active_consumer_queue = false
    @data_dir : String
    Log = LavinMQ::Log.for "queue"
    @metadata : ::Log::Metadata
    @deduper : Deduplication::Deduper?

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = AMQP::Table.new)
      @data_dir = make_data_dir
      @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
      @log = Logger.new(Log, @metadata)
      File.open(File.join(@data_dir, ".queue"), "w") { |f| f.sync = true; f.print @name }
      @state = QueueState::Paused if File.exists?(File.join(@data_dir, ".paused"))
      @msg_store = init_msg_store(@data_dir)
      if @msg_store.closed
        close
      end
      @empty_change = @msg_store.empty_change
      handle_arguments
      spawn queue_expire_loop, name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}" if @expires
      spawn message_expire_loop, name: "Queue#message_expire_loop #{@vhost.name}/#{@name}"
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

    def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?) # ameba:disable Metrics/CyclomaticComplexity
      clear_policy
      Policy.merge_definitions(policy, operator_policy).each do |k, v|
        @log.debug { "Applying policy #{k}: #{v}" }
        case k
        when "max-length"
          unless @max_length.try &.< v.as_i64
            @max_length = v.as_i64
            drop_overflow
          end
        when "max-length-bytes"
          unless @max_length_bytes.try &.< v.as_i64
            @max_length_bytes = v.as_i64
            drop_overflow
          end
        when "message-ttl"
          unless @message_ttl.try &.< v.as_i64
            @message_ttl = v.as_i64
            @message_ttl_change.try_send? nil
          end
        when "expires"
          unless @expires.try &.< v.as_i64
            @expires = v.as_i64
            spawn queue_expire_loop, name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}"
            @queue_expiration_ttl_change.try_send? nil
          end
        when "overflow"
          @reject_on_overflow ||= v.as_s == "reject-publish"
        when "dead-letter-exchange"
          @dlx ||= v.as_s
        when "dead-letter-routing-key"
          @dlrk ||= v.as_s
        when "delivery-limit"
          @delivery_limit ||= v.as_i64
        when "federation-upstream"
          @vhost.upstreams.try &.link(v.as_s, self)
        when "federation-upstream-set"
          @vhost.upstreams.try &.link_set(v.as_s, self)
        when "consumer-timeout"
          unless @consumer_timeout.try &.< v.as_i64
            @consumer_timeout = v.as_i64.to_u64
          end
        end
      end
      @policy = policy
      @operator_policy = operator_policy
    end

    private def clear_policy
      handle_arguments
      @operator_policy = nil
      return if @policy.nil?
      @policy = nil
      @vhost.upstreams.try &.stop_link(self)
    end

    private def handle_arguments
      @dlx = parse_header("x-dead-letter-exchange", String)
      @dlrk = parse_header("x-dead-letter-routing-key", String)
      if @dlrk && @dlx.nil?
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
      end
      @expires = parse_header("x-expires", Int).try &.to_i64
      validate_gt_zero("x-expires", @expires)
      @queue_expiration_ttl_change.try_send? nil
      @max_length = parse_header("x-max-length", Int).try &.to_i64
      validate_positive("x-max-length", @max_length)
      @max_length_bytes = parse_header("x-max-length-bytes", Int).try &.to_i64
      validate_positive("x-max-length-bytes", @max_length_bytes)
      @message_ttl = parse_header("x-message-ttl", Int).try &.to_i64
      validate_positive("x-message-ttl", @message_ttl)
      @message_ttl_change.try_send? nil
      @delivery_limit = parse_header("x-delivery-limit", Int).try &.to_i64
      validate_positive("x-delivery-limit", @delivery_limit)
      @reject_on_overflow = parse_header("x-overflow", String) == "reject-publish"
      @single_active_consumer_queue = parse_header("x-single-active-consumer", Bool) == true
      @consumer_timeout = parse_header("x-consumer-timeout", Int).try &.to_u64
      validate_positive("x-consumer-timeout", @consumer_timeout)
      if parse_header("x-message-deduplication", Bool)
        size = parse_header("x-cache-size", Int).try(&.to_u32)
        raise LavinMQ::Error::PreconditionFailed.new("Invalid x-cache-size for message deduplication") unless size
        ttl = parse_header("x-cache-ttl", Int).try(&.to_u32)
        header_key = parse_header("x-deduplication-header", String)
        cache = Deduplication::MemoryCache(AMQ::Protocol::Field).new(size)
        @deduper = Deduplication::Deduper.new(cache, ttl, header_key)
      end
    end

    private macro parse_header(header, type)
      if value = @arguments["{{ header.id }}"]?
        value.as?({{ type }}) || raise LavinMQ::Error::PreconditionFailed.new("{{ header.id }} header not a {{ type.id }}")
      end
    end

    private def validate_positive(header, value) : Nil
      return if value.nil?
      return if value >= 0
      raise LavinMQ::Error::PreconditionFailed.new("#{header} has to be positive")
    end

    private def validate_gt_zero(header, value) : Nil
      return if value.nil?
      return if value > 0
      raise LavinMQ::Error::PreconditionFailed.new("#{header} has to be larger than 0")
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
      @paused = true
      while @paused_change.try_send? true
      end
      File.touch(File.join(@data_dir, ".paused"))
    end

    def resume!
      return unless @state.paused?
      @state = QueueState::Running
      @log.debug { "Resuming" }
      @paused = false
      while @paused_change.try_send? false
      end
      File.delete(File.join(@data_dir, ".paused"))
    end

    def close : Bool
      return false if @closed
      @closed = true
      @state = QueueState::Closed
      @queue_expiration_ttl_change.close
      @message_ttl_change.close
      @paused_change.close
      @consumers_empty_change.close
      @consumers_lock.synchronize do
        @consumers.each &.cancel
        @consumers.clear
      end
      Fiber.yield # Allow all consumers to cancel before closing mmap:s
      @msg_store_lock.synchronize do
        @msg_store.close
      end
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
      @vhost.users.each do |_, user|
        user.remove_queue_from_acl_caches(@vhost.name, @name)
      end
      true
    end

    def details_tuple
      {
        name:                        @name,
        durable:                     durable?,
        exclusive:                   @exclusive,
        auto_delete:                 @auto_delete,
        arguments:                   @arguments,
        consumers:                   @consumers.size,
        vhost:                       @vhost.name,
        messages:                    @msg_store.size + @unacked_count,
        total_bytes:                  @msg_store.bytesize + @unacked_bytesize,
        messages_persistent:         durable? ? @msg_store.size + @unacked_count : 0,
        ready:                       @msg_store.size,
        ready_bytes:                 @msg_store.bytesize,
        ready_avg_bytes:             @msg_store.avg_bytesize,
        unacked:                     @unacked_count,
        unacked_bytes:               @unacked_bytesize,
        unacked_avg_bytes:           unacked_avg_bytes,
        operator_policy:             @operator_policy.try &.name,
        policy:                      @policy.try &.name,
        exclusive_consumer_tag:      @exclusive ? @consumers_lock.synchronize { @consumers.first?.try(&.tag) } : nil,
        single_active_consumer_tag:  @single_active_consumer.try &.tag,
        state:                       @state.to_s,
        effective_policy_definition: Policy.merge_definitions(@policy, @operator_policy),
        message_stats:               current_stats_details,
      }
    end

    private def unacked_avg_bytes : UInt64
      return 0u64 if @unacked_count.zero?
      @unacked_bytesize // @unacked_count
    end

    class RejectOverFlow < Exception; end

    class Closed < Exception; end

    def publish(msg : Message) : Bool
      return false if @deleted || @state.closed?
      if d = @deduper
        if d.duplicate?(msg)
          @dedup_count += 1
          return false
        end
        d.add(msg)
      end
      reject_on_overflow(msg)
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count += 1
      end
      drop_overflow unless immediate_delivery?
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

    private def drop_overflow : Nil
      counter = 0
      if ml = @max_length
        @msg_store_lock.synchronize do
          while @msg_store.size > ml
            env = @msg_store.shift? || break
            @log.debug { "Overflow drop head sp=#{env.segment_position}" }
            expire_msg(env, :maxlen)
            counter &+= 1
            if counter >= 16 * 1024
              Fiber.yield
              counter = 0
            end
          end
        end
      end

      if mlb = @max_length_bytes
        @msg_store_lock.synchronize do
          while @msg_store.bytesize > mlb
            env = @msg_store.shift? || break
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
      else
        nil
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
      if sp.has_dlx? || @dlx
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
      if dlx = msg.dlx || @dlx
        if dead_letter_loop?(msg.properties.headers, reason)
          @log.debug { "#{msg} in a dead letter loop, dropping it" }
        else
          dlrk = msg.dlrk || @dlrk || msg.routing_key
          props = handle_dlx_header(msg, reason)
          dead_letter_msg(msg, props, dlx, dlrk)
        end
      end
      delete_message sp
    end

    # checks if the message has been dead lettered to the same queue
    # for the same reason already
    private def dead_letter_loop?(headers, reason) : Bool
      return false if headers.nil?
      if xdeaths = headers["x-death"]?.as?(Array(AMQ::Protocol::Field))
        xdeaths.each do |xd|
          if xd = xd.as?(AMQ::Protocol::Table)
            break if xd["reason"]? == "rejected"
            if xd["queue"]? == @name && xd["reason"]? == reason.to_s
              @log.debug { "preventing dead letter loop" }
              return true
            end
          end
        end
      end
      false
    end

    private def handle_dlx_header(msg, reason) : AMQP::Properties
      h = msg.properties.headers || AMQP::Table.new
      h.reject! { |k, _| k.in?("x-dead-letter-exchange", "x-dead-letter-routing-key") }

      # there's a performance advantage to do `has_key?` over `||=`
      h["x-first-death-reason"] = reason.to_s unless h.has_key? "x-first-death-reason"
      h["x-first-death-queue"] = @name unless h.has_key? "x-first-death-queue"
      h["x-first-death-exchange"] = msg.exchange_name unless h.has_key? "x-first-death-exchange"

      routing_keys = [msg.routing_key.as(AMQP::Field)]
      if cc = h.delete("CC")
        # should route to all the CC RKs but then delete them,
        # so we (ab)use the BCC header for that
        h["BCC"] = cc
        routing_keys.concat cc.as(Array(AMQP::Field))
      end

      msg.properties.headers = handle_xdeath_header(h, msg.exchange_name, routing_keys, reason, msg.properties.expiration)
      msg.properties.expiration = nil
      msg.properties
    end

    private def handle_xdeath_header(headers, exchange_name, routing_keys, reason, expiration) : AMQP::Table
      xdeaths = headers["x-death"]?.as?(Array(AMQP::Field)) || Array(AMQP::Field).new(1)

      found_at = -1
      xdeaths.each_with_index do |xd, idx|
        xd = xd.as(AMQP::Table)
        next if xd["queue"]? != @name
        next if xd["reason"]? != reason.to_s
        next if xd["exchange"]? != exchange_name
        count = xd["count"].as?(Int) || 0
        xd.merge!({
          count:          count + 1,
          time:           RoughTime.utc,
          "routing-keys": routing_keys,
        })
        xd["original-expiration"] = expiration if expiration
        found_at = idx
        break
      end

      case found_at
      when -1 # not found so inserting new x-death
        death = AMQP::Table.new({
          "queue":        @name,
          "reason":       reason.to_s,
          "exchange":     exchange_name,
          "count":        1,
          "time":         RoughTime.utc,
          "routing-keys": routing_keys,
        })
        death["original-expiration"] = expiration if expiration
        xdeaths.unshift death
      when 0
        # do nothing, updated xd is in the front
      else
        # move updated xd to the front
        xd = xdeaths.delete_at(found_at)
        xdeaths.unshift xd
      end
      headers["x-death"] = xdeaths
      headers
    end

    private def dead_letter_msg(msg : BytesMessage, props, dlx, dlrk)
      @log.debug { "Dead lettering ex=#{dlx} rk=#{dlrk} body_size=#{msg.bodysize} props=#{props}" }
      @vhost.publish Message.new(RoughTime.unix_ms, dlx.to_s, dlrk.to_s,
        props, msg.bodysize, IO::Memory.new(msg.body))
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
      @get_count += 1
      @deliver_get_count += 1
      get(no_ack) do |env|
        yield env
      end
    end

    # If nil is returned it means that the delivery limit is reached
    def consume_get(consumer, & : Envelope -> Nil) : Bool
      get(consumer.no_ack?) do |env|
        yield env
        if env.redelivered
          @redeliver_count += 1
        else
          @deliver_count += 1
          @deliver_get_count += 1
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
          mark_unacked(sp) do
            yield env # deliver the message
          end
        end
        return true
      end
      false
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    private def mark_unacked(sp, &)
      @log.debug { "Counting as unacked: #{sp}" }
      @unacked_lock.synchronize do
        @unacked_count += 1
        @unacked_bytesize += sp.bytesize
      end
      begin
        yield
      rescue ex
        @log.debug { "Not counting as unacked: #{sp}" }
        @msg_store_lock.synchronize do
          @msg_store.requeue(sp)
        end
        @unacked_lock.synchronize do
          @unacked_count -= 1
          @unacked_bytesize -= sp.bytesize
        end
        raise ex
      end
    end

    private def with_delivery_count_header(env) : Envelope?
      if limit = @delivery_limit
        sp = env.segment_position
        headers = env.message.properties.headers || AMQP::Table.new
        delivery_count = @deliveries.fetch(sp, 0)
        # @log.debug { "Delivery count: #{delivery_count} Delivery limit: #{@delivery_limit}" }
        if delivery_count >= limit
          expire_msg(env, :delivery_limit)
          return nil
        end
        headers["x-delivery-count"] = @deliveries[sp] = delivery_count + 1
        env.message.properties.headers = headers
      end
      env
    end

    def ack(sp : SegmentPosition) : Nil
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @unacked_lock.synchronize do
        @ack_count += 1
        @unacked_count -= 1
        @unacked_bytesize -= sp.bytesize
      end
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
      @unacked_lock.synchronize do
        @reject_count += 1
        @unacked_count -= 1
        @unacked_bytesize -= sp.bytesize
      end
      if requeue
        if has_expired?(sp, requeue: true) # guarantee to not deliver expired messages
          expire_msg(sp, :expired)
        else
          @msg_store_lock.synchronize do
            @msg_store.requeue(sp)
          end
          drop_overflow unless immediate_delivery?
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
      while @consumers_empty_change.try_send? is_empty
      end
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      delete_count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
      @log.info { "Purged #{delete_count} messages" }
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
        details_tuple.merge(message_stats: stats_details).each do |k, v|
          json.field(k, v) unless v.nil?
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
