require "digest/sha1"
require "../mfile"
require "../segment_position"
require "../policy"
require "../observable"
require "../stats"
require "../sortable_json"
require "./ready"
require "./unacked"
require "../client/channel"
require "../message"
require "../error"
require "../consumer_store"

module LavinMQ
  enum QueueState
    Running
    Paused
    Flow
    Closed
    Deleted

    def to_s
      super.downcase
    end
  end

  class Queue
    include PolicyTarget
    include Observable
    include Stats
    include SortableJSON

    getter to_deliver = ::Channel(SegmentPosition).new
    @durable = false
    @log : Log
    @message_ttl : ArgumentNumber?
    @max_length : ArgumentNumber?
    @max_length_bytes : ArgumentNumber?
    @expires : ArgumentNumber?
    @delivery_limit : ArgumentNumber?
    @dlx : String?
    @dlrk : String?
    @reject_on_overflow = false
    @exclusive_consumer = false
    @requeued = Set(SegmentPosition).new
    @deliveries = Hash(SegmentPosition, Int32).new
    @read_lock = Mutex.new(:reentrant)
    @consumers = ConsumerStore.new
    @message_available = Channel(Nil).new(1)
    @consumers_empty = Channel(Nil).new(1)
    @refresh_ttl_timeout = Channel(Nil).new(1)
    @ready = ReadyQueue.new
    @unacked = UnackQueue.new
    @paused = Channel(Nil).new(1)

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(
      %w(ack deliver confirm get get_no_ack publish redeliver reject return_unroutable),
      %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers, ready,
      unacked, last_get_time
    getter policy : Policy?
    getter operator_policy : OperatorPolicy?
    getter? closed
    property? internal = false
    getter state = QueueState::Running

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @last_get_time = Time.monotonic
      @log = Log.for "queue[vhost=#{@vhost.name} name=#{@name}]"
      handle_arguments
      if @internal
        spawn expire_loop, name: "Queue#expire_loop #{@vhost.name}/#{@name}"
      else
        spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
      end
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
      @last_get_time = Time.monotonic
      step_loop # necessary to recalculate ttl
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
          @max_length = v.as_i64
          drop_overflow
        when "max-length-bytes"
          @max_length_bytes = v.as_i64
          drop_overflow
        when "message-ttl"
          @message_ttl = v.as_i64
          expire_messages
        when "overflow"
          @reject_on_overflow = v.as_s == "reject-publish"
        when "expires"
          @last_get_time = Time.monotonic
          @expires = v.as_i64
        when "dead-letter-exchange"
          @dlx = v.as_s
        when "dead-letter-routing-key"
          @dlrk = v.as_s
        when "federation-upstream"
          @vhost.upstreams.try &.link(v.as_s, self)
        when "federation-upstream-set"
          @vhost.upstreams.try &.link_set(v.as_s, self)
        when "delivery-limit"
          @delivery_limit = v.as_i64
        else nil
        end
      end
      @policy = policy
      @operator_policy = operator_policy
      step_loop unless @closed
    end

    # force trigger a loop in delivery_loop
    private def step_loop
      message_available
    end

    private def clear_policy
      handle_arguments
      @operator_policy = nil
      return if @policy.nil?
      @policy = nil
      @vhost.upstreams.try &.stop_link(self)
    end

    def unacked_count
      @unacked.size.to_u32
    end

    def message_available
      select
      when @message_available.send nil
      else
      end
    end

    def consumers_empty
      select
      when @consumers_empty.send nil
      else
      end
    end

    def refresh_ttl_timeout
      select
      when @refresh_ttl_timeout.send nil
      else
      end
    end

    private def handle_arguments
      @dlx = parse_header("x-dead-letter-exchange", String)
      @dlrk = parse_header("x-dead-letter-routing-key", String)
      if @dlrk && @dlx.nil?
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
      end
      @expires = parse_header("x-expires", ArgumentNumber)
      validate_gt_zero("x-expires", @expires)
      @max_length = parse_header("x-max-length", ArgumentNumber)
      validate_positive("x-max-length", @max_length)
      @max_length_bytes = parse_header("x-max-length-bytes", ArgumentNumber)
      validate_positive("x-max-length-bytes", @max_length_bytes)
      @message_ttl = parse_header("x-message-ttl", ArgumentNumber)
      validate_positive("x-message-ttl", @message_ttl)
      @delivery_limit = parse_header("x-delivery-limit", ArgumentNumber)
      validate_positive("x-delivery-limit", @delivery_limit)
      @reject_on_overflow = parse_header("x-overflow", String) == "reject-publish"
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
      @consumers.immediate_delivery?
    end

    def message_count
      @ready.size.to_u32
    end

    def empty? : Bool
      @ready.size.zero?
    end

    def any? : Bool
      !empty?
    end

    def consumer_count
      @consumers.size.to_u32
    end

    private def expire_loop
      loop do
        if ttl = time_to_message_expiration
          select
          when @refresh_ttl_timeout.receive
            @log.debug { "Queue#expire_loop Refresh TTL timeout" }
          when timeout ttl
            expire_messages
          end
        else
          @message_available.receive
        end
      rescue Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in expire_loop: #{ex.inspect_with_backtrace}" }
      end
    end

    def pause!
      return unless @state.running?
      @state = QueueState::Paused
    end

    def resume!
      return unless @state.paused?
      @state = QueueState::Running
      @paused.send nil
    end

    def flow=(flow : Bool)
      if flow
        @state = QueueState::Running
        @paused.send nil
      else
        @state = QueueState::Flow
      end
    end

    private def deliver_loop
      loop do
        if @ready.empty?
          receive_or_expire || break
        end
        break if @state.closed?
        deliver_or_expire || break
      rescue Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in deliver_loop: #{ex.inspect_with_backtrace}" }
      end
      @log.debug { "Delivery loop closed" }
    end

    private def time_to_expiration : Time::Span?
      if e = @expires
        expires_in = @last_get_time + e.milliseconds - Time.monotonic
        if expires_in > Time::Span.zero
          expires_in
        else
          Time::Span.zero
        end
      end
    end

    private def receive_or_expire : Bool
      @log.debug { "Waiting for msgs" }
      unless @consumers.empty?
        select
        when @message_available.receive
          return true
        when @consumers_empty.receive
          @log.debug { "Consumers empty" }
        end
      end
      if q_ttl = time_to_expiration
        select
        when @message_available.receive
        when timeout q_ttl
          expire_queue && return false
        end
      else
        @message_available.receive
      end
      @log.debug { "Message available" }
      true
    end

    private def deliver_or_expire : Bool
      @log.debug { "Deliver or expire" }
      sp = @ready.first? || return true
      q_ttl = time_to_expiration
      m_ttl = time_to_message_expiration
      ttl = {q_ttl, m_ttl}.select(Time::Span).min?
      if ttl
        @log.debug &.emit("Queue#deliver_or_expire", ttl: ttl.to_s)
        select
        when @paused.receive
          @log.debug { "Queue unpaused" }
        when @to_deliver.send(sp)
          @log.debug { "Delivered SP" }
          @ready.shift == sp || raise "didnt shift the SP we expected"
          if @requeued.includes? sp
            @redeliver_count += 1
          else
            @deliver_count += 1
          end
        when @refresh_ttl_timeout.receive
          @log.debug { "Queue#deliver_or_expire Refresh TTL timeout" }
        when timeout ttl
          return true if @state.closed?
          case ttl
          when q_ttl
            expire_queue && return false
          when m_ttl
            expire_messages
          else raise "Unknown TTL"
          end
        end
      else
        select
        when @paused.receive
          @log.debug { "Queue unpaused" }
        when @to_deliver.send(sp)
          @log.debug { "Delivered SP #{sp}" }
          @ready.shift == sp || raise "didnt shift the SP we expected"
          if @requeued.includes? sp
            @redeliver_count += 1
          else
            @deliver_count += 1
          end
        end
      end
      true
    end

    def close : Bool
      return false if @closed
      @closed = true
      @state = QueueState::Closed
      @message_available.close
      @consumers_empty.close
      @consumers.cancel_consumers
      @consumers.clear
      @to_deliver.close
      # TODO: When closing due to ReadError, queue is deleted if exclusive
      delete if @exclusive
      Fiber.yield
      notify_observers(:close)
      @log.info { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      @state = QueueState::Deleted
      @vhost.delete_queue(@name)
      @vhost.trigger_gc!
      @log.info { "(messages=#{message_count}) Deleted" }
      notify_observers(:delete)
      true
    end

    def details_tuple
      {
        name:                        @name,
        durable:                     @durable,
        exclusive:                   @exclusive,
        auto_delete:                 @auto_delete,
        arguments:                   @arguments,
        consumers:                   @consumers.size,
        vhost:                       @vhost.name,
        messages:                    @ready.size + @unacked.size,
        messages_persistent:         @durable ? @ready.size + @unacked.size : 0,
        ready:                       @ready.size,
        ready_bytes:                 @ready.bytesize,
        ready_avg_bytes:             @ready.avg_bytesize,
        unacked:                     @unacked.size,
        unacked_bytes:               @unacked.bytesize,
        unacked_avg_bytes:           @unacked.avg_bytesize,
        operator_policy:             @operator_policy.try &.name,
        policy:                      @policy.try &.name,
        exclusive_consumer_tag:      @exclusive ? @consumers.first?.try(&.tag) : nil,
        state:                       @state.to_s,
        effective_policy_definition: Policy.merge_definitions(@policy, @operator_policy),
        message_stats:               stats_details,
        internal:                    @internal,
      }
    end

    def size_details_tuple
      {
        messages:          @ready.size + @unacked.size,
        ready:             @ready.size,
        ready_bytes:       @ready.bytesize,
        ready_avg_bytes:   @ready.avg_bytesize,
        ready_max_bytes:   @ready.max_bytesize &.bytesize,
        ready_min_bytes:   @ready.min_bytesize &.bytesize,
        unacked:           @unacked.size,
        unacked_bytes:     @unacked.bytesize,
        unacked_avg_bytes: @unacked.avg_bytesize,
        unacked_max_bytes: @unacked.max_bytesize &.sp.bytesize,
        unacked_min_bytes: @unacked.min_bytesize &.sp.bytesize,
      }
    end

    class RejectOverFlow < Exception; end

    class Closed < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @state.closed?
      # @log.debug { "Enqueuing message sp=#{sp}" }
      reject_on_overflow(sp)
      was_empty = @ready.push(sp) == 1
      drop_overflow unless immediate_delivery?
      @publish_count += 1
      if was_empty
        message_available
      elsif sp.expiration_ts > 0
        refresh_ttl_timeout
      end
      # @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} consumers=#{@consumers.size}" }
      true
    rescue Channel::ClosedError
      # if message_availabe channel is closed then abort
      false
    end

    private def reject_on_overflow(sp : SegmentPosition) : Nil
      return unless @reject_on_overflow
      if ml = @max_length
        if @ready.size >= ml
          @log.debug { "Overflow reject message sp=#{sp}" }
          raise RejectOverFlow.new
        end
      end

      if mlb = @max_length_bytes
        if @ready.bytesize + sp.bytesize >= mlb
          @log.debug { "Overflow reject message sp=#{sp}" }
          raise RejectOverFlow.new
        end
      end
    end

    private def drop_overflow : Nil
      if ml = @max_length
        @ready.limit_size(ml) do |sp|
          @log.debug { "Overflow drop head sp=#{sp}" }
          expire_msg(sp, :maxlen)
        end
      end

      if mlb = @max_length_bytes
        @ready.limit_byte_size(mlb) do |sp|
          @log.debug { "Overflow drop head sp=#{sp}" }
          expire_msg(sp, :maxlenbytes)
        end
      end
    end

    @segment_id = 0_u32
    @segment_file : MFile?

    private def segment_file(id : UInt32) : MFile
      return @segment_file.not_nil! if @segment_id == id
      mfile = @vhost.segment_file(id)
      @segment_id = id
      @segment_file = mfile
    end

    def metadata(sp) : MessageMetadata?
      seg = segment_file(sp.segment)
      bytes = seg.to_slice(sp.position.to_i32, sp.bytesize)
      MessageMetadata.from_bytes(bytes)
    rescue e : KeyError
      @log.error { "Segment file not found for #{sp}, removing segment position" }
      @ready.delete(sp)
      delete_message sp
      nil
    end

    private def time_to_message_expiration : Time::Span?
      sp = @ready.first? || return
      @log.debug { "Checking if message #{sp} has to be expired" }
      if expire_at = expire_at(sp)
        expire_in = expire_at - RoughTime.utc.to_unix_ms
        if expire_in > 0
          expire_in.milliseconds
        else
          Time::Span.zero
        end
      end
    end

    private def expire_at(sp : SegmentPosition) : Int64?
      if message_ttl = @message_ttl
        meta = metadata(sp)
        return nil unless meta
        expire_at = meta.timestamp + message_ttl
        if sp.expiration_ts > 0
          Math.min(expire_at, sp.expiration_ts)
        else
          expire_at
        end
      elsif sp.expiration_ts > 0
        sp.expiration_ts
      else
        nil
      end
    end

    private def expire_messages : Nil
      i = 0
      now = RoughTime.utc.to_unix_ms
      @ready.shift do |sp|
        @log.debug { "Checking if next message #{sp} has to be expired" }
        if expire_at = expire_at(sp)
          expire_in = expire_at - now
          @log.debug { "expire sp=#{sp} expire_in=#{expire_in}" }
          if expire_in <= 0
            expire_msg(sp, :expired)
            if (i += 1) == 8192
              Fiber.yield
              i = 0
            end
            true
          else
            false
          end
        else
          false
        end
      end
      @log.info { "Expired #{i} messages" } if i > 0
    end

    private def expire_msg(sp : SegmentPosition, reason : Symbol)
      if sp.flags.has_dlx? || @dlx
        env = read(sp)
        expire_msg(env, reason)
      else
        delete_message sp
      end
    end

    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      if sp.flags.has_dlx? || @dlx
        dlx = msg.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
        if dlx
          unless dead_letter_loop?(msg.properties.headers, reason)
            dlrk = msg.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || msg.routing_key
            props = handle_dlx_header(msg, reason)
            dead_letter_msg(msg, sp, props, dlx, dlrk)
          end
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

    private def handle_dlx_header(meta, reason)
      props = meta.properties
      headers = props.headers ||= AMQP::Table.new

      headers.delete("x-delay")
      headers.delete("x-dead-letter-exchange")
      headers.delete("x-dead-letter-routing-key")

      # there's a performance advantage to do `has_key?` over `||=`
      headers["x-first-death-reason"] = reason.to_s unless headers.has_key? "x-first-death-reason"
      headers["x-first-death-queue"] = @name unless headers.has_key? "x-first-death-queue"
      headers["x-first-death-exchange"] = meta.exchange_name unless headers.has_key? "x-first-death-exchange"

      routing_keys = [meta.routing_key.as(AMQP::Field)]
      if cc = headers.delete("CC")
        # should route to all the CC RKs but then delete them,
        # so we (ab)use the BCC header for that
        headers["BCC"] = cc
        routing_keys.concat cc.as(Array(AMQP::Field))
      end

      handle_xdeath_header(headers, meta, routing_keys, reason)

      props.expiration = nil if props.expiration
      props
    end

    private def handle_xdeath_header(headers, meta, routing_keys, reason)
      props = meta.properties
      xdeaths = headers["x-death"]?.as?(Array(AMQP::Field)) || Array(AMQP::Field).new(1)

      found_at = -1
      xdeaths.each_with_index do |xd, idx|
        xd = xd.as(AMQP::Table)
        next if xd["queue"]? != @name
        next if xd["reason"]? != reason.to_s
        next if xd["exchange"]? != meta.exchange_name

        count = xd["count"].as?(Int) || 0
        xd["count"] = count + 1
        xd["time"] = RoughTime.utc
        xd["routing_keys"] = routing_keys
        xd["original-expiration"] = props.expiration if props.expiration
        found_at = idx
        break
      end

      case found_at
      when -1
        # not found so inserting new x-death
        xd = AMQP::Table.new
        xd["queue"] = @name
        xd["reason"] = reason.to_s
        xd["exchange"] = meta.exchange_name
        xd["count"] = 1
        xd["time"] = RoughTime.utc
        xd["routing-keys"] = routing_keys
        xd["original-expiration"] = props.expiration if props.expiration
        xdeaths.unshift xd
      when 0
        # do nothing, updated xd is in the front
      else
        # move updated xd to the front
        xd = xdeaths.delete_at(found_at)
        xdeaths.unshift xd.as(AMQP::Table)
      end
      headers["x-death"] = xdeaths
      nil
    end

    private def dead_letter_msg(msg : BytesMessage, sp, props, dlx, dlrk)
      @log.debug { "Dead lettering #{sp}, ex=#{dlx} rk=#{dlrk} body_size=#{msg.size} props=#{props}" }
      @vhost.publish Message.new(msg.timestamp, dlx.to_s, dlrk.to_s,
        props, msg.size, IO::Memory.new(msg.body))
    end

    private def expire_queue(now = Time.monotonic) : Bool
      @log.debug { "Trying to expire queue" }
      return false unless @consumers.empty?
      @log.debug { "Queue expired" }
      @vhost.delete_queue(@name)
      true
    end

    def basic_get(no_ack, force = false, &blk : Envelope -> Nil) : Bool
      return false if !@state.running? && (@state.paused? && !force)
      @last_get_time = Time.monotonic
      @get_count += 1
      get(no_ack) do |env|
        yield env
        # ack/unack the message after it has been delivered
        if no_ack
          delete_message(env.segment_position)
        else
          @unacked.push(env.segment_position, nil)
        end
        return true
      end
      false
    end

    # If nil is returned it means that the delivery limit is reached
    def get_msg(sp, no_ack) : Envelope?
      env = read(sp)
      env = with_delivery_count_header(env) if @delivery_limit && !no_ack
      env
    end

    # return the next message in the ready queue
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(no_ack : Bool, &blk : Envelope -> _)
      return nil if @state.closed?
      if sp = @ready.shift?
        begin
          env = read(sp)
          env = with_delivery_count_header(env) if @delivery_limit && !no_ack
          return nil unless env
          return yield env
        rescue ReadError
          @ready.insert(sp)
          close
        rescue ex
          @ready.insert(sp)
          raise ex
        end
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

    def read(sp : SegmentPosition) : Envelope
      seg = segment_file(sp.segment)
      bytes = seg.to_slice(sp.position.to_i32, sp.bytesize)
      msg = BytesMessage.from_bytes(bytes)
      redelivered = @requeued.includes?(sp)
      Envelope.new(sp, msg, redelivered)
    rescue ex : KeyError
      @log.error { "Segment file not found for #{sp}, removing segment position" }
      @ready.delete(sp)
      delete_message sp
      raise ex
    rescue ex
      @log.error { "Error reading message #{sp}, possible message loss. #{ex.inspect}" }
      raise ReadError.new(cause: ex)
    ensure
      @requeued.delete(sp) if redelivered
    end

    def ack(sp : SegmentPosition) : Nil
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @ack_count += 1
      @unacked.delete(sp)
      delete_message(sp)
    end

    protected def delete_message(sp : SegmentPosition) : Nil
      @deliveries.delete(sp) if @delivery_limit
      @vhost.dirty = true
    end

    def compact
      ready = @ready
      if ready.capacity > 1024 && ready.capacity > ready.size * 2
        elapsed = Time.measure do
          ready.compact
        end
        @log.info { "Compacting ready queue took #{elapsed.total_milliseconds} ms" }
      end
      unacked = @unacked
      if unacked.capacity > 1024 && unacked.capacity > unacked.size * 2
        elapsed = Time.measure do
          unacked.compact
        end
        @log.info { "Compacting unacked queue took #{elapsed.total_milliseconds} ms" }
      end
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted || @closed
      @log.debug { "Rejecting #{sp}, requeue: #{requeue}" }
      @unacked.delete(sp)
      if requeue
        was_empty = @ready.insert(sp) == 1
        @requeued << sp
        drop_overflow if @consumers.empty?
        message_available if was_empty
      else
        expire_msg(sp, :rejected)
      end
      @reject_count += 1
    end

    private def requeue_many(sps : Enumerable(SegmentPosition))
      return if @deleted
      return if sps.empty?
      @log.debug { "Returning #{sps.size} msgs to ready state" }
      @reject_count += sps.size
      was_empty = @ready.insert(sps) == sps.size
      drop_overflow if @consumers.empty?
      message_available if was_empty
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers.add_consumer(consumer)
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer, basic_cancel = false)
      deleted = @consumers.delete_consumer consumer
      consumer_unacked_size = @unacked.sum { |u| u.consumer == consumer ? 1 : 0 }
      unless basic_cancel
        requeue_many(@unacked.delete(consumer))
      end
      if deleted
        @exclusive_consumer = false if consumer.exclusive
        @log.debug { "Removing consumer with #{consumer_unacked_size} \
                      unacked messages \
                      (#{@consumers.size} consumers left)" }
        notify_observers(:rm_consumer, consumer)
        consumers_empty if @consumers.empty?
        delete if @consumers.empty? && @auto_delete
      end
    end

    def purge_and_close_consumers : UInt32
      # closing all channels will move all unacked back into ready queue
      # so we are purging all messages from the queue, not only ready
      @consumers.each(&.channel.close)
      count = @unacked.purge + purge(nil, trigger_gc: false)
      count.to_u32
    end

    def purge(max_count : Int? = nil, trigger_gc = true) : UInt32
      @log.info { "Purging at most #{max_count || "all"} messages" }
      delete_count = 0_u32
      if max_count.nil? || max_count >= @ready.size
        delete_count += @ready.purge
      else
        max_count.times { @ready.shift? && (delete_count += 1) }
      end
      @log.info { "Purged #{delete_count} messages" }
      @vhost.trigger_gc! if trigger_gc
      delete_count
    end

    def match?(frame)
      @durable == frame.durable &&
        @exclusive == frame.exclusive &&
        @auto_delete == frame.auto_delete &&
        @arguments == frame.arguments.to_h
    end

    def match?(durable, exclusive, auto_delete, arguments)
      @durable == durable &&
        @exclusive == exclusive &&
        @auto_delete == auto_delete &&
        @arguments == arguments.to_h
    end

    def in_use?
      !(empty? && @consumers.empty?)
    end

    def fsync_enq
    end

    def fsync_ack
    end

    def to_json(builder : JSON::Builder, limit : Int32 = -1)
      builder.object do
        details_tuple.each do |k, v|
          builder.field(k, v) unless v.nil?
        end
        builder.field("consumer_details") do
          builder.array do
            @consumers.each do |c|
              c.to_json(builder)
              limit -= 1
              break if limit.zero?
            end
          end
        end
      end
    end

    def size_details_to_json(builder : JSON::Builder, limit : Int32 = -1)
      builder.object do
        size_details_tuple.each do |k, v|
          builder.field(k, v) unless v.nil?
        end
      end
    end

    class Error < Exception; end

    class ReadError < Exception; end
  end
end
