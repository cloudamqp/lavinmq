require "digest/sha1"
require "../mfile"
require "../segment_position"
require "../policy"
require "../observable"
require "../stats"
require "../sortable_json"
require "./ready"
require "../client/channel"
require "../message"
require "../error"

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
    @message_ttl : Int64?
    @max_length : Int64?
    @max_length_bytes : Int64?
    @expires : Int64?
    @delivery_limit : Int64?
    @dlx : String?
    @dlrk : String?
    @reject_on_overflow = false
    @exclusive_consumer = false
    @requeued = Set(SegmentPosition).new
    @deliveries = Hash(SegmentPosition, Int32).new
    @consumers = Array(Client::Channel::Consumer).new
    @message_ttl_change = Channel(Nil).new
    @ready = ReadyQueue.new

    getter unacked_count = 0u32
    getter unacked_bytesize = 0u64
    @unacked_lock = Mutex.new(:unchecked)

    getter? paused = false
    getter paused_change = Channel(Bool).new

    @consumers_empty_change = Channel(Bool).new

    private def queue_expire_loop
      loop do
        if @consumers.empty? && (ttl = queue_expiration_ttl)
          @log.debug { "Queue expires in #{ttl}" }
          select
          when @queue_expiration_ttl_change.receive
          when @consumers_empty_change.receive
          when timeout ttl
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
        if @ready.empty?
          @ready.empty_change.receive
        else
          if @consumers.empty?
            if ttl = time_to_message_expiration
              select
              when @message_ttl_change.receive
              when @ready.empty_change.receive # might be empty now (from basic get)
              when @consumers_empty_change.receive
              when timeout ttl
                expire_messages
              end
            else
              # first message in queue should not be expired
              # wait for empty queue or TTL change
              select
              when @message_ttl_change.receive
              when @ready.empty_change.receive
              end
            end
          else
            @consumers_empty_change.receive
          end
        end
      rescue ::Channel::ClosedError
        break
      end
    end

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(
      {"ack", "deliver", "confirm", "get", "get_no_ack", "publish", "redeliver", "reject", "return_unroutable"},
      {"message_count", "unacked_count"})

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers, ready, last_get_time
    getter policy : Policy?
    getter operator_policy : OperatorPolicy?
    getter? closed
    property? internal = false
    getter state = QueueState::Running

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @last_get_time = RoughTime.monotonic
      @log = Log.for "queue[vhost=#{@vhost.name} name=#{@name}]"
      handle_arguments
      spawn queue_expire_loop, name: "Queue#queue_expire_loop #{@vhost.name}/#{@name}"
      spawn message_expire_loop, name: "Queue#message_expire_loop #{@vhost.name}/#{@name}"
      if @internal
        spawn expire_loop, name: "Queue#expire_loop #{@vhost.name}/#{@name}"
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
      @last_get_time = RoughTime.monotonic
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
            @last_get_time = RoughTime.monotonic
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
        else nil
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
      @consumers.any? &.accepts?
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
          when @message_ttl_change.receive
            @log.debug { "Queue#expire_loop Message TTL changed" }
          when timeout ttl
            expire_messages
          end
        else
          @ready.empty_change.receive
        end
      rescue ::Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in expire_loop: #{ex.inspect_with_backtrace}" }
      end
    end

    def pause!
      return unless @state.running?
      @state = QueueState::Paused
      @log.debug { "Paused" }
      @paused = true
      while @paused_change.try_send? true
      end
    end

    def resume!
      return unless @state.paused?
      @state = QueueState::Running
      @log.debug { "Resuming" }
      @paused = false
      while @paused_change.try_send? false
      end
    end

    @queue_expiration_ttl_change = Channel(Nil).new

    private def queue_expiration_ttl : Time::Span?
      if e = @expires
        expires_in = @last_get_time + e.milliseconds - RoughTime.monotonic
        if expires_in > Time::Span.zero
          expires_in
        else
          Time::Span.zero
        end
      end
    end

    def close : Bool
      return false if @closed
      @closed = true
      @state = QueueState::Closed
      @queue_expiration_ttl_change.close
      @message_ttl_change.close
      @consumers_empty_change.close
      @consumers.each &.cancel
      @consumers.clear
      @to_deliver.close
      @paused_change.close
      @ready.close
      # TODO: When closing due to ReadError, queue is deleted if exclusive
      delete if @exclusive
      Fiber.yield
      notify_observers(:close)
      @log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      @state = QueueState::Deleted
      vhost = @vhost
      vhost.delete_queue(@name)
      @ready.each do |sp|
        vhost.decrease_segment_references(sp.segment)
      end
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
        messages:                    @ready.size + @unacked_count,
        messages_persistent:         @durable ? @ready.size + @unacked_count : 0,
        ready:                       @ready.size,
        ready_bytes:                 @ready.bytesize,
        ready_avg_bytes:             @ready.avg_bytesize,
        unacked:                     @unacked_count,
        unacked_bytes:               @unacked_bytesize,
        unacked_avg_bytes:           unacked_avg_bytes,
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
        messages:          @ready.size + @unacked_count,
        ready:             @ready.size,
        ready_bytes:       @ready.bytesize,
        ready_avg_bytes:   @ready.avg_bytesize,
        ready_max_bytes:   @ready.max_bytesize &.bytesize,
        ready_min_bytes:   @ready.min_bytesize &.bytesize,
        unacked:           @unacked_count,
        unacked_bytes:     @unacked_bytesize,
        unacked_avg_bytes: unacked_avg_bytes,
        unacked_max_bytes: 0,
        unacked_min_bytes: 0,
      }
    end

    private def unacked_avg_bytes : UInt64
      return 0u64 if @unacked_count.zero?
      @unacked_bytesize // @unacked_count
    end

    class RejectOverFlow < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @state.closed?
      # @log.debug { "Enqueuing message sp=#{sp}" }
      reject_on_overflow(sp)
      @ready.push(sp)
      drop_overflow unless immediate_delivery?
      @publish_count += 1
      @vhost.increase_segment_references(sp.segment)
      # @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} consumers=#{@consumers.size}" }
      true
    rescue ::Channel::ClosedError
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
        expire_in = expire_at - RoughTime.unix_ms
        if expire_in > 0
          expire_in.milliseconds
        else
          Time::Span.zero
        end
      end
    end

    private def has_expired?(sp, requeue = false) : Bool
      if ttl = @message_ttl
        ttl = Math.min(ttl, sp.ttl) if sp.flags.has_ttl?
        return false if ttl.zero? && !requeue && !@consumers.empty?
        sp.timestamp + ttl <= RoughTime.unix_ms
      elsif sp.flags.has_ttl?
        return false if sp.ttl.zero? && !requeue && !@consumers.empty?
        sp.timestamp + sp.ttl <= RoughTime.unix_ms
      else
        false
      end
    end

    private def expire_at(sp : SegmentPosition) : Int64?
      if ttl = @message_ttl
        ttl = Math.min(ttl, sp.ttl) if sp.flags.has_ttl?
        sp.timestamp + ttl
      elsif sp.flags.has_ttl?
        sp.timestamp + sp.ttl
      else
        nil
      end
    end

    private def expire_messages : Nil
      i = 0
      @ready.shift do |sp|
        @log.debug { "Checking if next message #{sp} has to be expired" }
        if expire_at = expire_at(sp)
          expire_in = expire_at - RoughTime.unix_ms
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
      @log.debug { "Expiring #{sp} (#{sp.flags}) now due to #{reason}, headers=#{msg.properties.headers}" }
      if sp.flags.has_dlx? || @dlx
        if dlx = msg.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
          if dead_letter_loop?(msg.properties.headers, reason)
            @log.debug { "#{sp} in a dead letter loop, dropping it" }
          else
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

    private def expire_queue : Bool
      @log.debug { "Trying to expire queue" }
      return false unless @consumers.empty?
      @log.debug { "Queue expired" }
      @vhost.delete_queue(@name)
      true
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      return false if !@state.running? && (@state.paused? && !force)
      @last_get_time = RoughTime.monotonic
      @queue_expiration_ttl_change.try_send? nil
      @get_count += 1
      get(no_ack) do |env|
        yield env
      end
    end

    # If nil is returned it means that the delivery limit is reached
    def get_msg(consumer : Client::Channel::Consumer, &) : Bool
      get(consumer.no_ack) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(no_ack : Bool, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      while sp = @ready.shift? # retry if msg expired or deliver limit hit
        begin
          if has_expired?(sp) # guarantee to not deliver expired messages
            expire_msg(sp, :expired)
            next
          end
          env = read(sp)
          if @delivery_limit && !no_ack
            env = with_delivery_count_header(env)
          end
          if env
            unless no_ack
              @log.debug { "Counting as unacked: #{sp}" }
              @unacked_lock.synchronize do
                @unacked_count += 1
                @unacked_bytesize += sp.bytesize
              end
            end
            yield env # deliver the message
            if no_ack
              @log.debug { "Deleting: #{sp}" }
              delete_message(sp)
            end
            return true
          end
        rescue ex
          @ready.insert(sp)
          unless no_ack
            @log.debug { "Not counting as unacked: #{sp}" }
            @unacked_lock.synchronize do
              @unacked_count -= 1
              @unacked_bytesize -= sp.bytesize
            end
          end
          if ex.is_a? ReadError
            close
            return false
          end
          raise ex
        end
      end
      false
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
      @log.error(exception: ex) { "Error reading message #{sp.inspect}, possible message loss." }
      raise ReadError.new(cause: ex)
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
      @deliveries.delete(sp) if @delivery_limit
      @requeued.delete(sp)
      @vhost.decrease_segment_references(sp.segment)
    end

    def compact
      ready = @ready
      if ready.capacity > 1024 && ready.capacity > ready.size * 2
        elapsed = Time.measure do
          ready.compact
        end
        @log.info { "Compacting ready queue took #{elapsed.total_milliseconds} ms" }
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
          @ready.insert(sp)
          @requeued << sp
          drop_overflow if @consumers.empty?
        end
      else
        expire_msg(sp, :rejected)
      end
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = RoughTime.monotonic
      was_empty = @consumers.empty?
      @consumers << consumer
      notify_consumers_empty(false) if was_empty
      @exclusive_consumer = true if consumer.exclusive
      @has_priority_consumers = true unless consumer.priority.zero?
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    getter? has_priority_consumers = false

    def rm_consumer(consumer : Client::Channel::Consumer, basic_cancel = false)
      return if @closed
      deleted = @consumers.delete consumer
      @has_priority_consumers = @consumers.any? { |c| !c.priority.zero? }
      if deleted
        @exclusive_consumer = false if consumer.exclusive
        @log.debug { "Removing consumer with #{consumer.unacked} \
                      unacked messages \
                      (#{@consumers.size} consumers left)" }
        notify_observers(:rm_consumer, consumer)
        if @consumers.empty?
          notify_consumers_empty(true)
          delete if @auto_delete
        end
      end
    end

    private def notify_consumers_empty(is_empty)
      while @consumers_empty_change.try_send? is_empty
      end
    end

    def purge_and_close_consumers : UInt32
      # closing all channels will move all unacked back into ready queue
      # so we are purging all messages from the queue, not only ready
      @consumers.each(&.channel.close)
      count = purge(nil, trigger_gc: false)
      notify_consumers_empty(true)
      count.to_u32
    end

    def purge(max_count : Int? = nil, trigger_gc = true) : UInt32
      @log.info { "Purging at most #{max_count || "all"} messages" }
      delete_count = 0_u32
      if max_count.nil? || max_count >= @ready.size
        @ready.each do |sp|
          vhost.decrease_segment_references(sp.segment)
        end
        delete_count = @ready.purge
      else
        max_count.times { (sp = @ready.shift?) && (delete_count += 1) && vhost.decrease_segment_references(sp.segment) }
      end
      @log.info { "Purged #{delete_count} messages" }
      delete_count.to_u32
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

    class ClosedError < Error; end
  end
end
