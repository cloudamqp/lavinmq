require "logger"
require "digest/sha1"
require "./segment_position"
require "./policy"
require "./observable"
require "./stats"
require "./sortable_json"
require "./reference_counter"

module AvalancheMQ
  class Queue
    include PolicyTarget
    include Observable
    include Stats
    include SortableJSON

    alias ArgumentNumber = UInt16 | Int32 | Int64

    @durable = false
    @log : Logger
    @message_ttl : ArgumentNumber?
    @max_length : ArgumentNumber?
    @expires : ArgumentNumber?
    @delivery_limit : ArgumentNumber?
    @dlx : String?
    @dlrk : String?
    @reject_on_overflow = false
    @closed = false
    @deleted = false
    @exclusive_consumer = false
    @requeued = Set(SegmentPosition).new
    @deliveries = Hash(SegmentPosition, Int32).new
    @get_unacked = Deque(SegmentPosition).new
    @segment_ref_count = ReferenceCounter(UInt32).new

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(%w(ack deliver get publish redeliver reject), %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, vhost, consumers
    getter policy : Policy?
    getter? closed

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @log = @vhost.log.dup
      @log.progname += " queue=#{@name}"
      handle_arguments
      @consumers = Deque(Client::Channel::Consumer).new
      @consumers_lock = Mutex.new
      @message_available = Channel(Nil).new
      @consumer_available = Channel(Nil).new(1)
      @ready = Deque(SegmentPosition).new(1024)
      @ready_lock = Mutex.new
      @segment_pos = Hash(UInt32, UInt32).new { 0_u32 }
      @segments = Hash(UInt32, File).new do |h, seg|
        path = File.join(@vhost.data_dir, "msgs.#{seg.to_s.rjust(10, '0')}")
        h[seg] = File.open(path, "r").tap { |f| f.buffer_size = Config.instance.file_buffer_size }
      end
      @last_get_time = Time.monotonic
      spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << " @vhost=" << @vhost.name << ">"
    end

    def self.generate_name
      "amq.gen-#{Random::Secure.urlsafe_base64(24)}"
    end

    def redeclare
      @last_get_time = Time.monotonic
    end

    def has_exclusive_consumer?
      @exclusive_consumer
    end

    def apply_policy(policy : Policy)
      clear_policy
      policy.not_nil!.definition.each do |k, v|
        @log.debug { "Applying policy #{k}: #{v}" }
        case k
        when "max-length"
          @max_length = v.as_i64
          drop_overflow
        when "message-ttl"
          @message_ttl = v.as_i64
        when "overflow"
          @reject_on_overflow = v.as_s == "reject-publish"
        when "expires"
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
        end
      end
      @policy = policy
    end

    def clear_policy
      handle_arguments
      @policy = nil
      @vhost.upstreams.try &.stop_link(self)
    end

    def unacked_count : UInt32
      count = 0_u32
      @consumers_lock.synchronize do
        @consumers.each do |c|
          count += c.unacked.size
        end
      end
      count + @get_unacked.size
    end

    def message_available
      select
      when @message_available.send nil
      else
      end
    end

    def consumer_available
      select
      when @consumer_available.send nil
      else
      end
    end

    private def drop_overflow
      while @ready.size > @max_length.not_nil!
        drophead
      end
    end

    private def handle_arguments
      message_ttl = @arguments["x-message-ttl"]?
      @message_ttl = message_ttl if message_ttl.is_a? ArgumentNumber
      expires = @arguments["x-expires"]?
      @expires = expires if expires.is_a? ArgumentNumber
      @dlx = @arguments["x-dead-letter-exchange"]?.try &.to_s
      @dlrk = @arguments["x-dead-letter-routing-key"]?.try &.to_s
      max_length = @arguments["x-max-length"]?
      @max_length = max_length if max_length.is_a? ArgumentNumber
      delivery_limit = @arguments["x-delivery-limit"]?
      @delivery_limit = delivery_limit if delivery_limit.is_a? ArgumentNumber
      @reject_on_overflow = @arguments.fetch("x-overflow", "").to_s == "reject-publish"
    end

    def immediate_delivery?
      @consumers_lock.synchronize do
        @consumers.any? { |c| c.accepts? }
      end
    end

    def message_count : UInt32
      @ready.size.to_u32
    end

    def empty? : Bool
      @ready.size.zero?
    end

    def consumer_count : UInt32
      @consumers.size.to_u32
    end

    def referenced_segments(s : Set(UInt32))
      @segment_ref_count.each do |seg, count|
        if count.zero?
          if f = @segments.delete(seg)
            @log.debug { "Closing non referenced segment #{seg}" }
            f.close
          end
        else
          s << seg
        end
      end
      @ready_lock.synchronize { @segment_ref_count.gc! }
    end

    private def deliver_loop
      loop do
        break if @closed
        if @ready.empty?
          recieve_or_expire || break
        end
        if c = find_consumer
          deliver_to_consumer(c)
        else
          break if @closed
          consumer_or_expire || break
        end
      rescue Channel::ClosedError
        @log.debug "Delivery loop channel closed"
        break
      rescue ex
        @log.error { "Unexpected exception in deliver_loop: #{ex.inspect_with_backtrace}" }
      end
      @log.debug "Exiting delivery loop"
    end

    private def schedule_expiration_of_queue
      if @expires
        now = Time.monotonic
        if queue_expires_in = expires_in(now)
          if queue_expires_in <= Time::Span.zero
            expire_queue(now)
            return true
          else
            spawn(name: "expire_queue_later #{@name}") do
              sleep queue_expires_in.not_nil!
              expire_queue
            end
          end
        end
      end
      false
    end

    private def recieve_or_expire
      schedule_expiration_of_queue && return false
      @log.debug { "Waiting for msgs" }
      @message_available.receive
      @log.debug { "Message available" }
      true
    end

    private def consumer_or_expire
      @log.debug "No consumer available"
      schedule_expiration_of_queue && return false
      schedule_expiration_of_next_msg && return true
      @log.debug "Waiting for consumer"
      @consumer_available.receive
      @log.debug "Consumer available"
      true
    end

    private def wakeup_in(queue_expires_in)
      msg_expires_in = expire_message
      if queue_expires_in && msg_expires_in
        if queue_expires_in < msg_expires_in
          queue_expires_in
        else
          msg_expires_in
        end
      elsif queue_expires_in && msg_expires_in.nil?
        queue_expires_in
      elsif msg_expires_in && queue_expires_in.nil?
        msg_expires_in
      else
        nil
      end
    end

    private def find_consumer
      @log.debug { "Looking for available consumers" }
      case @consumers.size
      when 0
        nil
      when 1
        c = @consumers[0]
        c.accepts? ? c : nil
      else
        @consumers_lock.synchronize do
          @consumers.size.times do
            c = @consumers.shift
            @consumers.push c
            return c if c.accepts?
          end
        end
        nil
      end
    end

    private def deliver_to_consumer(c)
      @log.debug { "Getting a new message" }
      get(c.no_ack) do |env|
        if env
          sp = env.segment_position
          @log.debug { "Delivering #{sp} to consumer" }
          if c.deliver(env.message, sp, env.redelivered)
            if env.redelivered
              @redeliver_count += 1
            else
              @deliver_count += 1
            end
            @log.debug { "Delivery done" }
          else
            @log.debug { "Delivery failed" }
          end
        else
          @log.debug { "Consumer found, but not a message" }
        end
      end
    end

    def close : Bool
      return false if @closed
      @closed = true
      @message_available.close
      @consumer_available.close
      @consumers_lock.synchronize do
        loop do
          c = @consumers.shift? || break
          c.cancel
        end
      end
      @segments.each_value &.close
      @segments.clear
      @vhost.delete_queue(@name) if @auto_delete || @exclusive
      Fiber.yield
      notify_observers(:close)
      @log.debug { "Closed" }
      true
    end

    def delete : Bool
      return false if @deleted
      @deleted = true
      close
      notify_observers(:delete)
      @log.info { "Deleted" }
      true
    end

    def details_tuple
      unacked = unacked_count
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
        messages: @ready.size + unacked,
        ready: @ready.size,
        unacked: unacked,
        policy: @policy.try &.name,
        exclusive_consumer_tag: @exclusive ? @consumers.first?.try(&.tag) : nil,
        state: @closed ? :closed : :running,
        effective_policy_definition: @policy,
        message_stats: stats_details,
      }
    end

    class RejectOverFlow < Exception; end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      return false if @closed
      @log.debug { "Enqueuing message sp=#{sp}" }
      was_empty = false
      handle_max_length
      @ready_lock.synchronize do
        was_empty = @ready.empty?
        @ready.push sp
        @segment_ref_count.inc(sp.segment)
      end
      @publish_count += 1
      message_available if was_empty
      @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} \
                    consumers=#{@consumers.size}" }
      true
    rescue ex : RejectOverFlow
      @log.debug { "Overflow reject message sp=#{sp}" }
      raise ex
    end

    private def handle_max_length
      if @max_length.try { |ml| @ready.size >= ml }
        @log.debug { "Overflow #{@max_length} #{@reject_on_overflow ? "reject-publish" : "drop-head"}" }
        if @reject_on_overflow
          raise RejectOverFlow.new
        else
          drophead
        end
      end
    end

    private def metadata(sp) : MessageMetadata?
      @read_lock.synchronize do
        seg = @segments[sp.segment]
        if @segment_pos[sp.segment] != sp.position
          @log.debug { "Seeking to #{sp.position}, was at #{@segment_pos[sp.segment]}" }
          seg.seek(sp.position, IO::Seek::Set)
          @segment_pos[sp.segment] = sp.position
        end
        ts = Int64.from_io seg, IO::ByteFormat::NetworkEndian
        ex = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
        rk = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
        pr = AMQP::Properties.from_io seg, IO::ByteFormat::NetworkEndian
        sz = UInt64.from_io seg, IO::ByteFormat::NetworkEndian
        meta = MessageMetadata.new(ts, ex, rk, pr, sz)
        @segment_pos[sp.segment] += meta.bytesize
        meta
      end
    rescue ex : Errno
      @log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      drop(sp, true)
    rescue ex : IO::EOFError
      @log.error { "Could not read metadata for sp=#{sp}" }
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
      nil
    end

    private def schedule_expiration_of_next_msg(now = Time.utc.to_unix_ms) : Bool
      expired_msg = false
      i = 0_u32
      loop do
        sp = @ready_lock.synchronize { @ready[0]? } || break
        @log.debug { "Checking if next message has to be expired" }
        meta = metadata(sp) || break
        @log.debug { "Next message: #{meta}" }
        exp_ms = meta.properties.expiration.try(&.to_i64?) || @message_ttl
        if exp_ms
          expire_at = meta.timestamp + exp_ms
          expire_in = expire_at - now
          if expire_in <= 0
            @ready_lock.synchronize do
              @ready.shift
              @segment_ref_count.dec(sp.segment)
            end
            expired_msg = true
            expire_msg(meta, sp, :expired)
            Fiber.yield if (i += 1_u32) % 8192_u32 == 0_u32
          else
            spawn(expire_later(expire_in, meta, sp),
                  name: "Queue#expire_later(#{expire_in}) #{@vhost.name}/#{@name}")
            break
          end
        else
          @log.debug { "No message to expire" }
          break
        end
      end
      expired_msg
    end

    private def expire_later(expire_in, meta, sp)
      @log.debug { "Expiring #{sp} in #{expire_in}ms" }
      sleep expire_in.milliseconds if expire_in > 0
      return if @closed
      @ready_lock.synchronize do
        return unless @ready[0]? == sp
        @ready.shift
        @segment_ref_count.dec(sp.segment)
      end
      expire_msg(meta, sp, :expired)
    end

    def expire_msg(sp : SegmentPosition, reason : Symbol)
      meta = metadata(sp) || return
      expire_msg(meta, sp, reason)
    end

    private def expire_msg(meta : MessageMetadata, sp : SegmentPosition, reason : Symbol)
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      dlx = meta.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
      if dlx
        dlrk = meta.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || meta.routing_key
        props = handle_dlx_header(meta, reason)
        dead_letter_msg(meta, sp, props, dlx, dlrk)
      end
      drop sp, false
    rescue ex : IO::EOFError
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
      raise ex
    end

    private def handle_dlx_header(meta, reason)
      props = meta.properties
      headers = (props.headers ||= AMQP::Table.new)
      headers.delete("x-dead-letter-exchange")
      headers.delete("x-dead-letter-routing-key")

      xdeaths = Array(AMQP::Table).new(1)
      if headers.has_key? "x-death"
        headers["x-death"].as?(Array(AMQP::Field)).try &.each do |tbl|
          xdeaths << tbl.as(AMQP::Table)
        end
      end
      xd = xdeaths.find { |d| d["queue"] == @name && d["reason"] == reason.to_s }
      xdeaths.delete(xd)
      count = xd ? xd["count"].as?(Int32) || 0 : 0
      death = Hash(String, AMQP::Field){
        "exchange"     => meta.exchange_name,
        "queue"        => @name,
        "routing-keys" => [meta.routing_key.as(AMQP::Field)],
        "reason"       => reason.to_s,
        "count"        => count + 1,
        "time"         => Time.utc,
      }
      if props.expiration
        death["original-expiration"] = props.expiration
        props.expiration = nil
      end
      xdeaths.unshift AMQP::Table.new(death)

      headers["x-death"] = xdeaths
      props.headers = headers
      props
    end

    private def dead_letter_msg(meta, sp, props, dlx, dlrk)
      @read_lock.synchronize do
        seg = @segments[sp.segment]
        body_pos = sp.position + meta.bytesize
        if @segment_pos[sp.segment] != body_pos
          @log.debug { "Current pos #{@segment_pos[sp.segment]}" }
          @log.debug { "Message position: #{sp.position}" }
          @log.debug { "Metadata size #{meta.bytesize}" }
          @log.debug { "Seeking to body pos #{body_pos}" }
          seg.seek(body_pos, IO::Seek::Set)
          @segment_pos[sp.segment] = body_pos
        end
        msg = Message.new(meta.timestamp, dlx.to_s, dlrk.to_s, props, meta.size, seg)
        @log.debug { "Dead-lettering #{sp} to exchange \"#{msg.exchange_name}\", routing key \"#{msg.routing_key}\"" }
        @vhost.publish msg
      end
    end

    private def expires_in(now = Time.monotonic) : Time::Span?
      if @expires
        return @last_get_time + @expires.not_nil!.milliseconds - now
      end
    end

    private def expire_queue(now = Time.monotonic) : Bool
      exp = expires_in(now)
      return false if exp.nil?
      return false if exp - 1.second > Time::Span.zero
      return false unless @consumers.empty?
      @log.debug "Expired"
      @vhost.delete_queue(@name)
      true
    end

    private def expire_message(now = Time.utc) : Time::Span?
      loop do
        sp = @ready_lock.synchronize { @ready[0]? } || break
        @log.debug { "Checking if next message has to be expired" }
        meta = metadata(sp) || break
        exp_ms = meta.properties.expiration.try(&.to_i64?) || @message_ttl
        if exp_ms
          expire_at = Time.unix_ms(meta.timestamp) + exp_ms.milliseconds
          expire_in = expire_at - now
          if expire_in <= Time::Span.zero
            @ready_lock.synchronize do
              @ready.shift && @segment_ref_count.dec(sp.segment)
            end
            expire_msg(meta, sp, :expired)
          else
            return expire_in
          end
        else
          @log.debug { "No message to expire" }
          break
        end
      end
      nil
    end

    def basic_get(no_ack, &blk : Envelope? -> Nil)
      @last_get_time = Time.monotonic
      get(no_ack) do |env|
        if env
          @get_count += 1
          @get_unacked << env.segment_position unless no_ack
        end
        yield env
      end
    end

    @read_lock = Mutex.new

    def read_message(sp : SegmentPosition, &blk : Envelope -> Nil)
      @read_lock.synchronize do
        read(sp) do |env|
          yield env
        end
      end
    end

    private def get(no_ack : Bool, &blk : Envelope? -> Nil)
      return yield nil if @closed
      sp = @ready_lock.synchronize do
        @ready.shift?.tap do |v|
          @segment_ref_count.dec(v.segment) if v && no_ack
        end
      end
      return yield nil if sp.nil?
      @read_lock.synchronize do
        read(sp) do |env|
          env = add_delivery_count_header(env) unless no_ack
          yield env
        end
      end
    end

    private def add_delivery_count_header(env)
      sp = env.segment_position
      if @delivery_limit
        headers = env.message.properties.headers || AMQP::Table.new
        delivery_count = @deliveries[sp]? || 0
        @log.debug { "Delivery count: #{delivery_count} Delivery limit: #{@delivery_limit}" }
        if delivery_count >= @delivery_limit.not_nil!
          @deliveries.delete(sp)
          expire_msg(sp, :delivery_limit)
          return nil
        end
        headers["x-delivery-count"] = @deliveries[sp] = delivery_count + 1
        env.message.properties.headers = headers
      end
      env
    end

    private def read(sp : SegmentPosition, &blk : Envelope -> Nil)
      seg = @segments[sp.segment]
      if @segment_pos[sp.segment] != sp.position
        @log.debug { "Seeking to #{sp.position}, was at #{@segment_pos[sp.segment]}" }
        seg.seek(sp.position, IO::Seek::Set)
        @segment_pos[sp.segment] = sp.position
      end
      ts = Int64.from_io seg, IO::ByteFormat::NetworkEndian
      ex = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
      rk = AMQP::ShortString.from_io seg, IO::ByteFormat::NetworkEndian
      pr = AMQP::Properties.from_io seg, IO::ByteFormat::NetworkEndian
      sz = UInt64.from_io seg, IO::ByteFormat::NetworkEndian
      msg = Message.new(ts, ex, rk, pr, sz, seg)
      redelivered = @requeued.includes?(sp)
      yield Envelope.new(sp, msg, redelivered)
      @requeued.delete(sp) if redelivered
      @segment_pos[sp.segment] += msg.bytesize
    rescue ex : IO::EOFError
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
      @log.error { "Could not read sp=#{sp}, rejecting" }
      drop sp, true
    rescue ex : Errno
      @log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      drop sp, true
    rescue ex
      if seg
        @log.error "Error reading message at #{sp}: #{ex.inspect}"
        @log.error "Hexdump of the first 1024 bytes on disk:"
        seg.seek(sp.position, IO::Seek::Set)
        buffer = uninitialized UInt8[1024]
        io = IO::Hexdump.new(seg, output: STDERR, read: true)
        io.read(buffer.to_slice)
      end
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
      raise ex
    end

    def ack(sp : SegmentPosition, persistent : Bool)
      return if @deleted
      @log.debug { "Acking #{sp}" }
      @ready_lock.synchronize do
        @segment_ref_count.dec(sp.segment)
      end
      idx = @get_unacked.index(sp)
      @get_unacked.delete_at(idx) if idx
      @deliveries.delete(sp)
      @ack_count += 1
      Fiber.yield if @ack_count % 8192 == 0
      consumer_available
    end

    private def drop(sp, delete_in_ready, persistent = true) : Nil
      return if @deleted
      @log.debug { "Dropping #{sp}" }
      @ready_lock.synchronize do
        if delete_in_ready
          head = @ready.shift
          if head == sp
            @segment_ref_count.dec(sp.segment)
          else
            @log.debug { "Dropping #{sp} but #{head} was at head" }
            if idx = @ready.index(sp)
              @ready.delete_at(idx)
              @ready.unshift head
              @segment_ref_count.dec(sp.segment)
            else
              @log.error { "Dropping #{sp} but wasn't in ready queue" }
            end
          end
        end
      end
      if idx = @get_unacked.index(sp)
        @get_unacked.delete_at(idx)
      end
      @deliveries.delete(sp)
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted
      @log.debug { "Rejecting #{sp}" }
      idx = @get_unacked.index(sp)
      @get_unacked.delete_at(idx) if idx
      @reject_count += 1
      if requeue
        was_empty = false
        @ready_lock.synchronize do
          was_empty = @ready.empty?
          i = @ready.index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
        end
        @requeued << sp
        message_available if was_empty
      else
        expire_msg(sp, :rejected)
      end
      Fiber.yield if @reject_count % 8192 == 0
    end

    private def requeue_many(sps : Deque(SegmentPosition))
      return if @deleted
      return if sps.empty?
      was_empty = false
      @log.debug { "Returning #{sps.size} msgs to ready state" }
      @reject_count += sps.size
      @ready_lock.synchronize do
        was_empty = @ready.empty?
        sps.reverse_each do |sp|
          i = @ready.index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @requeued << sp
        end
      end
      message_available if was_empty
    end

    private def drophead
      if sp = @ready_lock.synchronize { @ready.shift? }
        @log.debug { "Overflow drop head sp=#{sp}" }
        @segment_ref_count.dec(sp.segment)
        expire_msg(sp, :maxlen)
      end
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers_lock.synchronize do
        @consumers.push consumer
      end
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      consumer_available
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      deleted = @consumers_lock.synchronize { @consumers.delete consumer }
      if deleted
        @exclusive_consumer = false if consumer.exclusive
        requeue_many(consumer.unacked)
        @log.debug { "Removing consumer with #{consumer.unacked.size} unacked messages \
                        (#{@consumers.size} consumers left)" }
        notify_observers(:rm_consumer, consumer)
        delete if @consumers.size == 0 && @auto_delete
      end
    end

    def purge
      purged_count = message_count
      @ready_lock.synchronize { @ready.clear }
      @log.debug { "Purged #{purged_count} messages" }
      purged_count
    end

    def match?(frame)
      @durable == frame.durable &&
        @exclusive == frame.exclusive &&
        @auto_delete == frame.auto_delete &&
        @arguments == frame.arguments.to_h
    end

    def match?(durable, auto_delete, arguments)
      @durable == durable &&
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
  end
end
