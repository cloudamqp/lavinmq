require "logger"
require "digest/sha1"
require "./segment_position"
require "./policy"
require "./observable"
require "./stats"
require "./sortable_json"

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

    # Creates @[x]_count and @[x]_rate and @[y]_log
    rate_stats(%w(ack deliver get publish redeliver reject), %w(message_count unacked_count))

    getter name, durable, exclusive, auto_delete, arguments, policy, vhost, consumers
    getter? closed
    def_equals_and_hash @vhost, @name

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = Hash(String, AMQP::Field).new)
      @log = @vhost.log.dup
      @log.progname += " queue=#{@name}"
      handle_arguments
      @consumers = Deque(Client::Channel::Consumer).new
      @message_available = Channel(Nil).new
      @consumer_available = Channel(Nil).new(1)
      @ready = Deque(SegmentPosition).new(1024)
      @ready_lock = Mutex.new
      @segment_pos = Hash(UInt32, UInt32).new do |h, seg|
        h[seg] = 0_u32
      end
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
      (@consumers.reduce(0) { |memo, c| memo + c.unacked.size } + @get_unacked.size).to_u32
    end

    def consumer_available
      @consumer_available.send nil unless @consumer_available.full?
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
      @consumers.any? { |c| c.accepts? }
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
      @ready.each { |sp| s << sp.segment }
      @consumers.each { |c| c.unacked.each { |sp| s << sp.segment } }
      @segments.delete_if do |seg, f|
        unless s.includes? seg
          @log.debug { "Closing non referenced segment #{seg}" }
          f.close
          next true
        end
        false
      end
    end

    private def deliver_loop
      loop do
        break if @closed
        if @ready.empty?
          @log.debug { "Waiting for msgs" }
          now = Time.monotonic
          queue_expires_in = expires_in(now)
          if queue_expires_in
            if queue_expires_in <= Time::Span.zero
              expire_queue(now)
              break
            else
              select
              when @message_available.receive
              when timeout(queue_expires_in)
                expire_queue(now)
              end
            end
          else
            @message_available.receive
          end
          @log.debug { "Message available" }
        end
        if c = find_consumer
          deliver_to_consumer(c)
        else
          break if @closed
          @log.debug "No consumer available"
          now = Time.monotonic
          queue_expires_in = expires_in(now)
          if queue_expires_in && queue_expires_in <= Time::Span.zero
            expire_queue(now)
            break
          end
          msg_expires_in = expire_message
          wakeup_in =
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

          @log.debug "Waiting for consumer"
          if wakeup_in
            select
            when @consumer_available.receive
              @log.debug "Consumer available"
            when timeout(wakeup_in)
              @log.debug "Consumer wait timeout"
            end
          else
            @consumer_available.receive
          end
        end
      rescue Channel::ClosedError
        @log.debug "Delivery loop channel closed"
        break
      rescue ex : Errno
        sp = @ready_lock.synchronize { @ready.shift }
        @log.error { "Segment #{sp} not found, possible message loss. #{ex.inspect}" }
      rescue ex
        @log.error { "Unexpected exception in deliver_loop: #{ex.inspect_with_backtrace}" }
      end
      @log.debug "Exiting delivery loop"
    end

    private def find_consumer
      @log.debug { "Looking for available consumers" }
      if @consumers.size == 1
        c = @consumers[0]
        return c if c.accepts?
      end
      @consumers.size.times do
        c = @consumers.shift
        @consumers.push c
        return c if c.accepts?
      end
      nil
    end

    private def deliver_to_consumer(c)
      @log.debug { "Getting a new message" }
      get(c.no_ack) do |env|
        if env
          sp = env.segment_position
          msg = env.message
          if !c.no_ack && @delivery_limit
            headers = msg.properties.headers || AMQP::Table.new
            delivery_count = @deliveries[sp]? || 0
            if delivery_count > @delivery_limit.not_nil!
              @deliveries.delete(sp)
              return expire_msg(sp, :delivery_limit)
            end
            headers["x-delivery-count"] = @deliveries[sp] = delivery_count + 1
            msg.properties.headers = headers
          end
          @log.debug { "Delivering #{sp} to consumer" }
          if c.deliver(msg, sp, env.redelivered)
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
      loop do
        c = @consumers.shift? || break
        c.cancel
      end
      @segments.each_value &.close
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

    def publish(sp : SegmentPosition, flush = false) : Bool
      return false if @closed
      if @max_length.try { |ml| @ready.size >= ml }
        @log.debug { "Overflow #{@max_length} #{@reject_on_overflow ? "reject-publish" : "drop-head"}" }
        if @reject_on_overflow
          @log.debug { "Overflow reject message sp=#{sp}" }
          raise RejectOverFlow.new
        else
          drophead
        end
      end
      @log.debug { "Enqueuing message sp=#{sp}" }
      @ready.push sp
      @message_available.send nil unless @message_available.full?
      @log.debug { "Enqueued successfully #{sp} ready=#{@ready.size} unacked=#{unacked_count} \
                      consumers=#{@consumers.size}" }
      @publish_count += 1
      true
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
    rescue ex : IO::EOFError
      @log.error { "Could not read metadata for sp=#{sp}" }
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
      nil
    end

    private def schedule_expiration_of_next_msg(now) : Bool
      expired_msg = false
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
            @ready_lock.synchronize { @ready.shift }
            expired_msg = true
            expire_msg(meta, sp, :expired)
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
      end
      expire_msg(meta, sp, :expired)
    end

    def expire_msg(sp : SegmentPosition, reason : Symbol)
      if @dlx
        meta = metadata(sp) || return
        expire_msg(meta, sp, reason)
      else
        ack(sp, true)
      end
    end

    private def expire_msg(meta : MessageMetadata, sp : SegmentPosition, reason : Symbol)
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      meta_bytesize = meta.bytesize
      dlx = meta.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
      dlrk = meta.properties.headers.try(&.fetch("x-dead-letter-routing-key", nil)) || @dlrk || meta.routing_key
      if dlx
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
          "time"         => Time.utc_now,
        }
        if props.expiration
          death["original-expiration"] = props.expiration
          props.expiration = nil
        end
        xdeaths.unshift AMQP::Table.new(death)

        headers["x-death"] = xdeaths

        @read_lock.synchronize do
          seg = @segments[sp.segment]
          body_pos = sp.position + meta_bytesize
          if @segment_pos[sp.segment] != body_pos
            @log.debug { "Current pos #{@segment_pos[sp.segment]}" }
            @log.debug { "Message position: #{sp.position}" }
            @log.debug { "Metadata size #{meta_bytesize}" }
            @log.debug { "Seeking to body pos #{body_pos}" }
            seg.seek(body_pos, IO::Seek::Set)
            @segment_pos[sp.segment] = body_pos
          end
          msg = Message.new(meta.timestamp, dlx.to_s,
            dlrk.to_s, props, meta.size, seg)
          @log.debug { "Dead-lettering #{sp} to exchange \"#{msg.exchange_name}\", routing key \"#{msg.routing_key}\"" }
          @vhost.publish msg
        end
      end
      ack(sp, true)
    rescue IO::EOFError
      @log.error { "EOF while dead-lettering sp=#{sp}" }
      @segment_pos[sp.segment] = @segments[sp.segment].pos.to_u32
    end

    private def expires_in(now = Time.monotonic) : Time::Span?
      if @expires
        return @last_get_time + @expires.not_nil!.milliseconds - now
      end
    end

    private def expire_queue(now = Time.monotonic) : Bool
      exp = expires_in(now)
      return false if exp.nil?
      return false if exp > Time::Span.zero
      return false unless @consumers.empty?
      @log.debug "Expired"
      @vhost.delete_queue(@name)
      true
    end

    private def expire_message(now = Time.utc_now) : Time::Span?
      loop do
        sp = @ready_lock.synchronize { @ready[0]? } || break
        @log.debug { "Checking if next message has to be expired" }
        meta = metadata(sp) || break
        exp_ms = meta.properties.expiration.try(&.to_i64?) || @message_ttl
        if exp_ms
          expire_at = Time.unix_ms(meta.timestamp) + exp_ms.milliseconds
          expire_in = expire_at - now
          if expire_in <= Time::Span.zero
            @ready_lock.synchronize { @ready.shift }
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
      sp = @ready.shift?
      return yield nil if sp.nil?
      @read_lock.synchronize do
        read(sp) do |env|
          yield env
        end
      end
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
      reject(sp, false)
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

    def ack(sp : SegmentPosition, flush : Bool)
      return if @deleted
      @log.debug { "Acking #{sp}" }
      idx = @get_unacked.index(sp)
      @get_unacked.delete_at(idx) if idx
      @deliveries.delete(sp)
      @ack_count += 1
      Fiber.yield if @ack_count % 8192 == 0
      consumer_available
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted
      @log.debug { "Rejecting #{sp}" }
      idx = @get_unacked.index(sp)
      @get_unacked.delete_at(idx) if idx
      @reject_count += 1
      if requeue
        @ready_lock.synchronize do
          i = @ready.index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
        end
        @requeued << sp
        @message_available.send nil unless @message_available.full?
      else
        expire_msg(sp, :rejected)
      end
      Fiber.yield if @reject_count % 8192 == 0
    end

    private def requeue_many(sps : Deque(SegmentPosition))
      return if @deleted
      return if sps.empty?
      @log.debug { "Returning #{sps.size} msgs to ready state" }
      @reject_count += sps.size
      @ready_lock.synchronize do
        sps.reverse_each do |sp|
          i = @ready.index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @requeued << sp
        end
      end
      @message_available.send nil unless @message_available.full?
    end

    private def drophead
      if sp = @ready_lock.synchronize { @ready.shift? }
        @log.debug { "Overflow drop head sp=#{sp}" }
        expire_msg(sp, :maxlen)
      end
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      return if @closed
      @last_get_time = Time.monotonic
      @consumers.push consumer
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      consumer_available
      spawn(name: "Notify observer vhost=#{@vhost.name} queue=#{@name}") do
        notify_observers(:add_consumer, consumer)
      end
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      if @consumers.delete consumer
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
