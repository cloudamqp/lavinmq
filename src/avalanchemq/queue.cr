require "logger"
require "digest/sha1"
require "./amqp/io"
require "./segment_position"

module AvalancheMQ
  class Queue
    include PolicyTarget
    class QueueFile < File
      include AMQP::IO
    end

    @durable = false
    @log : Logger
    @message_ttl : UInt16 | Int32 | Int64 | Nil
    @max_length : UInt16 | Int32 | Int64 | Nil
    @dlx : String?
    @dlrk : String?
    @overflow : String?
    @closed = false
    @exclusive_consumer = false
    getter name, durable, exclusive, auto_delete, arguments
    def_equals_and_hash @vhost.name, @name

    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = AMQP::Table.new)
      @log = @vhost.log.dup
      @log.progname += "/Queue[#{@name}]"
      handle_arguments
      @consumers = Array(Client::Channel::Consumer).new
      @message_available = Channel(Nil).new(1)
      @consumer_available = Channel(Nil).new(1)
      @unacked = Deque(SegmentPosition).new
      @ready = Deque(SegmentPosition).new
      @segments = Hash(UInt32, QueueFile).new do |h, seg|
        path = File.join(@vhost.data_dir, "msgs.#{seg.to_s.rjust(10, '0')}")
        h[seg] = QueueFile.open(path, "r")
      end
      spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
    end

    def has_exclusive_consumer?
      @exclusive_consumer
    end

    def apply_policy(@policy : Policy)
      handle_arguments
      @policy.not_nil!.definition.each do |k, v|
        case k
        when "max-length"
          @max_length = v.as Int64
        when "message-ttl"
          @message_ttl = v.as Int64
        when "overflow"
          @overflow = v.to_s
        when "expires"
          # TODO
        when "dead-letter-exchange"
          @dlx = v.to_s
        when "dead-letter-routing-key"
          @dlrk = v.to_s
        end
      end
    end

    def handle_arguments
      message_ttl = @arguments.fetch("x-message-ttl", nil)
      @message_ttl = message_ttl if message_ttl.is_a? UInt16 | Int32 | Int64
      @dlx = @arguments.fetch("x-dead-letter-exchange", nil).try &.to_s
      @dlrk = @arguments.fetch("x-dead-letter-routing-key", nil).try &.to_s
      max_length = @arguments.fetch("x-max-length", nil)
      @max_length = max_length if max_length.is_a? UInt16 | Int32 | Int64
      @overflow = @arguments.fetch("x-overflow", "drop-head").try &.to_s
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

    def close_unused_segments_and_report_used : Set(UInt32)
      s = Set(UInt32).new
      @ready.each { |sp| s << sp.segment }
      @unacked.each { |sp| s << sp.segment }
      @segments.each do |seg, f|
        unless s.includes? seg
          @log.debug { "Closing non referenced segment #{seg}" }
          f.close
        end
      end
      @segments.reject! s.to_a
      s
    end

    def deliver_loop
      loop do
        unless @ready[0]?
          @log.debug { "Waiting for msgs" }
          @message_available.receive
        end
        break if @closed
        @log.debug { "Looking for available consumers" }
        consumers = @consumers.select { |c| c.accepts? }
        if consumers.size != 0
          @log.debug { "Picking a consumer" }
          c = consumers.sample
          @log.debug { "Getting a new message" }
          if env = get(c.no_ack)
            @log.debug { "Delivering #{env.segment_position} to consumer" }
            begin
              c.deliver(env.message, env.segment_position, self)
            rescue Channel::ClosedError
              @log.debug "Consumer chosen for delivery has disconnected"
              reject env.segment_position, true
              Fiber.yield
            end
            @log.debug { "Delivery done" }
          end
        else
          @log.debug "No consumer available"
          schedule_expiration_of_next_msg
          @log.debug "Waiting for consumer"
          @consumer_available.receive
        end
      end
      @log.debug "Exiting delivery loop"
    rescue Channel::ClosedError
      @log.debug "Delivery loop channel closed"
    end

    def close(deleting = false) : Nil
      if @closed
        @log.debug "Already closed"
        return
      end
      @log.info "Closing"
      @closed = true
      @message_available.close
      @consumer_available.close
      @consumers.clear
      @segments.each_value &.close
      delete if !deleting && (@auto_delete || @exclusive)
      @log.info "Closed"
    end

    protected def delete
      @log.info "Deleting"
      @vhost.apply AMQP::Queue::Delete.new 0_u16, 0_u16, @name, false, false, false
    end

    def to_json(json : JSON::Builder)
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
        messages: @ready.size + @unacked.size,
        ready: @ready.size,
        unacked: @unacked.size,
        policy: @policy
      }.to_json(json)
    end

    def publish(sp : SegmentPosition, flush = false)
      if @max_length.try { |ml| @ready.size >= ml }
        @log.debug { "Overflow #{@max_length} #{@overflow}" }
        case @overflow
        when "reject-publish"
          reject(sp, false)
          return false
        when "drop-head"
          if spr = @ready.shift?
            reject(spr, false)
          end
        end
      end
      @log.debug { "Enqueuing message #{sp}" }
      @ready.push sp
      @message_available.send nil unless @message_available.full?
      @log.debug { "Enqueued successfully #{sp}" }
      true
    end

    def metadata(sp) : MessageMetadata
      seg = @segments[sp.segment]
      seg.seek(sp.position, IO::Seek::Set)
      ts = seg.read_int64
      ex = seg.read_short_string
      rk = seg.read_short_string
      pr = AMQP::Properties.decode seg
      MessageMetadata.new(ts, ex, rk, pr)
    end

    def schedule_expiration_of_next_msg
      sp = @ready[0]? || return nil
      @log.debug { "Checking if next message has to be expired" }
      meta = metadata(sp)
      @log.debug { "Next message: #{meta}" }
      exp_ms = meta.properties.expiration.try(&.to_i64?) || @message_ttl
      if exp_ms
        expire_at = meta.timestamp + exp_ms
        expire_in = expire_at - Time.now.epoch_ms
        spawn(expire_later(expire_in, meta, sp),
              name: "expire_later(#{expire_in}) #{@vhost.name}/#{@name}")
      else
        @log.debug { "No message to expire" }
      end
    end

    def expire_later(expire_in, meta, sp)
      @log.debug { "Expiring #{sp} in #{expire_in}ms" }
      sleep expire_in.milliseconds if expire_in > 0
      return unless @ready[0]? == sp
      @ready.shift
      expire_msg(meta, sp, :expired)
    end

    def expire_msg(sp : SegmentPosition, reason : Symbol)
      expire_msg(metadata(sp), sp, reason)
    end

    def expire_msg(meta : MessageMetadata, sp : SegmentPosition, reason : Symbol)
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      dlx = meta.properties.headers.try(&.fetch("x-dead-letter-exchange", nil)) || @dlx
      if dlx
        env = read(sp)
        msg = env.message
        msg.properties.headers.try &.delete("x-dead-letter-exchange")
        msg.exchange_name = dlx.to_s
        dlrk = msg.properties.headers.try(&.delete("x-dead-letter-routing-key")) || @dlrk
        if dlrk
          msg.routing_key = dlrk.to_s
        end
        msg.properties.expiration = nil
        # TODO: Set x-death header https://www.rabbitmq.com/dlx.html
        @log.debug { "Dead-lettering #{sp} to exchange \"#{msg.exchange_name}\", routing key \"#{msg.routing_key}\"" }
        @vhost.publish msg
      end
      ack(sp, true)
    end

    def get(no_ack : Bool) : Envelope | Nil
      sp = @ready.shift? || return nil
      @unacked << sp unless no_ack
      read(sp)
    end

    def read(sp : SegmentPosition) : Envelope
      seg = @segments[sp.segment]
      seg.seek(sp.position, IO::Seek::Set)
      ts = seg.read_int64
      ex = seg.read_short_string
      rk = seg.read_short_string
      pr = AMQP::Properties.decode seg
      sz = seg.read_uint64
      bd = Bytes.new(sz)
      seg.read(bd)
      msg = Message.new(ts, ex, rk, pr, sz, bd)
      Envelope.new(sp, msg)
    end

    def ack(sp : SegmentPosition, flush : Bool)
      @log.debug { "Acking #{sp}" }
      idx = @unacked.rindex(sp)
      @log.debug { "Acking idx #{idx} in unacked deque" }
      @unacked.delete_at(idx) if idx
      @consumer_available.send nil unless @consumer_available.full?
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      @log.debug { "Rejecting #{sp}" }
      idx = @unacked.rindex(sp)
      @log.debug { "Rejecting idx #{idx} in unacked deque" }
      if idx
        @unacked.delete_at(idx)
        if requeue
          i = @ready.index { |rsp| rsp > sp } || 0
          @ready.insert(i, sp)
          @message_available.send nil unless @message_available.full?
        else
          expire_msg(sp, :rejected)
        end
      end
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      @consumers.push consumer
      @exclusive_consumer = true if consumer.exclusive
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      @consumer_available.send nil unless @consumer_available.full?
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      if @consumers.delete consumer
        @exclusive_consumer = false if consumer.exclusive
        consumer.unacked.each { |sp| reject(sp, true) }
        @log.debug { "Removing consumer (#{@consumers.size} left)" }
        if @auto_delete && @consumers.size == 0
          delete
        end
      end
    end

    def purge
      purged_count = message_count
      @ready.clear
      @consumers.each { |c| c.unacked.clear }
      @log.debug { "Purged #{purged_count} messages" }
      purged_count
    end
  end
end
