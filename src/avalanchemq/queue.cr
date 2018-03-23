require "logger"
require "digest/sha1"
require "./amqp/io"
require "./segment_position"

module AvalancheMQ
  class Queue
    class QueueFile < File
      include AMQP::IO
    end
    enum Event
      ConsumerAdded
      MessagePublished
      MessageAcked
      MessageRejected
      Purged
    end

    @log : Logger
    @ack : QueueFile | Nil
    @enq : QueueFile | Nil
    getter name, durable, exclusive, auto_delete, arguments

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      @log = @vhost.log
      @consumers = Array(Client::Channel::Consumer).new
      @event_channel = Channel(Event).new
      @unacked = Set(SegmentPosition).new
      @ready = Deque(SegmentPosition).new
      if @durable
        restore_index
        @enq = QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.enq"), "a")
        @ack = QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.ack"), "a")
      end
      @segments = Hash(UInt32, QueueFile).new do |h, seg|
        h[seg] = QueueFile.open(File.join(@vhost.data_dir, "msgs.#{seg}"), "r")
      end
      spawn deliver_loop, name: "Queue#deliver_loop #{@vhost.name}/#{@name}"
      @log.info "Queue #{@name}: Has #{message_count} messages"
    end

    def message_count : UInt32
      @ready.size.to_u32
    end

    def consumer_count : UInt32
      @consumers.size.to_u32
    end

    def close_unused_segments_and_report_used
      s = Set(UInt32).new
      @ready.each { |sp| s << sp.segment }
      @unacked.each { |sp| s << sp.segment }
      @segments.each do |sp, f|
        unless s.includes? sp
          @log.debug { "Closing non referenced segments #{sp} in #{@vhost.name}/#{@name}" }
          f.close
        end
      end
      @segments.reject! s.to_a
      s
    end

    def deliver_loop
      loop do
        consumers = @consumers.select { |c| c.accepts? }
        if consumers.size != 0
          begin
            @log.info { "Queue #{@name} got #{@consumers.size} consumers, sampling now" }
            c = consumers.sample
            if env = get(c.no_ack)
              @log.info { "Queue #{@name} got #{env.segment_position} for delivery to consumer" }
              begin
                c.deliver(env.message, env.segment_position, self)
                @log.info { "Queue #{@name} return after delivery to consumer" }
              rescue Channel::ClosedError
                @log.info "Consumer channel closed, rejecting msg"
                reject env.segment_position, true
              end
              next
            end
          rescue IndexError
            @log.info "IndexError in consumer.sample"
          end
        end
        schedule_expiration_of_next_msg
        @log.info { "Queue event #{@name} waiting" }
        event = @event_channel.receive
        @log.info { "Queue event #{@name}: #{event}" }
      end
    rescue Channel::ClosedError
      @log.debug "Delivery loop channel closed for queue #{@name}"
    end

    def restore_index
      @log.info "Restoring index on #{@vhost.name}/#{@name}"
      QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.ack"), "w") do |acks|
        QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.enq"), "w") do |enqs|
          acked = Set(SegmentPosition).new(acks.size / sizeof(SegmentPosition))
          loop do
            break if acks.pos == acks.size
            acked << SegmentPosition.decode acks
          end
          loop do
            break if enqs.pos == enqs.size
            sp = SegmentPosition.decode enqs
            @ready << sp unless acked.includes? sp
          end
        end
      end
    end

    def close(deleting = false)
      @consumers.clear
      @ack.try &.close
      @enq.try &.close
      @segments.each_value { |s| s.close }
      delete if !deleting && @auto_delete
      @event_channel.close
    end

    def delete
      @log.info "deleting queue #{@name}"
      @vhost.queues.delete @name
      close(deleting: true)
      if @durable
        File.delete File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.enq")
        File.delete File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.ack")
      end
    rescue ex : Errno
      @log.info "Deleting queue #{@name}, file not found"
    end

    def to_json(json : JSON::Builder)
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
        messages: @ready.size + @unacked.size,
        ready: @ready.size,
        unacked: @unacked.size
      }.to_json(json)
    end

    def publish(sp : SegmentPosition, flush = false)
      @log.debug { "Publishing message #{sp} in queue #{@name}" }
      @ready.push sp
      if @durable
        @enq.try &.write_bytes sp
        @enq.try &.flush if flush
      end

      @log.debug { "Sending to MessagePublishing to @event_channel in queue #{@name}" }
      @event_channel.send Event::MessagePublished unless @event_channel.full?
      @log.debug { "Published message #{sp} in queue #{@name}" }
    end

    def msg_properties(sp)
      seg = @segments[sp.segment]
      seg.seek(sp.position, IO::Seek::Set)
      seg.seek(seg.read_byte.to_i, IO::Seek::Current)
      seg.seek(seg.read_byte.to_i, IO::Seek::Current)
      AMQP::Properties.decode seg
    end

    def schedule_expiration_of_next_msg
      @log.debug { "Scheduling expiration of next msg for queue #{@vhost.name}/#{@name}" }
      sp = @ready[0]? || return nil
      p = msg_properties(sp)
      # TODO: save timestamp when message arrived, subtract expiration, sleep that time

      if exp_ms = p.expiration.try &.to_i64?
        expire_in = exp_ms + p.timestamp.not_nil!.epoch_ms - Time.now.epoch_ms
        spawn do
          sleep expire_in / 1_000
          expire_msg(sp, :expired)
        end
      end
    end

    def expire_msg(sp : SegmentPosition, reason : Symbol)
      props = msg_properties(sp)
      if props.headers.try &.key? "x-dead-letter-exchange"
        env = read(sp)
        msg = env.message
        if dlx = msg.properties.headers.try &.delete("x-dead-letter-exchange")
          msg.exchange_name = dlx.to_s
          if dlrk = msg.properties.headers.try &.delete("x-dead-letter-routing-key")
            msg.routing_key = dlrk.to_s
          end
          # TODO: Set x-death header https://www.rabbitmq.com/dlx.html
          @vhost.publish msg
        end
      end
      ack(sp)
    end

    def get(no_ack : Bool) : Envelope | Nil
      @log.info "Getting message from queue #{@name}"
      sp = @ready.shift? || return nil
      @log.info "Trying to read message #{sp} for queue #{@name}"
      if no_ack && @durable
        @ack.try &.write_bytes sp
      else
        @unacked << sp
      end
      read(sp)
    end

    def read(sp : SegmentPosition) : Envelope
      seg = @segments[sp.segment]
      seg.seek(sp.position, IO::Seek::Set)
      ex = seg.read_short_string
      rk = seg.read_short_string
      pr = AMQP::Properties.decode seg
      sz = seg.read_uint64
      bd = Bytes.new(sz)
      seg.read(bd)
      msg = Message.new(ex, rk, sz, pr, bd)
      Envelope.new(sp, msg)
    end

    def ack(sp : SegmentPosition)
      if @durable
        @ack.try &.write_bytes sp
      end
      @unacked.delete sp
      @event_channel.send Event::MessageAcked unless @event_channel.full?
    end

    def reject(sp : SegmentPosition, requeue : Bool)
      @unacked.delete sp
      if requeue
        @ready.unshift sp
      else
        expire_msg(sp, :rejected)
      end
      @event_channel.send Event::MessageRejected unless @event_channel.full?
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      @consumers.push consumer
      @log.info { "Adding consumer, Queue #{@name} got #{@consumers.size} consumers" }
      @event_channel.send Event::ConsumerAdded unless @event_channel.full?
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      @consumers.delete consumer
      consumer.unacked.each { |sp| reject(sp, true) }
      @log.info { "Removing consumer, Queue #{@name} got #{@consumers.size} consumers" }
      if @auto_delete && @consumers.size == 0
        delete
      end
    end

    def purge
      purged_count = message_count
      @ready.clear
      @enq.try &.truncate
      @log.info { "Purging #{purged_count} from #{@name}" }
      @event_channel.send Event::Purged unless @event_channel.full?
      purged_count
    end
  end
end
