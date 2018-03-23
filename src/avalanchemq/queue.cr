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
            if msg_sp = get(c.no_ack)
              msg, sp = msg_sp
              @log.info { "Queue #{@name} got #{sp} for delivery to consumer" }
              begin
                c.deliver(msg, sp, self)
                @log.info { "Queue #{@name} return after delivery to consumer" }
              rescue Channel::ClosedError
                @log.info "Consumer channel closed, rejecting msg"
                reject sp, true
              end
              next
            end
          rescue IndexError
            @log.info "IndexError in consumer.sample"
          end
        end
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

    def get(no_ack : Bool) : Tuple(Message, SegmentPosition) | Nil
      @log.info "Getting message from queue #{@name}"
      sp = @ready.shift? || return nil
      @log.info "Trying to read message #{sp} for queue #{@name}"
      if no_ack && @durable
        @ack.try &.write_bytes sp
      else
        @unacked << sp
      end

      seg = @segments[sp.segment]
      seg.seek(sp.position, IO::Seek::Set)
      ex = seg.read_short_string
      rk = seg.read_short_string
      pr = AMQP::Properties.decode seg
      sz = seg.read_uint64
      bd = Bytes.new(sz)
      seg.read(bd)
      msg = Message.new(ex, rk, sz, pr, bd)
      { msg, sp }
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
      @ready.unshift sp if requeue
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
