require "logger"
require "./amqp/io"
require "./segment_position"

module AMQPServer
  class Queue
    class QueueFile < File
      include AMQP::IO
    end
    enum Event
      ConsumerAdded
      ConsumerRemoved
      MessagePublished
      Close
    end

    @log : Logger
    getter name, durable, exclusive, auto_delete, arguments

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      @log = @vhost.log
      @consumers = Array(Client::Channel::Consumer).new
      @event_channel = Channel(Event).new
      @unacked = Set(SegmentPosition).new
      @ready = Deque(SegmentPosition).new
      @enq = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "a+")
      @ack = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "a+")
      restore_index
      @segments = Hash(UInt32, QueueFile).new do |h, seg|
        h[seg] = QueueFile.open(File.join(@vhost.data_dir, "msgs.#{seg}"), "r")
      end
      spawn deliver_loop
      @log.info "Queue #{@name}: Has #{message_count} messages"
    end

    def message_count : UInt32
      @ready.size.to_u32
    end

    def consumer_count : UInt32
      @consumers.size.to_u32
    end

    def report_referenced_segments(s)
      @ready.each { |sp| s << sp.segment }
      @unacked.each { |sp| s << sp.segment }
    end

    def deliver_loop
      loop do
        if @consumers.size > 0
          c = @consumers.sample
          if msg_sp = get(c.no_ack)
            msg, sp = msg_sp
            c.deliver(msg, sp, self)
            next
          end
        end
        event = @event_channel.receive
        @log.info { "Queue event #{@name}: #{event}" }
        break if event == Event::Close
      end
    end

    def restore_index
      @log.info "Queue #{@name}: Restoring index"
      acked = Set(SegmentPosition).new(@ack.size / sizeof(SegmentPosition))
      loop do
        break if @ack.pos == @ack.size
        acked << SegmentPosition.decode @ack
      end
      loop do
        break if @enq.pos == @enq.size
        sp = SegmentPosition.decode @enq
        @ready << sp unless acked.includes? sp
      end
    end

    def close(deleting = false)
      @consumers.each { |c| c.close }
      @event_channel.send Event::Close
      @ack.close
      @enq.close
      @segments.each_value { |s| s.close }
      delete if !deleting && @auto_delete
    end

    def delete
      @log.info "deleting queue #{@name}"
      close(deleting: true)
      @vhost.queues.delete @name
      File.delete File.join(@vhost.data_dir, "#{@name}.enq")
      File.delete File.join(@vhost.data_dir, "#{@name}.ack")
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
      @log.debug { "Publihsing message #{sp} in queue #{@name}" }
      @ready.push sp
      sp.encode @enq
      @enq.flush if flush
      @event_channel.send Event::MessagePublished
      @log.debug { "Published message #{sp} in queue #{@name}" }
    end

    def get(no_ack = true) : Tuple(Message, SegmentPosition) | Nil
      @log.info "Getting message from queue #{@name}"
      sp = @ready.shift? || return nil
      @log.info "Trying to read message #{sp} for queue #{@name}"
      if no_ack
        sp.encode @ack
      else
        # TODO: must couple with a consumer
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
      sp.encode @ack
      @unacked.delete sp
    end

    def reject(sp : SegmentPosition)
      @unacked.delete sp 
      @queue.unshift sp
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      @consumers.push consumer
      @event_channel.send Event::ConsumerAdded
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      @log.info "Removing consumer from queue #{@name}"
      @event_channel.send Event::ConsumerRemoved
      @consumers.delete consumer
      if @auto_delete && @consumers.size == 0
        delete
      end
    end
  end
end
