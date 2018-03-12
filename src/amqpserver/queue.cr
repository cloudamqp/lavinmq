require "logger"
require "./amqp/io"

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
      @enq = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "a+")
      @ack = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "a+")
      @msgs = QueueFile.open(File.join(@vhost.data_dir, "messages.0"), "r")
      @unacked = Set(Int32).new
      @ready = Deque(Int32).new
      restore_index
      spawn deliver_loop
      @log.info "Queue #{@name}: Has #{message_count} messages"
    end

    def message_count : UInt32
      @ready.size.to_u32
    end

    def deliver_loop
      loop do
        if @consumers.size > 0
          c = @consumers.sample
          msg, pos = get(c.no_ack)
          if msg
            c.deliver(msg, pos, self)
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
      acked = Set(Int32).new(@ack.size / 4)
      loop do
        break if @ack.pos == @ack.size
        acked << @ack.read_int32
      end
      loop do
        break if @enq.pos == @enq.size
        pos = @enq.read_int32
        @ready << pos unless acked.includes? pos
      end
    end

    def close(deleting = false)
      @consumers.each { |c| c.close }
      @event_channel.send Event::Close
      @ack.close
      @enq.close
      @msgs.close
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

    def consumer_count
      @consumers.size.to_u32
    end

    def publish(pos : Int32, flush = false)
      @log.info "Publihsing message #{pos} in queue #{@name}"
      @ready.push pos
      @enq.write_int pos
      @enq.flush if flush
      @event_channel.send Event::MessagePublished
      @log.info "Published message #{pos} in queue #{@name}"
    end

    def get(no_ack = true) : Tuple(Message | Nil, Int32)
      @log.info "Getting message from queue #{@name}"
      pos = @ready.pop? || return { nil, 0 }
      @log.info "Trying to read message #{pos} for queue #{@name}"
      if no_ack
        @ack.write_int pos
      else
        # TODO: must couple with a consumer
        @unacked << pos
      end

      @msgs.seek(pos, IO::Seek::Set)
      ex = @msgs.read_short_string
      rk = @msgs.read_short_string
      pr = AMQP::Properties.decode @msgs
      sz = @msgs.read_uint64
      bd = Bytes.new(sz)
      @msgs.read(bd)
      msg = Message.new(ex, rk, sz, pr)
      msg << bd
      { msg, pos }
    end

    def ack(pos : Int32)
      @ack.write_int pos
      @unacked.delete(pos)
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
