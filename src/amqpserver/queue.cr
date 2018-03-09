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
      @unacked = Set(UInt64).new
      @ready = Deque(UInt64).new
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
          msg, offset = get(c.no_ack)
          if msg
            c.deliver(msg, offset, self)
            next
          end
        end
        event = @event_channel.receive
        @log.info "Queue event #{@name}: #{event}"
        break if event == Event::Close
      end
    rescue ex
      @log.error "Queue delivery loop: #{ex.inspect}"
      raise ex
    end

    def restore_index
      @log.info "Queue #{@name}: Restoring index"
      acked = Set(UInt64).new(@ack.size / 8)
      loop do
        acked << @ack.read_uint64
      rescue ex : IO::EOFError
        break
      end
      loop do
        offset = @enq.read_uint64
        @ready << offset unless acked.includes? offset
      rescue ex : IO::EOFError
        break
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
        ready: @read.size,
        unacked: @unacked.size
      }.to_json(json)
    end

    def consumer_count
      @consumers.size.to_u32
    end

    def publish(offset, flush = false)
      @log.info "Publihsing message #{offset} in queue #{@name}"
      @ready.push offset
      @enq.write_int offset
      @enq.flush if flush
      @event_channel.send Event::MessagePublished
      @log.info "Published message #{offset} in queue #{@name}"
    end

    def get(no_ack = true) : Tuple(Message | Nil, UInt64)
      @log.info "Getting message from queue #{@name}"
      offset = @ready.pop? || return { nil, 0_u64 }
      @log.info "Trying to read message #{offset} for queue #{@name}"
      if no_ack
        @ack.write_int offset
      else
        # TODO: must couple with a consumer
        @unacked << offset
      end

      @log.info "@msgs pos: #{@msgs.pos}"
      os = @msgs.read_uint64
      ex = @msgs.read_short_string
      rk = @msgs.read_short_string
      pr = AMQP::Properties.decode @msgs
      sz = @msgs.read_uint64
      bd = Bytes.new(sz)
      @msgs.read(bd)
      msg = Message.new(ex, rk, sz, pr)
      msg << bd
      { msg, offset }
    rescue ex : IO::EOFError | Errno
      @log.info "EOF of queue #{@name}"
      { nil, 0_u64 }
    end

    def ack(offset : UInt64)
      @ack.write_int offset
      @unacked.delete(offset)
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
