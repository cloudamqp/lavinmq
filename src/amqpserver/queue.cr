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

    @message_count : UInt32
    @log : Logger
    getter name, durable, exclusive, auto_delete, arguments, message_count

    def initialize(@vhost : VHost, @name : String, @durable : Bool,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      @log = @vhost.log
      @consumers = Array(Client::Channel::Consumer).new
      @message_count = count_messages
      @log.info "Queue #{@name}: Has #{@message_count} messages"
      @event_channel = Channel(Event).new
      @enq = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "a+")
      @ack = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "a+")
      @msgs = QueueFile.open(File.join(@vhost.data_dir, "messages.0"), "r")
      @unacked = Set(UInt64).new
      @queue = Deque(UInt64).new
      spawn deliver_loop
    end

    def deliver_loop
      loop do
        @log.info "Delivery loop stage 1"
        if @consumers.size > 0
          c = @consumers.sample
          msg, offset = get(c.no_ack)
          if msg
            c.deliver(msg, offset, self)
            next
          end
        end
        event = @event_channel.receive
        @log.debug "Queue event #{@name}: #{event}"
        break if event == Event::Close
      end
    rescue ex
      @log.error "Queue delivery loop: #{ex.inspect}"
      raise ex
    end

    def count_messages : UInt32
      @log.info "Queue #{@name}: Counting messages"
      last_ack = 0_u64
      QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "r") do |rack|
        rack.seek(-4, IO::Seek::End)
        last_ack = rack.read_uint64
      end

      QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "r") do |renq|
        loop do
          enq_offset = renq.read_uint64
          break if enq_offset >= last_ack
        end
        msgs_since_last_ack = (renq.size - renq.pos) / 4
        @log.info "Queue #{@name}: Has #{msgs_since_last_ack} messages"
        return msgs_since_last_ack.to_u32
      end
    rescue
      0_u32
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
        messages: message_count
      }.to_json(json)
    end

    def consumer_count
      @consumers.size.to_u32
    end

    def publish(offset, flush = false)
      @log.info "Publihsing message #{offset} in queue #{@name}"
      @queue.push offset
      @enq.write_int offset
      @enq.flush if flush
      @message_count += 1
      @event_channel.send Event::MessagePublished
      @log.info "Published message #{offset} in queue #{@name}"
    end

    def get(no_ack = true) : Tuple(Message | Nil, UInt64)
      @log.info "Getting message from queue #{@name}"
      offset = @queue.pop? || return { nil, 0_u64 }
      @log.info "Trying to read message #{offset} for queue #{@name}"
      if no_ack
        @ack.write_int offset
        @message_count -= 1
      else
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
      @log.info "Got message from queue #{@name}"
      { msg, offset }
    rescue ex : IO::EOFError | Errno
      @log.info "EOF of queue #{@name}"
      { nil, 0_u64 }
    end

    def ack(offset : UInt64)
      @ack.write_int offset
      @message_count -= 1
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
