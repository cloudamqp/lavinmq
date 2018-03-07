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
      @event_channel = Channel(Event).new
      @enq = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "a")
      @ack = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "a")
      @msgs = QueueFile.open(File.join(@vhost.data_dir, "messages.0"), "r")
      @unacked = Set(UInt64).new
      spawn deliver_loop
    end

    def deliver_loop
      loop do
        if @consumers.size > 0
          c = @consumers.sample 
          if msg = get(c.no_ack)
            c.deliver(msg)
            next
          end
        end
        event = @event_channel.receive
        @log.debug "Queue event #{@name}: #{event}"
        break if event == Event::Close
      end
    end

    def count_messages : UInt32
      @log.debug "Queue #{@name}: Counting messages"
      last_ack = 0_u64
      rack = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.ack"), "r")
      begin
        rack.seek(-4, IO::Seek::End)
        last_ack = rack.read_uint64
      rescue ex : IO::EOFError
      ensure
        rack.close
      end

      renq = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.enq"), "r")
      begin
        loop do
          enq_offset = renq.read_uint64
          break if enq_offset >= last_ack
        end
        msgs_since_last_ack = (renq.size - renq.pos) / 4
        @log.debug "Queue #{@name}: Has #{msgs_since_last_ack} messages"
        msgs_since_last_ack.to_u32
      rescue ex : IO::EOFError
        return 0_u32
      ensure
        renq.close
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
      @enq.write_int offset
      @enq.flush if flush
      @message_count += 1
      @event_channel.send Event::MessagePublished
    end

    def get(no_ack = true)
      offset = @enq.read_uint64
      if no_ack
        @ack.write_int offset
        @message_count -= 1
      else
        @unacked << offset
      end

      hs = @msgs.read_uint32
      ex = @msgs.read_short_string
      rk = @msgs.read_short_string
      pr = AMQP::Properties.decode @msgs
      sz = @msgs.read_uint64
      bd = Bytes.new(sz)
      @msgs.read(bd)
      msg = Message.new(ex, rk, sz, pr)
      msg << bd
      msg
    rescue ex : IO::EOFError
      @log.info "EOF of queue #@name"
      nil
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      @consumers.push consumer
      @event_channel.send Event::ConsumerAdded
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      @event_channel.send Event::ConsumerRemoved
      @consumers.delete consumer
      if @auto_delete && @consumers.size == 0
        delete
      end
    end
  end
end
