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
    getter name, durable, exclusive, auto_delete, arguments, message_count

    def initialize(@vhost : VHost, @name : String, @durable : Bool, @exclusive : Bool, @auto_delete : Bool, @arguments : Hash(String, AMQP::Field))
      @consumers = Array(Client::Channel::Consumer).new
      @wfile = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.q"), "a")
      @rfile = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.q"), "r")
      @index = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.idx"), "a")
      @message_count = count_messages
      @event_channel = Channel(Event).new
      spawn deliver_loop
    end

    def reset_rfile_to_index
      if @index.pos > 3
        @index.seek(-4, IO::Seek::End)
        last_pos = @index.read_uint32.to_i
        @rfile.seek(last_pos)
      else
        @rfile.seek(0)
      end
    end

    def deliver_loop
      loop do
        if @consumers.size > 0
          if msg = get
            @consumers.sample.deliver(msg)
            next
          end
        end
        event = @event_channel.receive
        puts "Queue event #{@name}: #{event}"
        break if event == Event::Close
      end
    end

    def count_messages : UInt32
      reset_rfile_to_index
      print "Queue ", @name, ": Counting messages\n"
      count = 0_u32
      loop do
        begin
          ex_length = @rfile.read_byte.to_i
          @rfile.seek(ex_length, IO::Seek::Current)
          rk_length = @rfile.read_byte.to_i
          @rfile.seek(rk_length, IO::Seek::Current)
          AMQP::Properties.seek_past @rfile
          sz = @rfile.read_uint64.to_i
          @rfile.seek(sz, IO::Seek::Current)
          count += 1
        rescue ex : IO::EOFError
          reset_rfile_to_index
          break
        end
      end
      print "Queue ", @name, ": Has ", count, " messages\n"
      count
    end

    def close(deleting = false)
      @consumers.each { |c| c.close }
      @event_channel.send Event::Close
      @wfile.close
      @rfile.close
      @index.close
      delete if !deleting && @auto_delete
    end

    def delete
      close(deleting: true)
      @vhost.queues.delete @name
      File.delete File.join(@vhost.data_dir, "#{@name}.q")
      File.delete File.join(@vhost.data_dir, "#{@name}.idx")
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

    def publish(msg : Message)
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      msg.properties.encode @wfile
      @wfile.write_int msg.size
      @wfile.write msg.body.to_slice
      @wfile.flush if msg.properties.delivery_mode.try { |v| v > 0 }
      @message_count += 1
      @event_channel.send Event::MessagePublished
    end

    def get
      ex = @rfile.read_short_string
      rk = @rfile.read_short_string
      pr = AMQP::Properties.decode @rfile
      sz = @rfile.read_uint64
      bytes = Bytes.new(sz)
      @rfile.read(bytes)

      msg = Message.new(ex, rk, sz, pr)
      msg << bytes

      @index.write_int @rfile.pos.to_u32
      @message_count -= 1

      msg
    rescue ex : IO::EOFError
      print "EOF of queue ", @name, "\n"
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
