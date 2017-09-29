module AMQPServer
  class Queue
    class QueueFile < File
      include AMQP::IO
    end

    @message_count : UInt32
    getter name, durable, exclusive, auto_delete, arguments, message_count

    def initialize(@vhost : VHost, @name : String, @durable : Bool, @exclusive : Bool, @auto_delete : Bool, @arguments : Hash(String, AMQP::Field))
      @consumers = Array(Client::Channel::Consumer).new
      @wfile = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.q"), "a")
      @rfile = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.q"), "r")
      @index = QueueFile.open(File.join(@vhost.data_dir, "#{@name}.idx"), "a")
      if @index.pos > 0
        @index.seek(-4, IO::Seek::End)
        last_pos = @index.read_uint32
        @rfile.seek(last_pos)
      end
      @message_count = count_messages
    end

    def count_messages : UInt32
      count = 0_u32
      loop do
        begin
          ex_length = @rfile.read_byte.as(Int)
          @rfile.seek(ex_length, IO::Seek::Current)
          rk_length = @rfile.read_byte.as(Int)
          @rfile.seek(rk_length, IO::Seek::Current)
          tbl_length = @rfile.read_uint32
          @rfile.seek(tbl_length, IO::Seek::Current)
          sz = @rfile.read_uint64
          @rfile.seek(sz, IO::Seek::Current)
          count += 1
        rescue ex : IO::EOFError
          break
        end
      end
      count
    end

    def close(deleting = false)
      @consumers.each { |c| c.close }
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
      @consumers.sample.deliver(msg) if @consumers.size > 0
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_int msg.size
      msg.properties.encode @wfile
      @wfile.write msg.body.to_slice
      @wfile.flush #if msg.properties.delivery_mode.try { |v| v > 0 }
      @message_count += 1
    end

    def get
      ex = @rfile.read_short_string
      rk = @rfile.read_short_string
      sz = @rfile.read_uint64
      pr = AMQP::Properties.decode @rfile
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
      while msg = get
        consumer.deliver msg
      end
      @consumers.push consumer
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      @consumers.delete consumer
      if @auto_delete && @consumers.size == 0
        delete
      end
    end
  end
end
