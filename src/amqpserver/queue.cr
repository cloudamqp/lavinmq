module AMQPServer
  class Queue
    class QueueFile < File
      include AMQP::IO
    end

    getter name, durable, exclusive, auto_delete, arguments
    def initialize(@vhost : VHost, @name : String, @durable : Bool, @exclusive : Bool, @auto_delete : Bool, @arguments : Hash(String, AMQP::Field))
      @consumers = Array(Client::Channel::Consumer).new
      @wfile = QueueFile.open("/tmp/#{@name}.q", "a")
      @rfile = QueueFile.open("/tmp/#{@name}.q", "r")
      @index = QueueFile.open("/tmp/#{@name}.idx", "a")
      if @index.pos > 0
        @index.seek(-4, IO::Seek::End)
        last_pos = @index.read_uint32
        @rfile.seek(last_pos)
      end
    end

    def to_json(json : JSON::Builder)
      {
        name: @name, durable: @durable, exclusive: @exclusive,
        auto_delete: @auto_delete, arguments: @arguments,
        consumers: @consumers.size, vhost: @vhost.name,
      }.to_json(json)
    end

    def message_count
      0_u32 #FIXME
    end

    def consumer_count
      @consumers.size.to_u32
    end

    def publish(msg : Message)
      @consumers.each { |c| c.deliver(msg) }
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_int msg.size
      msg.properties.encode @wfile
      @wfile.write msg.body.to_slice
      @wfile.flush #if msg.properties.delivery_mode.try { |v| v > 0 }
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

      msg
    rescue ex : IO::EOFError
      puts "EOF of queue"
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
    end
  end
end
