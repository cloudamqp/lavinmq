module AMQPServer
  class Queue
    class QueueFile < File
      include AMQP::IO
    end

    def initialize(@name : String)
      @wfile = QueueFile.open("/tmp/#{@name}.q", "a")
      @rfile = QueueFile.open("/tmp/#{@name}.q", "r")
    end

    def write_msg(msg : Message)
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_int msg.size
      @wfile.write msg.body.to_slice
      @wfile.flush #if msg.persistent
    end

    def get
      ex = @rfile.read_short_string
      rk = @rfile.read_short_string
      sz = @rfile.read_uint64
      bytes = Bytes.new(sz)
      @rfile.read(bytes)
      msg = Message.new(ex, rk, sz)
      msg.add_content bytes
      msg
    end
  end
end
