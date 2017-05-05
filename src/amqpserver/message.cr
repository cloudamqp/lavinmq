module AMQPServer
  class Message
    getter exchange_name, routing_key, size, body

    def initialize(@exchange_name : String, @routing_key : String, @size = 0_u64)
      @body = IO::Memory.new(@size)
    end

    def body_size=(size)
      @size = size
      @body = IO::Memory.new(size)
    end

    def add_content(bytes)
      @body.write bytes
    end

    def full?
      @body.pos == @size
    end
  end
end
