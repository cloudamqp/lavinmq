module AMQPServer
  class Message
    getter exchange_name, routing_key, size, body, properties

    def initialize(@exchange_name : String, @routing_key : String, @size : UInt64, @properties : AMQP::Properties)
      @body = IO::Memory.new(@size)
    end

    def <<(bytes)
      @body.write bytes 
    end

    def full?
      @body.pos == @size
    end
  end
end
