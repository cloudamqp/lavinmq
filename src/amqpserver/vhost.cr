module AMQPServer
  class VHost
    getter name, exchanges, queues
    def initialize(@name : String)
      @queues = {
        "q1" => Queue.new("q1", durable: true, auto_delete: false, exclusive: false, arguments: {} of String => AMQP::Field)
      }
      @exchanges = {
        "" => Exchange.new("", type: "direct", durable: true,
                           arguments: {} of String => AMQP::Field,
                           bindings: { "q1" => [@queues["q1"]] })
      }
    end
  end
end
