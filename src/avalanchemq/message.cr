module AvalancheMQ
  struct Message
    getter exchange_name, routing_key, size, body, properties

    def initialize(@exchange_name : String, @routing_key : String, @size : UInt64,
                   @properties : AMQP::Properties, @body : Bytes)
    end
  end
end
