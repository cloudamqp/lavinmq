module AvalancheMQ
  struct Message
    property exchange_name, routing_key, properties
    getter size, body

    def initialize(@exchange_name : String, @routing_key : String, @size : UInt64,
                   @properties : AMQP::Properties, @body : Bytes)
    end
  end

  struct Envelope
    getter segment_position, message
    def initialize(@segment_position : SegmentPosition, @message : Message)
    end
  end
end
