module AvalancheMQ
  struct Message
    property timestamp, exchange_name, routing_key, properties
    getter size, body_io

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64, @body_io : IO)
    end
  end

  struct MessageMetadata
    getter timestamp, exchange_name, routing_key, properties

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties)
    end
  end

  struct Envelope
    getter segment_position, message
    def initialize(@segment_position : SegmentPosition, @message : Message)
    end
  end
end
