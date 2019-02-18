module AvalancheMQ
  struct Message
    property timestamp, exchange_name, routing_key, properties
    getter size, body_io

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64, @body_io : IO)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize + @properties.bytesize +
        sizeof(UInt64) + @size
    end
  end

  struct MessageMetadata
    getter timestamp, exchange_name, routing_key, properties

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize + @properties.bytesize
    end
  end

  struct Envelope
    getter segment_position, message, redelivered

    def initialize(@segment_position : SegmentPosition, @message : Message, @redelivered = false)
    end
  end
end
