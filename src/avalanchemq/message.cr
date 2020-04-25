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

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def self.skip(io)
      io.skip(sizeof(UInt64)) # ts
      io.skip(io.read_byte || raise IO::EOFError.new) # ex
      io.skip(io.read_byte || raise IO::EOFError.new) # rk
      AMQP::Properties.skip(io)
      sz = UInt64.from_io io, IO::ByteFormat::NetworkEndian
      io.skip(sz)
    end
  end

  struct MessageMetadata
    getter timestamp, exchange_name, routing_key, properties, size

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize + @properties.bytesize + sizeof(UInt64)
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end
  end

  struct Envelope
    getter segment_position, message, redelivered

    def initialize(@segment_position : SegmentPosition, @message : Message, @redelivered = false)
    end

    def persistent?
      @message.persistent?
    end
  end
end
