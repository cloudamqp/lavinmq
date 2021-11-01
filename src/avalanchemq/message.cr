module AvalancheMQ
  struct BytesMessage
    property timestamp, exchange_name, routing_key, properties
    getter size, body

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64, @body : Bytes)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64) + @size
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def self.skip(io, format) : UInt64
      skipped = 0_u64
      skipped += io.skip(sizeof(UInt64))                             # ts
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # ex
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # rk
      skipped += AMQP::Properties.skip(io, format)
      skipped += io.skip(UInt64.from_io io, format) + sizeof(UInt64)
      skipped
    end

    def self.from_io(io, format = IO::ByteFormat::SystemEndian) : self
      ts = Int64.from_io io, format
      ex = AMQP::ShortString.from_io io, format
      rk = AMQP::ShortString.from_io io, format
      pr = AMQP::Properties.from_io io, format
      sz = UInt64.from_io io, format
      body = io.to_slice(io.pos, sz)
      BytesMessage.new(ts, ex, rk, pr, sz, body)
    end
  end

  struct Message
    property timestamp, exchange_name, routing_key, properties
    getter size, body_io

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64, @body_io : IO)
    end

    def initialize(@exchange_name : String, @routing_key : String,
                   body : String, @properties = AMQP::Properties.new)
      @timestamp = Time.utc.to_unix_ms
      @size = body.bytesize.to_u64
      @body_io = IO::Memory.new(body)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64) + @size
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def self.skip(io, format) : UInt64
      skipped = 0_u64
      skipped += io.skip(sizeof(UInt64))                             # ts
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # ex
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # rk
      skipped += AMQP::Properties.skip(io, format)
      skipped += io.skip(UInt64.from_io io, format) + sizeof(UInt64)
      skipped
    end
  end

  struct MessageMetadata
    getter timestamp, exchange_name, routing_key, properties, size

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt64)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64)
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def self.from_io(io, format = IO::ByteFormat::SystemEndian)
      ts = Int64.from_io io, format
      ex = AMQP::ShortString.from_io io, format
      rk = AMQP::ShortString.from_io io, format
      pr = AMQP::Properties.from_io io, format
      sz = UInt64.from_io io, format
      MessageMetadata.new(ts, ex, rk, pr, sz)
    end
  end

  struct Envelope
    getter segment_position, message, redelivered

    def initialize(@segment_position : SegmentPosition, @message : BytesMessage,
                   @redelivered = false)
    end

    def persistent?
      @message.persistent?
    end
  end
end
