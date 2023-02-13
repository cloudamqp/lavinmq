module LavinMQ
  struct BytesMessage
    property timestamp, exchange_name, routing_key, properties
    getter size, body

    MIN_BYTESIZE = 8 + 1 + 1 + 2 + 4

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt32, @body : Bytes)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt32) + @size
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def ttl
      @properties.expiration.try(&.to_i64?)
    end

    def dlx : String?
      @properties.headers.try(&.fetch("x-dead-letter-exchange", nil).as?(String))
    end

    def dlrk : String?
      @properties.headers.try(&.fetch("x-dead-letter-routing-key", nil).as?(String))
    end

    def self.skip(io, format = IO::ByteFormat::SystemEndian) : UInt32
      skipped = 0_u32
      skipped += io.skip(sizeof(Int64))                              # ts
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # ex
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # rk
      skipped += AMQP::Properties.skip(io, format)
      skipped += io.skip(UInt32.from_io io, format) + sizeof(UInt32)
      skipped
    end

    def self.from_bytes(bytes, format = IO::ByteFormat::SystemEndian) : self
      ts = format.decode(Int64, bytes[0, 8]); pos = 8
      ex_len = bytes[pos]; pos += 1
      ex = String.new(bytes.to_unsafe + pos, ex_len); pos += ex_len
      rk_len = bytes[pos]; pos += 1
      rk = String.new(bytes.to_unsafe + pos, rk_len); pos += rk_len
      pr = AMQP::Properties.from_bytes(bytes + pos, format); pos += pr.bytesize
      sz = format.decode(UInt32, bytes[pos, 4]); pos += 4
      body = bytes[pos, sz]
      BytesMessage.new(ts, ex, rk, pr, sz, body)
    end
  end

  struct Message
    property timestamp, exchange_name, routing_key, properties
    getter size, body_io

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQP::Properties,
                   @size : UInt32, @body_io : IO)
    end

    def initialize(@exchange_name : String, @routing_key : String,
                   body : String, @properties = AMQP::Properties.new)
      @timestamp = RoughTime.unix_ms
      @size = body.bytesize.to_u32
      @body_io = IO::Memory.new(body)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt32) + @size
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def dlx : String?
      @properties.headers.try(&.fetch("x-dead-letter-exchange", nil).as?(String))
    end

    def to_io(io : IO, format = IO::ByteFormat::SystemEndian)
      io.write_bytes @timestamp, format
      io.write_bytes AMQP::ShortString.new(@exchange_name), format
      io.write_bytes AMQP::ShortString.new(@routing_key), format
      io.write_bytes @properties, format
      io.write_bytes @size, format # bodysize
      if io_mem = @body_io.as?(IO::Memory)
        io.write(io_mem.to_slice)
      else
        copied = IO.copy(@body_io, io, @size)
        if copied != @size
          raise IO::Error.new("Could only write #{copied} of #{@size} bytes to message store")
        end
      end
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
