require "amq-protocol"

module LavinMQ
  # Messages read from message store (mmap filed) and being delivered to consumers
  struct BytesMessage
    getter timestamp, exchange_name, routing_key, properties, bodysize, body

    MIN_BYTESIZE = 8 + 1 + 1 + 2 + 8 + 1

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQ::Protocol::Properties,
                   @bodysize : UInt64, @body : Bytes)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64) + @bodysize
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

    def offset : UInt64?
      @properties.headers.try(&.fetch("x-stream-offset", nil).as?(UInt64))
    end

    def dlrk : String?
      @properties.headers.try(&.fetch("x-dead-letter-routing-key", nil).as?(String))
    end

    def delay : UInt32?
      @properties.headers.try(&.fetch("x-delay", nil)).as?(Int).try(&.to_u32)
    rescue OverflowError
      nil
    end

    def self.skip(io, format = IO::ByteFormat::SystemEndian) : UInt64
      skipped = 0_u64
      skipped += io.skip(sizeof(UInt64))                             # ts
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # ex
      skipped += io.skip(io.read_byte || raise IO::EOFError.new) + 1 # rk
      skipped += AMQ::Protocol::Properties.skip(io, format)
      skipped += io.skip(UInt64.from_io io, format) + sizeof(UInt64)
      skipped
    end

    def self.from_bytes(bytes, format = IO::ByteFormat::SystemEndian) : self
      ts = format.decode(Int64, bytes[0, 8]); pos = 8
      ex = AMQ::Protocol::ShortString.from_bytes(bytes + pos); pos += 1 + ex.bytesize
      rk = AMQ::Protocol::ShortString.from_bytes(bytes + pos); pos += 1 + rk.bytesize
      pr = AMQ::Protocol::Properties.from_bytes(bytes + pos, format); pos += pr.bytesize
      sz = format.decode(UInt64, bytes[pos, 8]); pos += 8
      body = bytes[pos, sz]
      BytesMessage.new(ts, ex, rk, pr, sz, body)
    end

    def to_io(io : IO, format = IO::ByteFormat::SystemEndian)
      io.write_bytes @timestamp, format
      io.write_bytes AMQ::Protocol::ShortString.new(@exchange_name), format
      io.write_bytes AMQ::Protocol::ShortString.new(@routing_key), format
      io.write_bytes @properties, format
      io.write_bytes @bodysize, format
      io.write @body
    end
  end

  # Messages from publishers, read from socket and then written to mmap files
  struct Message
    property timestamp
    getter exchange_name, routing_key, properties, bodysize, body_io

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQ::Protocol::Properties,
                   @bodysize : UInt64, @body_io : IO)
    end

    def initialize(@exchange_name : String, @routing_key : String,
                   body : String, @properties = AMQ::Protocol::Properties.new)
      @timestamp = RoughTime.unix_ms
      @bodysize = body.bytesize.to_u64
      @body_io = IO::Memory.new(body)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64) + @bodysize
    end

    def persistent?
      @properties.delivery_mode == 2_u8
    end

    def dlx : String?
      @properties.headers.try(&.fetch("x-dead-letter-exchange", nil).as?(String))
    end

    def delay : UInt32?
      @properties.headers.try(&.fetch("x-delay", nil)).as?(Int).try(&.to_u32)
    rescue OverflowError
      nil
    end

    def to_io(io : IO, format = IO::ByteFormat::SystemEndian)
      io.write_bytes @timestamp, format
      io.write_bytes AMQ::Protocol::ShortString.new(@exchange_name), format
      io.write_bytes AMQ::Protocol::ShortString.new(@routing_key), format
      io.write_bytes @properties, format
      io.write_bytes @bodysize, format
      if io_mem = @body_io.as?(IO::Memory)
        io.write(io_mem.to_slice)
        io_mem.pos = @bodysize # necessary for rewinding in Vhost#publish
      else
        copied = IO.copy(@body_io, io, @bodysize)
        if copied != @bodysize
          raise IO::Error.new("Could only write #{copied} of #{@bodysize} bytes to message store")
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
