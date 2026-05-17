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

    def ttl
      @properties.expiration.try(&.to_i64?)
    end

    def dlx : String?
      @properties.headers.try(&.fetch("x-dead-letter-exchange", nil).as?(String))
    end

    def dlrk : String?
      @properties.headers.try(&.fetch("x-dead-letter-routing-key", nil).as?(String))
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
      io.write @body
    end

    def self.skip(io, format = IO::ByteFormat::SystemEndian) : UInt64
      skipped = 0_u64
      skipped += io.skip(sizeof(UInt64))                             # ts
      skipped += 1 + io.skip(io.read_byte || raise IO::EOFError.new) # ex
      skipped += 1 + io.skip(io.read_byte || raise IO::EOFError.new) # rk
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

    def to_io(io : IO, format = IO::ByteFormat::SystemEndian)
      io.write_bytes @timestamp, format
      io.write_bytes AMQ::Protocol::ShortString.new(@exchange_name), format
      io.write_bytes AMQ::Protocol::ShortString.new(@routing_key), format
      io.write_bytes @properties, format
      io.write_bytes @bodysize, format
      if io_mem = @body_io.as?(IO::Memory)
        slice = io_mem.to_slice
        raise IO::Error.new("Unexpected body size: #{slice.bytesize} != #{@bodysize}") if slice.bytesize != @bodysize
        io.write(slice)
        io_mem.pos = @bodysize # necessary for rewinding in Vhost#publish
      else
        copied = IO.copy(@body_io, io, @bodysize)
        if copied != @bodysize
          raise IO::Error.new("Could only write #{copied} of #{@bodysize} bytes to message store")
        end
      end
    end
  end

  # A range of bytes inside a `File`. Used by `FileRangeMessage` to refer to a
  # message body still on disk, so the body bytes are read straight into the
  # socket buffer instead of being copied off the segment's mmap onto the heap.
  record FileRange, file : File, pos : Int64, length : UInt64

  # Messages read by a stream consumer that owns its own `File` handle for the
  # current segment. The header (`timestamp`, `exchange_name`, `routing_key`,
  # `properties`, `bodysize`) is decoded eagerly via `from_io`; the body is
  # left on disk and referenced as a `FileRange` so the delivery path can read
  # it sequentially through the consumer's FD into the socket. Because the FD
  # is single-owner and offsets are monotonic, `file.pos` after `from_io` is
  # exactly the body start — no `pread` is needed.
  struct FileRangeMessage
    getter timestamp, exchange_name, routing_key, properties, bodysize, body

    def initialize(@timestamp : Int64, @exchange_name : String,
                   @routing_key : String, @properties : AMQ::Protocol::Properties,
                   @bodysize : UInt64, @body : FileRange)
    end

    def bytesize
      sizeof(Int64) + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize +
        @properties.bytesize + sizeof(UInt64) + @bodysize
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

    def delay : UInt32?
      @properties.headers.try(&.fetch("x-delay", nil)).as?(Int).try(&.to_u32)
    rescue OverflowError
      nil
    end

    # Decode a message header sequentially from `file`, leaving `file.pos` at
    # the body start. The returned `FileRangeMessage` carries a `FileRange`
    # pointing at that position; the caller will read the body sequentially
    # from the same FD (so `file.pos` lands at the next message after).
    def self.from_io(file : File, format = IO::ByteFormat::SystemEndian) : self
      ts = Int64.from_io(file, format)
      ex = AMQ::Protocol::ShortString.from_io(file, format)
      rk = AMQ::Protocol::ShortString.from_io(file, format)
      pr = AMQ::Protocol::Properties.from_io(file, format)
      sz = UInt64.from_io(file, format)
      body_pos = file.pos
      new(ts, ex, rk, pr, sz, FileRange.new(file, body_pos, sz))
    end
  end

  struct Envelope
    getter segment_position, message, redelivered

    def initialize(@segment_position : SegmentPosition,
                   @message : BytesMessage | FileRangeMessage,
                   @redelivered = false)
    end
  end
end
