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

  struct Envelope
    getter segment_position, message, redelivered

    def initialize(@segment_position : SegmentPosition, @message : BytesMessage,
                   @redelivered = false)
    end
  end

  # Heap-only copy of a message for the HTTP peek API. A BytesMessage's body
  # and header table are slices into a mmap:ed segment file that can be
  # unmapped at any time, so peek copies them while holding the message store
  # lock and serializes from the copy after releasing it.
  struct PeekedMessage
    getter exchange_name : String
    getter routing_key : String
    getter properties : AMQ::Protocol::Properties
    getter bodysize : UInt64
    getter body : Bytes
    getter? redelivered : Bool

    def initialize(msg : BytesMessage, max_body : Int32, @redelivered : Bool)
      @exchange_name = msg.exchange_name
      @routing_key = msg.routing_key
      @properties = copy_properties(msg.properties)
      @bodysize = msg.bodysize
      @body = msg.body[0, Math.min(msg.body.size, max_body)].dup
    end

    # Strings in Properties are heap allocated when decoded, but the headers
    # table is a slice into the source bytes and must be copied.
    private def copy_properties(pr : AMQ::Protocol::Properties) : AMQ::Protocol::Properties
      AMQ::Protocol::Properties.new(
        pr.content_type, pr.content_encoding, pr.headers.try(&.clone),
        pr.delivery_mode, pr.priority, pr.correlation_id, pr.reply_to,
        pr.expiration, pr.message_id, pr.timestamp_raw, pr.type,
        pr.user_id, pr.app_id, pr.reserved1)
    end
  end
end
