module LavinMQ
  struct SegmentPosition
    include Comparable(self)

    @[Flags]
    enum SPFlags : UInt8
      HasDLX
      HasTTL
    end

    # NOTE: order is important here
    getter segment : UInt32  # 4
    getter position : UInt32 # 4
    getter timestamp : Int64 # 8
    getter ttl : Int64       # 8
    getter bytesize : UInt32 # 4
    getter priority : UInt8  # 1
    getter flags : SPFlags   # 1
    BYTESIZE = 30

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @bytesize = 0_u32, @timestamp = 0_i64, @ttl = 0u32, @priority = 0_u8, @flags = SPFlags.new(0_u8))
    end

    def self.zero
      self.new(0_u32, 0_u32)
    end

    def zero?
      @segment.zero? && @position.zero?
    end

    def end_position
      @position + @bytesize
    end

    def expiration_ts : Int64
      @flags.has_ttl? ? @timestamp + @ttl : 0i64
    end

    def ttl? : UInt32?
      if @flags.has_ttl?
        @ttl
      end
    end

    def to_io(io : IO, format)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      format.encode(@bytesize, slice[8, 4])
      format.encode(@timestamp, slice[12, 8])
      format.encode(@ttl, slice[20, 8])
      slice[28] = @priority
      slice[29] = @flags.value
      io.write(slice)
    end

    def <=>(other : self)
      r = segment <=> other.segment
      return r unless r.zero?
      position <=> other.position
    end

    def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      io.read_fully(slice)
      seg = format.decode(UInt32, slice[0, 4])
      pos = format.decode(UInt32, slice[4, 4])
      bytesize = format.decode(UInt32, slice[8, 4])
      ts = format.decode(Int64, slice[12, 8])
      ttl = format.decode(Int64, slice[20, 8])
      priority = slice[28]
      flags = SPFlags.from_value slice[29]
      self.new(seg, pos, bytesize, ts, ttl, priority, flags)
    end

    def self.from_i64(i : Int64)
      seg = i.bits(32..)
      pos = i.bits(0..31)
      SegmentPosition.new(seg.to_u32, pos.to_u32)
    end

    def to_s(io : IO)
      io << @segment.to_s.rjust(10, '0')
      io << @position.to_s.rjust(10, '0')
    end

    def to_i64
      ((segment.to_i64 << 32) | position).to_i64
    end

    def self.parse(s)
      raise ArgumentError.new("A SegmentPosition string has to be 20 chars long") if s.bytesize != 20
      seg = s[0, 10].to_u32
      pos = s[10, 10].to_u32
      self.new seg, pos
    end

    def self.make(segment, position, msg)
      flags = SPFlags::None
      ttl = 0_i64
      if h = msg.properties.headers
        h.each do |key, value|
          case key
          when "x-delay"
            flags |= SPFlags::HasTTL
            ttl = value.as(ArgumentNumber).to_i64
          when "x-dead-letter-exchange"
            flags |= SPFlags::HasDLX
          end
        end
      end
      unless flags.has_ttl?
        if exp_ms = msg.properties.expiration.try(&.to_i64?)
          flags |= SPFlags::HasTTL
          ttl = exp_ms
        end
      end
      priority = msg.properties.priority || 0_u8
      self.new(segment, position, msg.bytesize.to_u32, msg.timestamp, ttl, priority, flags)
    end
  end
end
