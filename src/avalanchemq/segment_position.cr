module AvalancheMQ
  struct SegmentPosition
    include Comparable(self)

    @[Flags]
    enum SPFlags : UInt8
      HasDLX
      HasExpiration
      HasPriority
    end

    getter segment : UInt32
    getter position : UInt32
    getter bytesize : UInt32
    getter flags : SPFlags
    getter expiration_ts : Int64
    getter priority : UInt8
    BYTESIZE = 22

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @bytesize = 0_u32, @expiration_ts = 0_i64, @priority = 0_u8, @flags = SPFlags.new(0_u8))
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

    def to_io(io : IO, format)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      format.encode(@bytesize, slice[8, 4])
      slice[12] = @flags.value
      len = 12
      if @flags.includes?(SPFlags::HasPriority)
        slice[13] = @priority
        len = 13
      end
      if @flags.includes?(SPFlags::HasExpiration)
        format.encode(@expiration_ts, slice[14, 8])
        len = 22
      end
      io.write(slice[0, len])
    end

    def <=>(other : self)
      r = segment <=> other.segment
      return r unless r.zero?
      position <=> other.position
    end

    def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
      seg = UInt32.from_io(io, format)
      pos = UInt32.from_io(io, format)
      bytesize = UInt32.from_io(io, format)
      flags_value = io.read_byte || raise IO::EOFError.new
      flags = SPFlags.from_value flags_value
      priority = (io.read_byte || raise IO::EOFError.new) if flags.includes?(SPFlags::HasPriority)
      ts = Int64.from_io(io, format) if flags.includes?(SPFlags::HasExpiration)
      self.new(seg, pos, bytesize, ts, priority, flags)
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
      expires_at =
        if delay = msg.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
          msg.timestamp + delay.to_i64
        elsif exp_ms = msg.properties.expiration.try(&.to_i64?)
          msg.timestamp + exp_ms
        else
          0_i64
        end
      priority = msg.properties.priority || 0_u8
      flags =
        if msg.properties.headers.try(&.has_key?("x-dead-letter-exchange"))
          SPFlags::HasDLX
        else
          SPFlags.new(0u8)
        end
      self.new(segment, position, msg.bytesize.to_u32, expires_at, priority, flags)
    end
  end
end
