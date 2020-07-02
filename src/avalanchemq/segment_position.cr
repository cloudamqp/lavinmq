module AvalancheMQ
  struct SegmentPosition
    include Comparable(self)

    getter segment : UInt32
    getter position : UInt32
    getter bytesize : UInt32
    getter expiration_ts : Int64
    getter priority : UInt8
    BYTESIZE = 21

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @bytesize : UInt32 = 0_u32, @expiration_ts : Int64 = 0_i64, @priority : UInt8 = 0_u8)
    end

    def self.zero
      self.new(0_u32, 0_u32)
    end

    def to_io(io : IO, format)
      buf = uninitialized UInt8[BYTESIZE]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      format.encode(@bytesize, slice[8, 4])
      format.encode(@expiration_ts, slice[12, 8])
      format.encode(@priority, slice[20, 1])
      io.write(slice)
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
      ts = Int64.from_io(io, format)
      priority = UInt8.from_io(io, format)
      self.new(seg, pos, bytesize, ts, priority)
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
        self.new(segment, position, msg.bytesize.to_u32, expires_at, priority)
    end
  end
end
