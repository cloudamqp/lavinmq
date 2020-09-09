module AvalancheMQ
  struct SegmentPosition
    include Comparable(self)

    getter segment : UInt32
    getter position : UInt32
    getter expiration_ts : Int64

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @expiration_ts : Int64 = 0_i64)
    end

    def self.zero
      self.new(0_u32, 0_u32)
    end

    def to_io(io : IO, format)
      buf = uninitialized UInt8[sizeof(SegmentPosition)]
      slice = buf.to_slice
      format.encode(@segment, slice[0, 4])
      format.encode(@position, slice[4, 4])
      format.encode(@expiration_ts, slice[8, 8])
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
      ts = Int64.from_io(io, format)
      self.new(seg, pos, ts)
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
  end
end
