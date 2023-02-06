module LavinMQ
  struct SegmentPosition
    include Comparable(self)

    getter segment : UInt32
    getter position : UInt32
    getter bytesize : UInt32
    getter priority : UInt8
    getter? has_dlx : Bool

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @bytesize : UInt32, @has_dlx : Bool, @priority : UInt8)
    end

    def self.make(segment, position, msg)
      self.new(segment, position, msg.bytesize.to_u32, !!msg.dlx, msg.properties.priority || 0u8)
    end

    def <=>(other : self)
      r = segment <=> other.segment
      return r unless r.zero?
      position <=> other.position
    end

    def to_s(io : IO)
      io << @segment.to_s.rjust(10, '0')
      io << @position.to_s.rjust(10, '0')
    end

    # Used in persistent exchange
    def to_i64 : Int64
      ((segment.to_i64 << 32) | position).to_i64
    end

    # Used in persistent exchange
    def self.from_i64(i : Int64)
      seg = i.bits(32..)
      pos = i.bits(0..31)
      SegmentPosition.new(seg.to_u32, pos.to_u32, 0u32, false, 0u8)
    end
  end
end
