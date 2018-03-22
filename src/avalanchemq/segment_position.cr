module AvalancheMQ
  struct SegmentPosition
    getter segment, position
    def initialize(@segment : UInt32, @position : UInt32)
    end

    def to_io(io : IO, format)
      io.write_bytes @segment, IO::ByteFormat::BigEndian
      io.write_bytes @position, IO::ByteFormat::BigEndian
    end

    def self.decode(io : IO)
      seg = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      pos = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      self.new(seg, pos)
    end
  end
end
