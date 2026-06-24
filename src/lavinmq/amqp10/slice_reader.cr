module LavinMQ::AMQP10
  class SliceReader
    getter pos

    def initialize(@slice : Bytes = Bytes.empty)
      @pos = 0
    end

    def reset(@slice : Bytes) : self
      @pos = 0
      self
    end

    def remaining : Int32
      @slice.bytesize - @pos
    end

    def empty? : Bool
      remaining <= 0
    end

    def read_byte : UInt8
      raise IO::EOFError.new if @pos >= @slice.bytesize
      byte = @slice[@pos]
      @pos += 1
      byte
    end

    def read_slice(size : Int) : Bytes
      raise IO::EOFError.new if size < 0 || remaining < size
      start = @pos
      @pos += size
      @slice[start, size]
    end

    def read_string(size : Int) : String
      String.new(read_slice(size))
    end

    def skip(size : Int) : Nil
      raise IO::EOFError.new if size < 0 || remaining < size
      @pos += size
    end

    def read_u16 : UInt16
      IO::ByteFormat::NetworkEndian.decode(UInt16, read_slice(2))
    end

    def read_u32 : UInt32
      IO::ByteFormat::NetworkEndian.decode(UInt32, read_slice(4))
    end

    def read_u64 : UInt64
      IO::ByteFormat::NetworkEndian.decode(UInt64, read_slice(8))
    end

    def read_i32 : Int32
      IO::ByteFormat::NetworkEndian.decode(Int32, read_slice(4))
    end

    def read_i16 : Int16
      IO::ByteFormat::NetworkEndian.decode(Int16, read_slice(2))
    end

    def read_i64 : Int64
      IO::ByteFormat::NetworkEndian.decode(Int64, read_slice(8))
    end

    def read_f32 : Float32
      IO::ByteFormat::NetworkEndian.decode(Float32, read_slice(4))
    end

    def read_f64 : Float64
      IO::ByteFormat::NetworkEndian.decode(Float64, read_slice(8))
    end

    def remaining_slice : Bytes
      @slice[@pos, remaining]
    end
  end
end
