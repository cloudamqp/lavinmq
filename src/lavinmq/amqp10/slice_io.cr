module LavinMQ::AMQP10
  class SliceIO < IO
    @slice = Bytes.empty
    @pos = 0

    def reset(slice : Bytes) : Nil
      @slice = slice
      @pos = 0
    end

    def read(slice : Bytes) : Int32
      return 0 if @pos >= @slice.bytesize
      count = Math.min(slice.bytesize, @slice.bytesize - @pos)
      slice.copy_from(@slice[@pos, count])
      @pos += count
      count
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("SliceIO is read-only")
    end

    def seek(offset, whence : IO::Seek = IO::Seek::Set) : Nil
      new_pos = case whence
                in .set?
                  offset
                in .current?
                  @pos + offset
                in .end?
                  @slice.bytesize + offset
                end
      raise IO::Error.new("invalid seek") unless 0 <= new_pos <= @slice.bytesize
      @pos = new_pos.to_i
    end

    def pos : Int32
      @pos
    end
  end
end
