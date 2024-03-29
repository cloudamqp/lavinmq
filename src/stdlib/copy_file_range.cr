require "../lavinmq/mfile"

lib LibC
  {% if flag?(:linux) || flag?(:freebsd) %}
    fun copy_file_range(fd_in : Int, offset_in : OffT*, fd_out : Int, offset_out : OffT*, len : SizeT, flags : UInt) : Int
  {% end %}
end

module IO::Buffered
  # Unsafe, direct access to the underlying read buffer.
  protected def unsafe_read_buffer
    @in_buffer_rem
  end
end

class MFile < IO
  def copy_file_range(src : File, limit : Int)
    raise ArgumentError.new("Read buffering not supported") if src.read_buffering?
    raise IO::EOFError.new if @capacity < @size + limit

    remaining = limit.to_i64
    until remaining.zero?
      len = LibC.copy_file_range(src.fd, nil, fd, pointerof(@size), remaining, 0)
      # copy_file_range will update @size
      break if len <= 0 # fallback to buffer copying on error
      remaining &-= len
    end
    limit - remaining
  end

  def copy_file_range(src : MFile, limit : Int)
    raise IO::EOFError.new if @capacity < @size + limit
    raise IO::EOFError.new if src.pos + limit > src.size

    remaining = limit.to_i64
    until remaining.zero?
      len = LibC.copy_file_range(src.fd, pointerof(src.@pos), fd, pointerof(@size), remaining, 0)
      # copy_file_range will update src.@pos and @size
      break if len <= 0 # fallback to buffer copying on error
      remaining &-= len
    end
    limit - remaining
  end
end

class File
  def copy_file_range(src : MFile, limit : Int)
    raise ArgumentError.new("Write buffering not supported") unless sync?
    remaining = limit.to_i64
    until remaining.zero?
      len = LibC.copy_file_range(src.fd, pointerof(src.@pos), fd, nil, remaining, 0)
      # copy_file_range will update @pos
      break if len <= 0 # fallback to buffer copying on error
      remaining &-= len
    end
    limit - remaining
  end
end

class IO
  def self.copy(src, dst) : Int64
    count = 0_i64
    {% if LibC.has_method?(:copy_file_range) %}
      if src.is_a?(File) && dst.is_a?(File)
        dst.write(src.unsafe_read_buffer)
        count &+= src.unsafe_read_buffer.size
        src.skip(src.unsafe_read_buffer.size)
        dst.flush

        while (len = LibC.copy_file_range(src.fd, nil, dst.fd, nil, LibC::SSizeT::MAX, 0)) > 0 # fallback to buffer copying on error
          count &+= len
        end
        return count unless count.zero? # copy_file_range can return 0 on virtual FS files (e.g. /proc), if so then fallback
      end
    {% end %}

    buffer = uninitialized UInt8[DEFAULT_BUFFER_SIZE]
    while (len = src.read(buffer.to_slice).to_i32) > 0
      dst.write buffer.to_slice[0, len]
      count &+= len
    end
    count
  end

  def self.copy(src, dst, limit : Int) : Int64
    raise ArgumentError.new("Negative limit") if limit < 0

    remaining = limit = limit.to_i64

    {% if LibC.has_method?(:copy_file_range) %}
      if src.is_a?(File) && dst.is_a?(MFile)
        copied = dst.copy_file_range(src, limit)
        remaining &-= copied
        return limit if remaining.zero?
      elsif src.is_a?(MFile) && dst.is_a?(File)
        copied = dst.copy_file_range(src, limit)
        remaining &-= copied
        return limit if remaining.zero?
      elsif src.is_a?(MFile) && dst.is_a?(MFile)
        copied = dst.copy_file_range(src, limit)
        remaining &-= copied
        return limit if remaining.zero?
      elsif src.is_a?(File) && dst.is_a?(File)
        len = Math.min(limit, src.unsafe_read_buffer.size) # if copying less than the read buffer size
        dst.write(src.unsafe_read_buffer[0, len])
        src.skip(len)
        remaining &-= len
        return limit if remaining.zero?
        dst.flush

        while (len = LibC.copy_file_range(src.fd, nil, dst.fd, nil, remaining, 0)) > 0 # fallback to buffer copying on error
          remaining &-= len
          return limit if remaining.zero?
        end
      end
    {% end %}

    buffer = uninitialized UInt8[DEFAULT_BUFFER_SIZE]
    while (len = src.read(buffer.to_slice[0, Math.min(buffer.size, Math.max(remaining, 0))])) > 0
      dst.write buffer.to_slice[0, len]
      remaining &-= len
    end
    limit - remaining
  end
end
