require "./libc"

# No PR yet
class File
  def advise(advice : Advice)
    {% if LibC.has_method?(:posix_fadvise) %}
      if LibC.posix_fadvise(fd, 0, 0, advice) != 0
        raise File::Error.from_errno("fadvise", file: @path)
      end
    {% end %}
  end

  enum Advice
    Normal
    Random
    Sequential
    WillNeed
    DontNeed
    NoReuse
  end
end

class IO::FileDescriptor
  def write_at(buffer, offset) : Int64
    bytes_written = LibC.pwrite(fd, buffer, buffer.size, offset)

    if bytes_written == -1
      raise IO::Error.from_errno "Error writing file"
    end

    bytes_written.to_i64
  end
end
