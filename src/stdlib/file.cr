# No PR yet
class File
  def punch_hole(size, offset = 0, keep_size = true)
    {% if flag?(:linux) %}
      flags = LibC::FALLOC_FL_PUNCH_HOLE
      flags |= LibC::FALLOC_FL_KEEP_SIZE if keep_size
      if LibC.fallocate(fd, flags, offset, size) != 0
        raise File::Error.from_errno("fallocate", file: @path)
      end
    {% end %}
  end

  def allocate(size, offset = 0, keep_size = false)
    {% if flag?(:linux) %}
      flags = case
              when keep_size then LibC::FALLOC_FL_KEEP_SIZE
              end
      if LibC.fallocate(fd, flags, offset, size) != 0
        raise File::Error.from_errno("fallocate", file: @path)
      end
    {% end %}
  end

  def advise(advice)
    {% if flag?(:linux) %}
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
  def write_at(buffer, offset)
    bytes_written = LibC.pwrite(fd, buffer, buffer.size, offset)

    if bytes_written == -1
      raise IO::Error.from_errno "Error writing file"
    end

    bytes_written
  end
end
