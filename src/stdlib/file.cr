# No PR yet
class File
  def allocate(size, offset = 0, keep_size = false)
    {% if flag?(:linux) %}
      flags = case
              when keep_size then LibC::FALLOC_FL_KEEP_SIZE
              end
      if LibC.fallocate(fd, flags, offset, size) != 0
        raise Errno.new("fallocate")
      end
    {% end %}
  end

  def advise(advice)
    {% if flag?(:linux) %}
      if LibC.posix_fadvise(fd, 0, 0, advice) != 0
        raise Errno.new("fadvise")
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
