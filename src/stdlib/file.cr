# No PR yet
class File
  def hint_target_size(size)
    {% if flag?(:linux) %}
      if LibC.fallocate(fd, LibC::FALLOC_FL_KEEP_SIZE, 0, size) != 0
        raise Errno.new("fallocate failed")
      end
    {% end %}
  end

  def advise(advice)
    {% if flag?(:linux) %}
      if LibC.posix_fadvise(fd, 0, 0, advice) != 0
        raise Errno.new("fadvise failed")
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
