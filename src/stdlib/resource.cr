lib Resource
  RLIM_INFINITY = (1_u64 << 63) - 1
end

lib LibC
  {% if flag?(:darwin) %}
    RLIMIT_NOFILE = 7
  {% end %}

  {% if flag?(:linux) %}
    RLIMIT_NOFILE = 7

    alias RlimT = ULongLong

    struct Rlimit
      rlim_cur : RlimT
      rlim_max : RlimT
    end
  {% end %}

  fun getrlimit(Int, Rlimit*) : Int
  fun setrlimit(Int, Rlimit*) : Int
end

struct Time::Span
  def self.from_timeval(val)
    self.new(seconds: val.tv_sec.to_i64,
             nanoseconds: val.tv_usec.to_i64 * ::Time::NANOSECONDS_PER_MICROSECOND)
  end
end

module System
  struct ResourceUsage
    def initialize(usage)
      @user_time = Time::Span.from_timeval(usage.ru_utime)
      @sys_time = Time::Span.from_timeval(usage.ru_stime)
      @max_rss = usage.ru_maxrss
      @blocks_in = usage.ru_inblock
      @blocks_out = usage.ru_oublock
    end

    getter user_time : Time::Span
    getter sys_time : Time::Span
    getter max_rss : Int64
    getter blocks_in : Int64
    getter blocks_out : Int64
  end

  def self.resource_usage
    usg = uninitialized LibC::RUsage
    if LibC.getrusage(LibC::RUSAGE_SELF, pointerof(usg)) != 0
      raise Errno.new("rusage")
    end
    ResourceUsage.new(usg)
  end

  def self.file_descriptor_limit
    rlimit = uninitialized LibC::Rlimit
    if LibC.getrlimit(LibC::RLIMIT_NOFILE, pointerof(rlimit)) != 0
      raise Errno.new("getrlimit")
    end
    rlimit.rlim_cur
  end

  def self.file_descriptor_limit=(limit) : Nil
    rlimit = LibC::Rimit.new(limit, limit)
    if LibC.setrlimit(LibC::RLIMIT_NOFILE, pointerof(rlimit)) != 0
      raise Errno.new("setrlimit")
    end
  end
end
