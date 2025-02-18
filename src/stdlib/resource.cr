lib LibC
  fun setrlimit(Int, Rlimit*) : Int
  {% if flag?(:windows) %}
    fun _setmaxstdio(max : Int) : Int
  {% end %}
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
      @blocks_in = usage.ru_inblock
      @blocks_out = usage.ru_oublock
      @minor_page_faults = usage.ru_minflt
      @major_page_faults = usage.ru_majflt
      @voluntary_context_switches = usage.ru_nvcsw
      @involuntary_context_switches = usage.ru_nivcsw
      @max_rss = usage.ru_maxrss * 1024
      {% if flag?(:darwin) %}
        @max_rss = usage.ru_maxrss
      {% end %}
    end

    def initialize(user_time, sys_time, blocks_in, blocks_out, minor_page_faults, major_page_faults, voluntary_context_switches, involuntary_context_switches, max_rss)
      @user_time = Time::Span.new(seconds: user_time.to_i)
      @sys_time = Time::Span.new(seconds: sys_time.to_i)
      @blocks_in = blocks_in
      @blocks_out = blocks_out
      @minor_page_faults = minor_page_faults
      @major_page_faults = major_page_faults
      @voluntary_context_switches = voluntary_context_switches
      @involuntary_context_switches = involuntary_context_switches
      @max_rss = max_rss
    end

    getter user_time : Time::Span
    getter sys_time : Time::Span
    {% if flag?(:arm) %}
      getter max_rss : Int32
      getter blocks_in : Int32
      getter blocks_out : Int32
      getter minor_page_faults : Int32
      getter major_page_faults : Int32
      getter voluntary_context_switches : Int32
      getter involuntary_context_switches : Int32
    {% else %}
      getter max_rss : Int64
      getter blocks_in : Int64
      getter blocks_out : Int64
      getter minor_page_faults : Int64
      getter major_page_faults : Int64
      getter voluntary_context_switches : Int64
      getter involuntary_context_switches : Int64
    {% end %}
  end

  def self.resource_usage
    {% if flag?(:windows) %}
      times = Process.times
      ResourceUsage.new(times.@utime, times.@stime, 0_i64, 0_i64, 0_i64, 0_i64, 0_i64, 0_i64, 0_i64)
    {% else %}
      usg = uninitialized LibC::RUsage
      if LibC.getrusage(LibC::RUSAGE_SELF, pointerof(usg)) != 0
        raise Error.from_errno("rusage")
      end
      ResourceUsage.new(usg)
    {% end %}
  end

  def self.file_descriptor_limit
    {% if flag?(:windows) %}
      {2048, 2048}
    {% else %}
      rlimit = uninitialized LibC::Rlimit
      if LibC.getrlimit(LibC::RLIMIT_NOFILE, pointerof(rlimit)) != 0
        raise Error.from_errno("getrlimit")
      end
      {rlimit.rlim_cur, rlimit.rlim_max}
    {% end %}
  end

  def self.file_descriptor_limit=(limit) : Nil
    rlimit = LibC::Rlimit.new
    rlimit.rlim_cur = limit
    rlimit.rlim_max = limit
    if LibC.setrlimit(LibC::RLIMIT_NOFILE, pointerof(rlimit)) != 0
      raise Error.from_errno("setrlimit")
    end
  end

  def self.file_descriptor_count : Int32
    i = 0
    Dir.each_child("/proc/#{Process.pid}/fd") do
      i += 1
    end
    i
  rescue File::Error
    -1
  end

  def self.maximize_fd_limit
    _, fd_limit_max = System.file_descriptor_limit
    {% unless flag?(:windows) %}
      System.file_descriptor_limit = fd_limit_max
      fd_limit_current, _ = System.file_descriptor_limit
      fd_limit_current
    {% else %}
      LibC._setmaxstdio(2048)
    {% end %}
  end

  class Error < Exception
    include SystemError
  end
end
