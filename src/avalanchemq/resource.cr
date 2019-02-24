lib Resource
  RLIMIT_NOFILE = 8

  struct RLimit
    soft_limit : UInt64
    hard_limit : UInt64
  end

  RUSAGE_SELF = 0
  RLIM_INFINITY = (1_u64 << 63) - 1

  struct RUsage
    utime : LibC::Timeval # user time used
    stime : LibC::Timeval # system time used
    maxrss : UInt64 # max resident set size
    ixrss : UInt64 # integral shared text memory size
    idrss : UInt64 # integral unshared data size
    isrss : UInt64 # integral unshared stack size
    minflt : UInt64 # page reclaims
    majflt : UInt64 # page faults
    nswap : UInt64 # swaps
    inblock : UInt64 # block input operations
    oublock : UInt64 # block output operations
    msgsnd : UInt64 # messages sent
    msgrcv : UInt64 # messages received
    nsignals : UInt64 # signals received
    nvcsw : UInt64 # voluntary context switches
    nivcsw : UInt64 # involuntary context switches
  end

  fun getrusage(who : Int32, rusage : RUsage*) : Int32
  fun getrlimit(resource : Int32, rlimit : RLimit*) : Int32
  fun setrlimit(resource : Int32, rlimit : RLimit*) : Int32
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
      @user_time = Time::Span.from_timeval(usage.utime)
      @sys_time = Time::Span.from_timeval(usage.stime)
      @max_rss = usage.maxrss
      @blocks_in = usage.inblock
      @blocks_out = usage.oublock
    end

    getter user_time : Time::Span
    getter sys_time : Time::Span
    getter max_rss : UInt64
    getter blocks_in : UInt64
    getter blocks_out : UInt64
  end

  def self.resource_usage
    usg = uninitialized Resource::RUsage
    if Resource.getrusage(Resource::RUSAGE_SELF, pointerof(usg)) != 0
      raise Errno.new("rusage")
    end
    ResourceUsage.new(usg)
  end

  def self.file_descriptor_limit
    rlimit = uninitialized Resource::RLimit
    if Resource.getrlimit(Resource::RLIMIT_NOFILE, pointerof(rlimit)) != 0
      raise Errno.new("getrlimit")
    end
    rlimit
  end

  def self.file_descriptor_limit=(limit) : Nil
    rlimit = Resource::RLimit.new(soft_limit: limit, hard_limit: limit)
    if Resource.setrlimit(Resource::RLIMIT_NOFILE, pointerof(rlimit)) != 0
      raise Errno.new("setrlimit")
    end
  end
end
