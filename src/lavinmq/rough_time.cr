# Cheap, low resolution clocks for hot paths.
#
# Reads the kernel's coarse clocks, which are served from the vDSO/commpage
# without a syscall and only updated at the timer tick (typically 1-4 ms on
# Linux). No background thread is needed, so nothing wakes up when idle.
#
# NOTE: `RoughTime.instant` must only be compared with other
# `RoughTime.instant` values, not with `Time.instant`. On Linux `Time.instant`
# uses `CLOCK_BOOTTIME` (includes suspended time) while there's no coarse
# variant of that clock, so `CLOCK_MONOTONIC_COARSE` is used here.
module RoughTime
  UNIX_EPOCH_IN_SECONDS = 62135596800_i64

  {% if flag?(:linux) %}
    REALTIME_CLOCK  = LibC::CLOCK_REALTIME_COARSE
    MONOTONIC_CLOCK = LibC::CLOCK_MONOTONIC_COARSE
  {% elsif flag?(:freebsd) || flag?(:dragonfly) %}
    REALTIME_CLOCK  = LibC::CLOCK_REALTIME_FAST
    MONOTONIC_CLOCK = LibC::CLOCK_MONOTONIC_FAST
  {% elsif flag?(:darwin) %}
    # CLOCK_REALTIME is already read from the commpage without a syscall.
    REALTIME_CLOCK = LibC::CLOCK_REALTIME
    # CLOCK_MONOTONIC_RAW_APPROX, same base as CLOCK_MONOTONIC_RAW used by
    # Time.instant, but only updated at context switches
    MONOTONIC_CLOCK = 5
  {% else %}
    REALTIME_CLOCK  = LibC::CLOCK_REALTIME
    MONOTONIC_CLOCK = LibC::CLOCK_MONOTONIC
  {% end %}

  def self.utc : Time
    ts = clock_gettime(REALTIME_CLOCK)
    Time.utc(seconds: ts.tv_sec.to_i64 + UNIX_EPOCH_IN_SECONDS, nanoseconds: ts.tv_nsec.to_i32)
  end

  def self.unix_ms : Int64
    ts = clock_gettime(REALTIME_CLOCK)
    ts.tv_sec.to_i64 * 1000 + ts.tv_nsec.to_i64 // 1_000_000
  end

  def self.instant : Time::Instant
    ts = clock_gettime(MONOTONIC_CLOCK)
    Time::Instant.new(seconds: ts.tv_sec.to_i64, nanoseconds: ts.tv_nsec.to_i32)
  end

  private def self.clock_gettime(clock) : LibC::Timespec
    ret = LibC.clock_gettime(clock, out ts)
    raise RuntimeError.from_errno("clock_gettime") unless ret == 0
    ts
  end
end
