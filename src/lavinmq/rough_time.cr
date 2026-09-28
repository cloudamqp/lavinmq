require "../stdlib/channel"

# Cached clocks for hot paths, updated every 100ms by a background ticker.
#
# Reading the real clocks costs ~30ns, which adds up at several reads per
# message. To not wake up 10 times per second when nothing reads the time, the
# ticker parks after PARK_AFTER ticks without any readers. The first read while
# parked refreshes the values itself (so it never returns a stale time) and
# wakes the ticker up again.
module RoughTime
  TICK       = 100.milliseconds
  PARK_AFTER = 10

  @@utc = Time.utc
  @@unix_ms : Int64 = @@utc.to_unix_ms // 100 * 100
  @@instant = Time.instant
  @@used = Atomic(Bool).new(false)
  @@parked = Atomic(Bool).new(false)
  @@wakeup = ::Channel(Nil).new(1)

  Fiber::ExecutionContext::Isolated.new("RoughTime") do
    idle_ticks = 0
    loop do
      sleep TICK
      refresh
      if @@used.swap(false, :relaxed)
        idle_ticks = 0
      elsif (idle_ticks += 1) >= PARK_AFTER
        @@parked.set(true, :release)
        @@wakeup.receive
        idle_ticks = 0
      end
    end
  end

  def self.utc : Time
    touch
    @@utc
  end

  def self.unix_ms : Int64
    touch
    @@unix_ms
  end

  def self.instant : Time::Instant
    touch
    @@instant
  end

  # :nodoc:
  def self.parked? : Bool
    @@parked.get(:acquire)
  end

  @[AlwaysInline]
  private def self.touch : Nil
    if @@parked.get(:acquire)
      unpark
    elsif !@@used.get(:relaxed)
      @@used.set(true, :relaxed)
    end
  end

  private def self.unpark : Nil
    refresh
    @@used.set(true, :relaxed)
    _, unparked = @@parked.compare_and_set(true, false, :acquire_release, :relaxed)
    @@wakeup.try_send?(nil) if unparked
  end

  private def self.refresh : Nil
    @@utc = utc = Time.utc
    @@unix_ms = utc.to_unix_ms // 100 * 100
    @@instant = Time.instant
  end
end
