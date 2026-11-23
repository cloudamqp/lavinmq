require "../stdlib/gc"

module LavinMQ
  # Reacts to systemd memory pressure events by collecting and returning free
  # heap memory to the OS, but only when that's likely to give back a
  # meaningful amount, and at most once per `min_interval`, as each
  # collection stops the world.
  #
  # Not thread safe: only called from the single systemd memory pressure
  # monitor thread.
  class MemoryPressureReclaimer
    @last_collect : Time::Instant? = nil

    def initialize(@min_interval : Time::Span = 30.seconds, @min_reclaim_ratio : Float64 = 0.1)
    end

    # Returns true if it collected
    def on_pressure(now : Time::Instant = Time.instant, stats : GC::Stats = GC.stats) : Bool
      if last = @last_collect
        return false if now - last < @min_interval
      end
      return false if stats.free_bytes + stats.bytes_since_gc < stats.heap_size * @min_reclaim_ratio
      @last_collect = now
      LibGC.gcollect_and_unmap
      true
    end
  end
end
