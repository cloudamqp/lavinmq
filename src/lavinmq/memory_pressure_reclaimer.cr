require "../stdlib/gc"
require "./logger"

module LavinMQ
  # Reacts to systemd memory pressure events by collecting and returning free
  # heap memory to the OS, but only when that's likely to give back a
  # meaningful amount, and at most once per `min_interval`, as each
  # collection stops the world.
  #
  # Not thread safe: only called from the single systemd memory pressure
  # monitor thread.
  class MemoryPressureReclaimer
    Log = LavinMQ::Log.for "memory_pressure"

    @last_collect : Time::Instant? = nil

    def initialize(@min_interval : Time::Span = 30.seconds, @min_reclaim_ratio : Float64 = 0.1)
    end

    # Returns true if it collected
    def on_pressure(now : Time::Instant = Time.instant, stats : GC::Stats = GC.stats) : Bool
      if (last = @last_collect) && now - last < @min_interval
        Log.debug { "Not collecting, last collection #{(now - last).total_seconds.round(1)}s ago" }
        return false
      end
      reclaimable = stats.free_bytes + stats.bytes_since_gc
      if reclaimable < stats.heap_size * @min_reclaim_ratio
        Log.debug { "Not collecting, only #{reclaimable.humanize_bytes} of #{stats.heap_size.humanize_bytes} heap reclaimable" }
        return false
      end
      @last_collect = now
      collect(stats)
      true
    end

    private def collect(before : GC::Stats) : Nil
      LibGC.gcollect_and_unmap
      after = GC.stats
      Log.info do
        "Collected and unmapped, heap #{before.heap_size.humanize_bytes} -> #{after.heap_size.humanize_bytes}, " \
        "unmapped #{before.unmapped_bytes.humanize_bytes} -> #{after.unmapped_bytes.humanize_bytes}"
      end
    end
  end
end
