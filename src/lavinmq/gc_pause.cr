module LavinMQ
  # Records Boehm GC stop-the-world pauses in a histogram.
  #
  # The collection event callback runs inside the collector with the
  # allocation lock held, so it must not allocate, log or touch fibers.
  # Counters are only written from that callback, readers may observe
  # a slightly stale but never torn value.
  #
  # The class vars below are initialized at program start. Don't make
  # them lazy: a lazy initializer is guarded by __crystal_once, which may
  # lock, and would first run inside the callback.
  module GCPauseRecorder
    BUCKETS = StaticArray[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]

    @@counts = StaticArray(UInt64, 11).new(0_u64) # last slot is +Inf
    @@sum = 0.0
    @@stop_started_at : Time::Instant? = nil
    @@previous : LibGC::OnCollectionEventProc? = nil
    @@installed = false

    def self.install : Nil
      return if @@installed
      @@installed = true
      previous = LibGC.get_on_collection_event
      @@previous = previous unless previous.pointer.null?
      LibGC.set_on_collection_event(->(event : LibGC::EventType) {
        case event
        when .pre_stop_world?
          @@stop_started_at = Time.instant
        when .post_start_world?
          if started_at = @@stop_started_at
            record (Time.instant - started_at).total_seconds
            @@stop_started_at = nil
          end
        end
        @@previous.try &.call(event)
      })
    end

    private def self.record(seconds : Float64) : Nil
      idx = BUCKETS.index { |le| seconds <= le } || BUCKETS.size
      @@counts[idx] &+= 1
      @@sum += seconds
    end

    def self.sum : Float64
      @@sum
    end

    # Yields each upper bound (nil for +Inf) with its cumulative count
    def self.each_bucket(& : Float64?, UInt64 ->) : Nil
      cumulative = 0_u64
      @@counts.each_with_index do |count, i|
        cumulative &+= count
        yield BUCKETS[i]?, cumulative
      end
    end
  end
end
