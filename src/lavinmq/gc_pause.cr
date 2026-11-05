module LavinMQ
  # Records Boehm GC stop-the-world pauses in a histogram.
  #
  # The collection event callback runs inside the collector with the
  # allocation lock held, so it must not allocate, log or touch fibers.
  # Counters are written from that callback and read by scrapes on other
  # threads, so they're relaxed atomics.
  #
  # The class vars below are initialized at program start. Don't make
  # them lazy: a lazy initializer is guarded by __crystal_once, which may
  # lock, and would first run inside the callback.
  module GCPauseRecorder
    BUCKETS = StaticArray[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]

    # Last slot is +Inf. `uninitialized` because the global is zeroed anyway, and
    # pointerof on a class var with an initializer re-runs that initializer
    @@counts = uninitialized StaticArray(Atomic(UInt64), 11)
    # Nanoseconds, an integer so a long uptime doesn't accumulate float rounding
    @@sum_ns = Atomic(UInt64).new(0_u64)
    # Read and written only inside the callback, which the GC lock serializes
    @@stop_started_at : Time::Instant? = nil
    # Written before set_on_collection_event, whose GC lock publishes it to the callback
    @@previous : LibGC::OnCollectionEventProc? = nil
    # Only touched by install, which runs once at startup
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
            record (Time.instant - started_at).total_nanoseconds.to_u64
            @@stop_started_at = nil
          end
        end
        @@previous.try &.call(event)
      })
    end

    def self.record(nanoseconds : UInt64) : Nil
      seconds = nanoseconds / 1e9
      idx = BUCKETS.index { |le| seconds <= le } || BUCKETS.size
      counter(idx).value.add(1_u64, :relaxed)
      @@sum_ns.add(nanoseconds, :relaxed)
    end

    # Seconds
    def self.sum : Float64
      @@sum_ns.get(:relaxed) / 1e9
    end

    # Yields each upper bound (nil for +Inf) with its cumulative count
    def self.each_bucket(& : Float64?, UInt64 ->) : Nil
      cumulative = 0_u64
      @@counts.size.times do |i|
        cumulative &+= counter(i).value.get(:relaxed)
        yield BUCKETS[i]?, cumulative
      end
    end

    # StaticArray is a value type: indexing @@counts would act on a copy
    private def self.counter(idx : Int) : Atomic(UInt64)*
      pointerof(@@counts).as(Atomic(UInt64)*) + idx
    end
  end
end
