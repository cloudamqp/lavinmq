{% unless flag?(:gc_none) %}
  lib LibGC
    fun set_min_bytes_allocd = GC_set_min_bytes_allocd(value : SizeT)
    fun get_min_bytes_allocd = GC_get_min_bytes_allocd : SizeT
  end

  module LavinMQ
    # Experimental. Measures GC pause time and, when LAVINMQ_GC_AUTOTUNE is set,
    # runs a feedback loop that resizes bdwgc's allocation floor
    # (min_bytes_allocd) to hold GC time near a target fraction of wall time,
    # in the spirit of Go's GOGC/GOMEMLIMIT and HotSpot's GCTimeRatio.
    #
    #   LAVINMQ_GC_MIN_BYTES=N      static floor in bytes (no feedback)
    #   LAVINMQ_GC_AUTOTUNE=PCT     target GC time in % of wall time, e.g. 1
    #   LAVINMQ_GC_AUTOTUNE_MAX=N   upper bound for the floor (default 256 MiB)
    module GCTuner
      Log = LavinMQ::Log.for "gc_tuner"

      @@collections = Atomic(UInt64).new(0)
      @@pause_ns = Atomic(UInt64).new(0)
      @@max_pause_ns = Atomic(UInt64).new(0)
      @@start_ns = Atomic(UInt64).new(0)
      @@floor = Atomic(UInt64).new(0)
      @@adjustments = Atomic(UInt64).new(0)

      def self.collections : UInt64
        @@collections.get(:relaxed)
      end

      def self.pause_ns : UInt64
        @@pause_ns.get(:relaxed)
      end

      def self.max_pause_ns : UInt64
        @@max_pause_ns.get(:relaxed)
      end

      def self.floor : UInt64
        @@floor.get(:relaxed)
      end

      def self.adjustments : UInt64
        @@adjustments.get(:relaxed)
      end

      def self.stats
        {
          collections:      collections,
          pause_ns_total:   pause_ns,
          pause_ns_max:     max_pause_ns,
          min_bytes_allocd: LibGC.get_min_bytes_allocd,
          adjustments:      adjustments,
        }
      end

      # :nodoc:
      # Called by the collecting thread with the GC lock held: must not allocate.
      def self.on_event(event : LibGC::EventType) : Nil
        case event
        when .start?
          @@start_ns.set(now_ns, :relaxed)
        when .end?
          d = now_ns &- @@start_ns.get(:relaxed)
          @@collections.add(1, :relaxed)
          @@pause_ns.add(d, :relaxed)
          @@max_pause_ns.max(d, :relaxed)
        end
      end

      private def self.now_ns : UInt64
        s, ns = Crystal::System::Time.monotonic
        s.to_u64 &* 1_000_000_000 &+ ns.to_u64
      end

      def self.setup : Nil
        LibGC.set_on_collection_event ->(e : LibGC::EventType) { GCTuner.on_event(e) }
        if v = ENV["LAVINMQ_GC_MIN_BYTES"]?.try &.to_u64?
          set_floor(v)
          Log.info { "min_bytes_allocd=#{v}" }
        end
        if target = ENV["LAVINMQ_GC_AUTOTUNE"]?.try &.to_f?
          max = ENV["LAVINMQ_GC_AUTOTUNE_MAX"]?.try(&.to_u64?) || 256_u64 << 20
          Log.info { "autotune target=#{target}% max=#{max}" }
          spawn(name: "GCTuner") { Controller.new(target / 100, max).run }
        end
      end

      protected def self.set_floor(v : UInt64) : Nil
        @@floor.set(v, :relaxed)
        LibGC.set_min_bytes_allocd(v)
      end

      protected def self.adjusted : Nil
        @@adjustments.add(1, :relaxed)
      end

      # Multiplicative increase when GC time is above target, gentle decrease
      # when well below, so the heap shrinks back once load goes away.
      class Controller
        BASE     = 256_u64 << 10 # bdwgc's effective floor on a small heap
        INTERVAL = 1.second

        @floor : UInt64

        def initialize(@target : Float64, @max : UInt64)
          @floor = GCTuner.floor.clamp(BASE, @max)
        end

        # Returns the new floor given GC time fraction of the last interval.
        def step(frac : Float64) : UInt64
          if frac > @target * 1.5
            @floor = (@floor * 2).clamp(BASE, @max)
          elsif frac < @target / 4
            @floor = (@floor - @floor // 4).clamp(BASE, @max)
          end
          @floor
        end

        def run
          last_pause = GCTuner.pause_ns
          last_t = Time.monotonic
          loop do
            sleep INTERVAL
            pause = GCTuner.pause_ns
            t = Time.monotonic
            frac = (pause - last_pause) / (t - last_t).total_nanoseconds
            last_pause, last_t = pause, t
            prev = @floor
            if step(frac) != prev
              GCTuner.set_floor(@floor)
              GCTuner.adjusted
              Log.debug { "gc_frac=#{(frac * 100).round(2)}% floor #{prev} -> #{@floor}" }
            end
          end
        end
      end
    end
  end
{% end %}
