module LavinMQ
  module Stats
    # A group of UInt64 counters, each kept as one stripe per thread. A thread
    # adds to its own stripe with a plain load and store, so counters that
    # every connection bumps on every frame (vhost bytes in/out, exchange
    # publishes) stop bouncing a cache line between the worker threads.
    # Reading a counter sums its stripes.
    #
    # Each stripe is a 64-byte aligned block holding all the group's counters,
    # so one thread's writes never share a line with another's. The first
    # threads to add get a stripe of their own, up to the default execution
    # context's capacity; any thread after that (isolated contexts, threads
    # started before the context was resized) shares the last stripe through
    # atomic adds.
    class StripedCounters
      CACHE_LINE = 64

      @@stripes : Int32 = 0
      @@next_stripe = Atomic(Int32).new(0)

      # The stripe this thread adds to, assigned on its first add. Thread
      # locals are never scanned by the GC, which is fine for an Int32.
      @[ThreadLocal]
      @@stripe : Int32 = -1

      # One more than the context's capacity, the last stripe shared by any
      # thread beyond it. Fixed on first use, so a context resized later
      # only costs those threads the shared stripe.
      def self.stripes : Int32
        stripes = @@stripes
        return stripes unless stripes.zero?
        @@stripes = Fiber::ExecutionContext.default.capacity + 1
      end

      def self.shared_stripe : Int32
        stripes - 1
      end

      protected def self.stripe : Int32
        stripe = @@stripe
        return stripe unless stripe < 0
        shared = shared_stripe
        stripe = @@next_stripe.add(1, :relaxed)
        @@stripe = stripe < shared ? stripe : shared
      end

      @base : Pointer(UInt8)
      @counters : Pointer(UInt64)
      @stride : Int32

      def initialize(@size : Int32)
        raise ArgumentError.new("size must be positive") unless @size > 0
        @stride = ((@size * sizeof(UInt64) + CACHE_LINE - 1) // CACHE_LINE) * CACHE_LINE
        bytes = @stride * self.class.stripes + CACHE_LINE
        # Kept for the GC; the counters pointer is aligned up from it
        @base = GC.malloc_atomic(bytes.to_u32).as(UInt8*)
        @base.clear(bytes)
        aligned = (@base.address + CACHE_LINE - 1) & ~(CACHE_LINE - 1).to_u64
        @counters = Pointer(UInt64).new(aligned)
      end

      # Number of counters in the group
      getter size : Int32

      private def slot(stripe : Int32, index : Int32) : Pointer(UInt64)
        @counters + (stripe * @stride) // sizeof(UInt64) + index
      end

      def add(index : Int32, value : UInt64) : Nil
        raise IndexError.new unless 0 <= index < @size
        stripe = self.class.stripe
        ptr = slot(stripe, index)
        if stripe == self.class.shared_stripe
          Atomic::Ops.atomicrmw(:add, ptr, value, :monotonic, false)
        else
          Atomic::Ops.store(ptr, Atomic::Ops.load(ptr, :monotonic, false) &+ value, :monotonic, false)
        end
      end

      # Wrapping, so a stripe can go below zero while the sum stays right
      def sub(index : Int32, value : UInt64) : Nil
        add(index, 0_u64 &- value)
      end

      def get(index : Int32) : UInt64
        raise IndexError.new unless 0 <= index < @size
        sum = 0_u64
        self.class.stripes.times do |stripe|
          sum &+= Atomic::Ops.load(slot(stripe, index), :monotonic, false)
        end
        sum
      end
    end
  end
end
