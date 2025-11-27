module LavinMQ
  # Pool of ExecutionContext instances for VHost reuse
  # Prevents thread leaks by reusing ExecutionContexts instead of creating new ones
  class ExecutionContextPool
    Log = LavinMQ::Log.for("execution_context_pool")

    @@instance : ExecutionContextPool = self.new

    def self.instance : ExecutionContextPool
      @@instance
    end

    def initialize(pool_size = 128)
      @pool = Channel(Fiber::ExecutionContext).new(pool_size)
      @created_count = Atomic(Int32).new(0)
    end

    # Get an ExecutionContext from the pool or create a new one if pool is empty
    def acquire : Fiber::ExecutionContext
      select
      when ec = @pool.receive
        return ec
      else
        # Pool is empty, create new ExecutionContext
        count = @created_count.add(1, :relaxed)
        Log.debug { "Creating new ExecutionContext (total created: #{count})" }
        Fiber::ExecutionContext::Concurrent.new("VHost-EC-#{count}")
      end
    end

    # Return an ExecutionContext to the pool for reuse
    def release(ec : Fiber::ExecutionContext) : Nil
      # Try to return to pool without blocking
      select
      when @pool.send(ec)
        Log.debug { "Returned ExecutionContext to pool" }
      else
        # Pool is full, ExecutionContext will be garbage collected
        Log.debug { "Pool full, discarding ExecutionContext" }
      end
    end

    # Get current pool statistics
    def stats : NamedTuple(created: Int32, available: Int32)
      {
        created:   @created_count.get(:relaxed),
        available: @pool.size,
      }
    end
  end
end
