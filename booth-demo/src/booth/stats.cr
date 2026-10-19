module Booth
  # A counter that can be marked from any fiber, sampled once per second into
  # a rate history the dashboard plots.
  class Meter
    HISTORY = 120

    @count = Atomic(Int64).new(0)
    @last_count = 0i64
    @last_at : Time::Instant = Time.instant
    @history = Deque(Float64).new(HISTORY)

    def mark(n = 1) : Nil
      @count.add(n.to_i64, :relaxed)
    end

    def count : Int64
      @count.get(:relaxed)
    end

    def rate : Float64
      @history.last? || 0.0
    end

    def history : Array(Float64)
      @history.to_a
    end

    # Records the rate since the previous tick
    def tick(now = Time.instant) : Nil
      count = self.count
      elapsed = (now - @last_at).total_seconds
      rate = elapsed > 0 ? (count - @last_count) / elapsed : 0.0
      @last_count = count
      @last_at = now
      @history.shift if @history.size >= HISTORY
      @history << Math.max(rate, 0.0)
    end

    def reset : Nil
      @count.set(0, :relaxed)
      @last_count = 0i64
      @history.clear
    end
  end

  # Paces a loop to a target rate without sleeping per message: it sleeps in
  # ~10 ms slices and yields how many messages are due in each.
  class Pacer
    property rate : Int32

    def initialize(@rate)
      @started = Time.instant
      @sent = 0i64
    end

    def restart : Nil
      @started = Time.instant
      @sent = 0i64
    end

    # Returns how many messages to send now, sleeping when none are due
    def due : Int32
      loop do
        if @rate <= 0
          sleep 100.milliseconds
          return 0
        end
        elapsed = (Time.instant - @started).total_seconds
        target = (elapsed * @rate).to_i64
        # Don't build up a burst after a stall (e.g. a failover)
        if target - @sent > @rate
          restart
          next
        end
        n = target - @sent
        if n > 0
          @sent += n
          return n.to_i32
        end
        sleep 10.milliseconds
      end
    end
  end
end
