module LavinMQPerf
  # Smooth per-iteration pacer; when @rate is zero just yields periodically.
  # Reference type so the consume callback shares one instance across calls.
  class Ticker
    SLEEP_GRANULARITY = 1.millisecond
    YIELD_INTERVAL    = 128 * 1024

    @check_interval : UInt64

    def initialize(@rate : Int32)
      @start = Time.instant
      @ops_done = 0_u64
      # Check the schedule about once per ms of work instead of every op. A clock
      # read is cheap on a healthy (vDSO) clocksource but slow on VMs that fall
      # back to a degraded one, where per-op reads would cap throughput. Robust
      # either way, and pacing is unaffected since the skip stays under SLEEP_GRANULARITY.
      @check_interval = @rate <= 1_000 ? 1_u64 : (@rate // 1_000).to_u64
    end

    def tick : Nil
      @ops_done &+= 1
      if @rate.zero?
        Fiber.yield if (@ops_done % YIELD_INTERVAL).zero?
        return
      end
      return unless (@ops_done % @check_interval).zero?

      target_ns = @ops_done.to_i64 &* 1_000_000_000_i64 // @rate.to_i64
      delay = (@start + target_ns.nanoseconds) - Time.instant
      sleep delay if delay >= SLEEP_GRANULARITY
    end
  end
end
