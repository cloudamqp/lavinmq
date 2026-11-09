module LavinMQPerf
  # Smooth per-iteration pacer; when @rate is zero just yields periodically.
  # Reference type so the consume callback shares one instance across calls.
  # The rate is operations per second and may be a fraction: 0.2 is one
  # operation every 5 seconds.
  class Ticker
    SLEEP_GRANULARITY = 1.millisecond
    YIELD_INTERVAL    = 128 * 1024

    @rate : Float64
    @check_interval : UInt64

    def initialize(rate : Number)
      @rate = rate.to_f64
      @start = Time.instant
      @ops_done = 0_u64
      # Check the schedule about once per ms of work instead of every op. A clock
      # read is cheap on a healthy (vDSO) clocksource but slow on VMs that fall
      # back to a degraded one, where per-op reads would cap throughput. Robust
      # either way, and pacing is unaffected since the skip stays under SLEEP_GRANULARITY.
      @check_interval = @rate <= 1_000 ? 1_u64 : (@rate / 1_000).to_u64
    end

    def tick : Nil
      @ops_done &+= 1
      if @rate.zero?
        Fiber.yield if (@ops_done % YIELD_INTERVAL).zero?
        return
      end
      return unless (@ops_done % @check_interval).zero?

      target = (@ops_done / @rate).seconds
      delay = (@start + target) - Time.instant
      sleep delay if delay >= SLEEP_GRANULARITY
    end
  end
end
