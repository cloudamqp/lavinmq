require "./spec_helper"

module LavinMQ
  # 3 rate + 2 count keys: exercises non-zero column offsets (i*cap) and the
  # path where one key diverging materializes all columns (others back-fill).
  class MultiProbe
    include Stats
    rate_stats({"a", "b", "c"}, {"m", "n"})

    @m = 0_u32
    @n = 0_u32

    def m
      @m
    end

    def n
      @n
    end

    def bump(da : UInt64, db : UInt64, dc : UInt64, mv : UInt32, nv : UInt32)
      @a_count.add(da)
      @b_count.add(db)
      @c_count.add(dc)
      @m = mv
      @n = nv
    end

    def constant?
      @_stats_rate_buffer.null?
    end
  end
end

# Inline PRNG (not stdlib Random) so the sequence and coverage are identical on
# every machine and Crystal version.
private class Xorshift
  def initialize(seed : UInt32)
    @s = seed.zero? ? 0x9e3779b9_u32 : seed
  end

  def u32 : UInt32
    x = @s
    x ^= x << 13
    x ^= x >> 17
    x ^= x << 5
    @s = x
    x
  end

  def below(n : Int32) : Int32
    (u32 % n.to_u32).to_i
  end
end

private def rate_for(dx : UInt64, interval : Float64) : Float64
  dx.zero? ? 0.0 : (dx / interval).round(1)
end

private def check(view, oracle, do_json) : Bool
  return false if view.size != oracle.size
  view.size.times do |i|
    return false if view[i] != oracle[i]
  end
  return false if do_json && view.to_json != oracle.to_a.to_json
  true
end

describe "constant-encoding differential vs Deque oracle (deterministic, multi-column)" do
  it "matches per-column Deque logs over a deterministic sequence (wrap + materialize + compact)" do
    orig_cap = LavinMQ::Config.instance.stats_log_size
    orig_int = LavinMQ::Config.instance.stats_interval
    begin
      LavinMQ::Config.instance.stats_interval = 5000
      interval = 5.0

      [1, 2, 3, 6, 120].each do |cap|
        LavinMQ::Config.instance.stats_log_size = cap
        saw_buffered = 0
        compactions = 0
        steps_total = 0

        (1_u32..24_u32).each do |stream|
          rng = Xorshift.new(stream)
          p = LavinMQ::MultiProbe.new
          orates = Array.new(3) { Deque(Float64).new }
          ocounts = Array.new(2) { Deque(UInt32).new }
          da = db = dc = 0_u64
          mv = nv = 0_u32
          was_constant = true

          250.times do |step|
            da = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
            db = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
            dc = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
            mv = rng.below(50).to_u32 if rng.below(10).zero?
            nv = rng.below(50).to_u32 if rng.below(10).zero?

            p.bump(da, db, dc, mv, nv)
            p.update_rates

            orates[0].push rate_for(da, interval); orates[0].shift if orates[0].size > cap
            orates[1].push rate_for(db, interval); orates[1].shift if orates[1].size > cap
            orates[2].push rate_for(dc, interval); orates[2].shift if orates[2].size > cap
            ocounts[0].push mv; ocounts[0].shift if ocounts[0].size > cap
            ocounts[1].push nv; ocounts[1].shift if ocounts[1].size > cap

            do_json = step % 16 == 0
            ok = check(p.a_log, orates[0], do_json) && check(p.b_log, orates[1], do_json) &&
                 check(p.c_log, orates[2], do_json) && check(p.m_log, ocounts[0], do_json) &&
                 check(p.n_log, ocounts[1], do_json)
            unless ok
              fail "cap=#{cap} stream=#{stream} step=#{step} constant?=#{p.constant?}\n" \
                   "  a got=#{p.a_log.to_a} exp=#{orates[0].to_a}\n  b got=#{p.b_log.to_a} exp=#{orates[1].to_a}\n" \
                   "  c got=#{p.c_log.to_a} exp=#{orates[2].to_a}\n  m got=#{p.m_log.to_a} exp=#{ocounts[0].to_a}\n" \
                   "  n got=#{p.n_log.to_a} exp=#{ocounts[1].to_a}"
            end

            saw_buffered += 1 unless p.constant?
            compactions += 1 if p.constant? && !was_constant
            was_constant = p.constant?
            steps_total += 1
          end
        end

        STDERR.puts "cap=#{cap}: steps=#{steps_total} buffered=#{saw_buffered} compactions=#{compactions}"
        saw_buffered.should be > 0
        compactions.should be > 0 if cap <= 6
      end
    ensure
      LavinMQ::Config.instance.stats_log_size = orig_cap
      LavinMQ::Config.instance.stats_interval = orig_int
    end
  end
end
