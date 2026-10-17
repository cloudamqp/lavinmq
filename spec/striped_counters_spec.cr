require "spec"
require "wait_group"
require "../src/lavinmq/config"
require "../src/lavinmq/stats"

describe LavinMQ::Stats::StripedCounters do
  it "sums adds from every thread, including threads beyond the stripe count" do
    counters = LavinMQ::Stats::StripedCounters.new(3)
    # More threads than stripes, so some share the last (atomic) stripe
    threads = LavinMQ::Stats::StripedCounters.stripes + 3
    ctx = Fiber::ExecutionContext::Parallel.new("striped-counters", threads)
    wg = WaitGroup.new(threads)
    threads.times do |t|
      ctx.spawn do
        10_000.times do
          counters.add(0, 1_u64)
          counters.add(2, 2_u64)
          counters.sub(1, 1_u64) if t.even?
        end
      ensure
        wg.done
      end
    end
    wg.wait
    counters.get(0).should eq 10_000_u64 * threads
    counters.get(2).should eq 20_000_u64 * threads
    # Subs wrap per stripe, the sum wraps back
    even = (threads + 1) // 2
    counters.get(1).should eq(0_u64 &- 10_000_u64 * even)
  end

  it "refuses an index outside the group" do
    counters = LavinMQ::Stats::StripedCounters.new(2)
    expect_raises(IndexError) { counters.add(2, 1_u64) }
    expect_raises(IndexError) { counters.get(-1) }
  end
end
