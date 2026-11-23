require "spec"
require "../src/lavinmq/memory_pressure_reclaimer"

private def stats(heap_size, free_bytes, bytes_since_gc)
  GC::Stats.new(heap_size: heap_size.to_u64, free_bytes: free_bytes.to_u64, unmapped_bytes: 0_u64,
    bytes_since_gc: bytes_since_gc.to_u64, total_bytes: 0_u64)
end

describe LavinMQ::MemoryPressureReclaimer do
  it "collects when enough of the heap is reclaimable" do
    r = LavinMQ::MemoryPressureReclaimer.new(min_interval: 30.seconds, min_reclaim_ratio: 0.1)
    r.on_pressure(Time.instant, stats(1000, 60, 40)).should be_true
  end

  it "doesn't collect when too little of the heap is reclaimable" do
    r = LavinMQ::MemoryPressureReclaimer.new(min_interval: 30.seconds, min_reclaim_ratio: 0.1)
    r.on_pressure(Time.instant, stats(1000, 50, 49)).should be_false
  end

  it "doesn't start the interval when it skipped for too little to reclaim" do
    r = LavinMQ::MemoryPressureReclaimer.new(min_interval: 30.seconds, min_reclaim_ratio: 0.1)
    now = Time.instant
    r.on_pressure(now, stats(1000, 0, 0)).should be_false
    r.on_pressure(now + 1.second, stats(1000, 500, 0)).should be_true
  end

  it "collects at most once per interval" do
    r = LavinMQ::MemoryPressureReclaimer.new(min_interval: 30.seconds, min_reclaim_ratio: 0.1)
    now = Time.instant
    enough = stats(1000, 500, 0)
    r.on_pressure(now, enough).should be_true
    r.on_pressure(now + 1.second, enough).should be_false
    r.on_pressure(now + 29.seconds, enough).should be_false
    r.on_pressure(now + 30.seconds, enough).should be_true
  end
end
