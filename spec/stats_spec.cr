require "./spec_helper"

module LavinMQ
  class DerivedProbe
    include Stats
    rate_stats({"deliver", "deliver_no_ack", "get", "get_no_ack", "deliver_get"})

    def bump(deliver : Int, deliver_no_ack : Int, get : Int, get_no_ack : Int)
      @deliver_count.add(deliver.to_u64, :relaxed)
      @deliver_no_ack_count.add(deliver_no_ack.to_u64, :relaxed)
      @get_count.add(get.to_u64, :relaxed)
      @get_no_ack_count.add(get_no_ack.to_u64, :relaxed)
    end
  end
end

describe LavinMQ::Stats do
  it "derives deliver_get from deliver, deliver_no_ack, get and get_no_ack" do
    probe = LavinMQ::DerivedProbe.new
    probe.bump(1, 2, 3, 4)
    probe.deliver_get_count.should eq 10
    probe.update_rates(1.0)
    probe.update_rates(2.0)
    probe.bump(1, 0, 0, 1)
    probe.update_rates(2.0)
    details = probe.stats_details
    details[:deliver_get].should eq 12
    details[:deliver_get_details][:rate].should eq 1.0
    details[:deliver_details][:rate].should eq 0.5
    details[:get_no_ack_details][:rate].should eq 0.5
    details[:deliver_get_details][:log].to_a.last(2).should eq [0.0, 1.0]
  end
end

private def view(values : Array, cap = 4) : LavinMQ::StatLogView
  ring = LavinMQ::StatColumn.alloc(cap, 0_u64)
  head = 0
  size = 0
  values.each do |v|
    tail = head + size
    tail -= cap if tail >= cap
    ring[tail] = v.to_u64
    if size < cap
      size += 1
    else
      head += 1
      head -= cap if head >= cap
    end
  end
  LavinMQ::StatLogView.new(ring, head, size, 0_u64)
end

private def const_view(value : Int, size : Int32) : LavinMQ::StatLogView
  LavinMQ::StatLogView.new(Pointer(UInt64).null, 0, size, value.to_u64)
end

describe LavinMQ::StatLogView do
  it "reads in order, overwriting the oldest once full" do
    view([1_u32, 2_u32, 3_u32]).to_a.should eq [1, 2, 3]
    view((1_u32..10_u32).to_a).to_a.should eq [7, 8, 9, 10]
  end

  it "indexes by position and raises out of range" do
    v = view([1_u32, 2_u32, 3_u32, 4_u32, 5_u32])
    v[0].should eq 2
    v[3].should eq 5
    expect_raises(IndexError) { v[-1] }
    expect_raises(IndexError) { v[4] }
  end

  # A read may pair a ring with stale head/size from before a resize.
  it "clamps stale head and size to the ring's own capacity" do
    ring = LavinMQ::StatColumn.alloc(3, 0_u64)
    ring[0] = 10_u64; ring[1] = 11_u64; ring[2] = 12_u64
    v = LavinMQ::StatLogView.new(ring, 999, 200_000_000, 0_u64)
    v.size.should eq 3
    v.to_a.sort.should eq [10, 11, 12]
    expect_raises(IndexError) { v[3] }
  end

  it "serializes to a JSON array" do
    view([1_u32, 2_u32, 3_u32, 4_u32, 5_u32]).to_json.should eq "[2,3,4,5]"
    view([] of UInt32).to_json.should eq "[]"
  end

  it "repeats the constant without a ring, serializing like an Array" do
    v = const_view(7_u32, 5)
    v.to_a.should eq [7, 7, 7, 7, 7]
    v[4].should eq 7
    [0, 1, 120].each do |n|
      const_view(42_u32, n).to_json.should eq Array.new(n, 42_u32).to_json
      [0_u32, 1_u32, 50_u32, 1_234_567_u32].each do |tenths|
        LavinMQ::RateLogView.new(const_view(tenths, n)).to_json.should eq Array.new(n, tenths / 10.0).to_json
      end
    end
    expect_raises(IndexError) { const_view(0_u32, 0)[0] }
  end

  it "serializes rates like the Float64 values they decode to" do
    max = LavinMQ::RateLogView(UInt64).max_tenths.to_u64
    tenths = [0_u64, 1_u64, 9_u64, 10_u64, 11_u64, 99_u64, 100_u64, UInt32::MAX.to_u64 + 1, max - 1, max]
    rng = Xorshift.new(7_u32)
    200.times { tenths << ((rng.u32.to_u64 << 19 | rng.u32) >> rng.below(51)) }
    view = view(tenths, tenths.size)
    LavinMQ::RateLogView.new(view).to_json.should eq tenths.map { |t| LavinMQ::RateLogView(UInt64).decode(t) }.to_json
    LavinMQ::RateLogView.new(view).to_a.should eq tenths.map { |t| t / 10.0 }
  end
end

describe LavinMQ::RateLogView do
  # The old formula: (delta / interval).round(1), stored as tenths
  it "encodes a count delta to the same tenths as rounding the rate first" do
    same = ->(delta : UInt64, ms : Int32) do
      interval = ms / 1000.0
      LavinMQ::RateLogView(UInt64).encode_delta(delta, interval).should eq LavinMQ::RateLogView(UInt64).encode((delta / interval).round(1))
    end
    deltas = [0_u64, 1_u64, 5_u64, 15_u64, 25_u64, UInt32::MAX.to_u64, UInt64::MAX]
    intervals = [1, 7, 100, 250, 500, 1000, 5000, 60_000]
    deltas.each { |delta| intervals.each { |ms| same.call(delta, ms) } }
    rng = Xorshift.new(3_u32)
    200_000.times do
      same.call((rng.u32.to_u64 << rng.below(33)) + rng.below(1000), rng.below(100_000) + 1)
    end
  end
end

private class AggProbe
  include LavinMQ::HTTP::StatsHelpers
end

describe "add_logs! with a StatLogView source" do
  it "sums right-aligned, newest samples together, without touching the view" do
    a = Deque(UInt64).new
    a.concat([1_u64, 2_u64, 3_u64, 4_u64])
    young = view([10_u32, 20_u32])
    AggProbe.new.add_logs!(a, young)
    a.to_a.should eq [1, 2, 13, 24]
    young.to_a.should eq [10, 20]

    b = Deque(UInt64).new
    b.concat([1_u64, 2_u64])
    AggProbe.new.add_logs!(b, view([10_u32, 20_u32, 30_u32, 40_u32]))
    b.to_a.should eq [10, 20, 31, 42]
  end

  it "sums a constant view" do
    a = Deque(UInt64).new
    a.concat([1_u64, 2_u64, 3_u64])
    AggProbe.new.add_logs!(a, const_view(10_u32, 3))
    a.to_a.should eq [11, 12, 13]
  end
end
