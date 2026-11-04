require "./spec_helper"

# A window of *cap* samples with the Deque semantics of the old update_rates: shift until
# size < cap, then push. A rate's first sample is skipped while the window still holds it
private class Oracle(T)
  @q = Deque(T).new
  @first = true

  def push(v : T, cap : Int32) : Nil
    while @q.size >= cap
      @q.shift
      @first = false
    end
    @q.push v
  end

  def rates : Array(T)
    @first ? @q.to_a[1..] : @q.to_a
  end

  def readings : Array(T)
    @q.to_a
  end
end

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

module LavinMQ
  class StatsProbe
    include Stats
    rate_stats({"x", "z_oct"}, {"y"})

    @y = 0_u32

    def y
      @y
    end

    def bump(dx : UInt64, yv : UInt32, dz : UInt64 = 0_u64)
      @x_count.add(dx)
      @z_oct_count.add(dz)
      @y = yv
    end

    def constant?
      !stats_log_buffered?
    end

    def rings
      {x: @x_rate_log.buffered?, z_oct: @z_oct_rate_log.buffered?, y: @y_log.buffered?}
    end
  end
end

describe LavinMQ::Stats do
  it "stays constant (no ring) while idle, serving zeros" do
    p = LavinMQ::StatsProbe.new
    10.times { p.bump(0_u64, 0_u32); p.update_rates }
    p.constant?.should be_true
    p.x_log.size.should eq 9
    p.x_log.to_a.should eq Array.new(9, 0.0)
    p.y_log.to_a.should eq Array.new(10, 0_u32)
  end

  it "stays constant for a steady rate" do
    p = LavinMQ::StatsProbe.new
    5.times { p.bump(25_u64, 7_u32); p.update_rates } # steady rate 5.0, y constant 7
    p.constant?.should be_true
    p.x_log.to_a.should eq Array.new(4, 5.0)
    p.y_log.to_a.should eq Array.new(5, 7_u32)
  end

  it "materializes on divergence, replaying the constant history losslessly" do
    p = LavinMQ::StatsProbe.new
    3.times { p.bump(25_u64, 5_u32); p.update_rates } # constant: rate 5.0 x3, y 5 x3
    p.constant?.should be_true
    p.bump(0_u64, 5_u32); p.update_rates # rate -> 0.0 diverges, y stays 5
    p.rings.should eq({x: true, z_oct: false, y: false})
    p.x_log.to_a.should eq [5.0, 5.0, 0.0]
    p.y_log.to_a.should eq [5_u32, 5_u32, 5_u32, 5_u32]
  end

  it "starts history at the second sample, so a first interval's handshake buys no ring" do
    p = LavinMQ::StatsProbe.new
    p.bump(25_u64, 0_u32); p.update_rates # rate 5.0 over the first, partial interval
    p.bump(0_u64, 0_u32); p.update_rates  # then nothing
    p.constant?.should be_true
    p.x_log.to_a.should eq [0.0]
    p.bump(0_u64, 0_u32); p.update_rates
    p.x_log.to_a.should eq [0.0, 0.0]
  end

  it "keeps a gauge's first sample, a reading rather than a rate over a partial interval" do
    p = LavinMQ::StatsProbe.new
    p.bump(0_u64, 5_u32); p.update_rates # declared with a backlog: no history to keep yet
    p.constant?.should be_true
    p.bump(0_u64, 9_u32); p.update_rates # the reading moves: history starts with the first
    p.y_log.to_a.should eq [5_u32, 9_u32]
    p.x_log.to_a.should eq [0.0]
  end

  it "compacts buffered -> constant after a full constant window" do
    p = LavinMQ::StatsProbe.new
    cap = LavinMQ::Config.instance.stats_log_size
    2.times { p.bump(5_u64, 1_u32); p.update_rates } # rate 1.0
    p.bump(50_u64, 9_u32); p.update_rates            # rate 10.0, y 9 -> materialize
    p.constant?.should be_false
    (cap + 2).times { p.bump(50_u64, 9_u32); p.update_rates } # steady -> compact
    p.constant?.should be_true
    p.x_log.to_a.should eq Array.new(cap, 10.0)
    p.y_log.to_a.should eq Array.new(cap, 9_u32)
  end

  it "drops a column's ring once its window is one value, while another column keeps changing" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 1_u32); p.update_rates }
      p.bump(10_u64, 9_u32); p.update_rates # x and y both change: a ring each
      6.times { |i| p.bump((i + 3).to_u64 * 5, 9_u32); p.update_rates }
      p.y_log.to_a.should eq [1_u64, 9_u64, 9_u64, 9_u64, 9_u64, 9_u64, 9_u64, 9_u64] # seven 9s so far
      p.rings.should eq({x: true, z_oct: false, y: true})
      p.bump(50_u64, 9_u32); p.update_rates
      p.y_log.constant?.should be_true
      p.rings.should eq({x: true, z_oct: false, y: false})
      p.y_log.to_a.should eq [9_u64] * 8
      p.x_log.to_a.should eq [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "keeps byte rates past 32-bit tenths and caps them where they still print exactly" do
    p = LavinMQ::StatsProbe.new
    2.times { p.bump(0_u64, 0_u32, 1_000_000_000_u64 * 5); p.update_rates } # 1 GB/s at the 5 s interval
    p.z_oct_log.to_a.should eq [1.0e9]
    p.bump(0_u64, 0_u32, UInt64::MAX // 2); p.update_rates
    p.z_oct_log.to_a.last.should eq LavinMQ::RateLogView(UInt64).max_tenths / 10.0
    p.current_stats_details[:z_oct_details][:rate].should eq LavinMQ::RateLogView(UInt64).max_tenths / 10.0
  end

  it "stores message rates as 4-byte samples, capped at 32-bit tenths" do
    sizeof(LavinMQ::StatColumn(UInt32)).should eq 16
    sizeof(LavinMQ::StatColumn(UInt64)).should eq 24
    p = LavinMQ::StatsProbe.new
    2.times { p.bump(5_u64, 0_u32, 5_u64); p.update_rates }
    p.bump(UInt64::MAX // 4, 0_u32, 5_u64); p.update_rates
    p.x_log.to_a.last.should eq UInt32::MAX / 10.0
    p.x_log.to_json.should end_with ",429496729.5]"
  end

  it "records a count-log change on an otherwise idle object" do
    p = LavinMQ::StatsProbe.new
    3.times { p.bump(0_u64, 0_u32); p.update_rates }
    p.bump(0_u64, 7_u32); p.update_rates
    p.y_log.to_a.should eq [0_u32, 0_u32, 0_u32, 7_u32]
    p.x_log.to_a.should eq [0.0, 0.0, 0.0]
  end

  it "adopts a reloaded stats_log_size on an idle object" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      p = LavinMQ::StatsProbe.new
      10.times { p.bump(0_u64, 0_u32); p.update_rates }
      p.x_log.size.should eq 8
      LavinMQ::Config.instance.stats_log_size = 3
      p.bump(0_u64, 0_u32); p.update_rates
      p.x_log.size.should eq 3
      p.y_log.size.should eq 3
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "holds no ring with stats_log_size = 1, since a one-sample window is always constant" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 1
      p = LavinMQ::StatsProbe.new
      p.bump(5_u64, 0_u32); p.update_rates
      p.bump(10_u64, 0_u32); p.update_rates
      p.constant?.should be_true
      p.x_log.to_a.should eq [2.0]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "drops its rings on a reload to stats_log_size = 1 and is idle again" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 1_u32); p.update_rates }
      p.bump(10_u64, 9_u32); p.update_rates # x and y change: a ring each
      p.constant?.should be_false
      LavinMQ::Config.instance.stats_log_size = 1
      p.bump(10_u64, 9_u32); p.update_rates
      p.constant?.should be_true
      p.x_log.to_a.should eq [2.0]
      LavinMQ::Config.instance.stats_log_size = 8
      p.bump(20_u64, 9_u32); p.update_rates # x changes: a ring again
      p.constant?.should be_false
      9.times { p.bump(20_u64, 9_u32); p.update_rates } # unchanged for a window: dropped
      p.constant?.should be_true
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "adopts a reloaded stats_log_size for existing objects, keeping the newest samples" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 0_u32); p.update_rates } # establish: rate 1.0
      p.bump(10_u64, 0_u32); p.update_rates            # rate 2.0 -> materialize
      p.bump(15_u64, 0_u32); p.update_rates            # rate 3.0
      p.bump(20_u64, 0_u32); p.update_rates            # rate 4.0
      p.x_log.to_a.should eq [1.0, 2.0, 3.0, 4.0]
      before = p.x_log

      LavinMQ::Config.instance.stats_log_size = 3 # shrink (SIGHUP reload)
      p.bump(25_u64, 0_u32); p.update_rates       # rate 5.0
      p.x_log.size.should eq 3
      p.x_log.to_a.should eq [3.0, 4.0, 5.0]
      before.to_a.should eq [1.0, 2.0, 3.0, 4.0] # a view keeps its ring

      LavinMQ::Config.instance.stats_log_size = 5 # grow
      p.bump(30_u64, 0_u32); p.update_rates       # rate 6.0
      p.x_log.to_a.should eq [3.0, 4.0, 5.0, 6.0]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end
end

module LavinMQ
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
      !stats_log_buffered?
    end
  end
end

# The old Deque-based formula
private def rate_for(dx : UInt64, interval : Float64) : Float64
  (dx / interval).round(1)
end

private def check(view, oracle, do_json) : Bool
  return false if view.size != oracle.size
  view.size.times do |i|
    return false if view[i] != oracle[i]
  end
  return false if do_json && view.to_json != oracle.to_json
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

        (1_u32..24_u32).each do |stream|
          rng = Xorshift.new(stream)
          p = LavinMQ::MultiProbe.new
          orates = Array.new(3) { Oracle(Float64).new }
          ocounts = Array.new(2) { Oracle(UInt32).new }
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

            orates[0].push(rate_for(da, interval), cap)
            orates[1].push(rate_for(db, interval), cap)
            orates[2].push(rate_for(dc, interval), cap)
            ocounts[0].push(mv, cap)
            ocounts[1].push(nv, cap)

            do_json = step % 16 == 0
            ok = check(p.a_log, orates[0].rates, do_json) && check(p.b_log, orates[1].rates, do_json) &&
                 check(p.c_log, orates[2].rates, do_json) && check(p.m_log, ocounts[0].readings, do_json) &&
                 check(p.n_log, ocounts[1].readings, do_json)
            unless ok
              fail "cap=#{cap} stream=#{stream} step=#{step} constant?=#{p.constant?}\n" \
                   "  a got=#{p.a_log.to_a} exp=#{orates[0].rates}\n  b got=#{p.b_log.to_a} exp=#{orates[1].rates}\n" \
                   "  c got=#{p.c_log.to_a} exp=#{orates[2].rates}\n  m got=#{p.m_log.to_a} exp=#{ocounts[0].readings}\n" \
                   "  n got=#{p.n_log.to_a} exp=#{ocounts[1].readings}"
            end

            saw_buffered += 1 unless p.constant?
            compactions += 1 if p.constant? && !was_constant
            was_constant = p.constant?
          end
        end

        saw_buffered.should be > 0 if cap > 1
        compactions.should be > 0 if cap > 1 && cap <= 6
      end
    ensure
      LavinMQ::Config.instance.stats_log_size = orig_cap
      LavinMQ::Config.instance.stats_interval = orig_int
    end
  end
end

describe "constant-encoding differential vs Deque oracle with stats_log_size reloads" do
  it "matches per-column Deque logs while stats_log_size changes mid-sequence" do
    orig_cap = LavinMQ::Config.instance.stats_log_size
    orig_int = LavinMQ::Config.instance.stats_interval
    begin
      LavinMQ::Config.instance.stats_interval = 5000
      interval = 5.0
      caps = [1, 2, 3, 6, 120]
      resizes = 0

      (1_u32..48_u32).each do |stream|
        rng = Xorshift.new(stream)
        cap = caps[rng.below(caps.size)]
        LavinMQ::Config.instance.stats_log_size = cap
        p = LavinMQ::MultiProbe.new
        orates = Array.new(3) { Oracle(Float64).new }
        ocounts = Array.new(2) { Oracle(UInt32).new }
        da = db = dc = 0_u64
        mv = nv = 0_u32

        250.times do |step|
          if rng.below(20).zero?
            cap = caps[rng.below(caps.size)]
            LavinMQ::Config.instance.stats_log_size = cap
            resizes += 1
          end
          da = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
          db = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
          dc = (rng.below(3).zero? ? 0_u64 : rng.below(200).to_u64 + 1) if rng.below(10).zero?
          mv = rng.below(50).to_u32 if rng.below(10).zero?
          nv = rng.below(50).to_u32 if rng.below(10).zero?

          p.bump(da, db, dc, mv, nv)
          p.update_rates

          orates[0].push(rate_for(da, interval), cap)
          orates[1].push(rate_for(db, interval), cap)
          orates[2].push(rate_for(dc, interval), cap)
          ocounts[0].push(mv, cap)
          ocounts[1].push(nv, cap)

          ok = check(p.a_log, orates[0].rates, false) && check(p.b_log, orates[1].rates, false) &&
               check(p.c_log, orates[2].rates, false) && check(p.m_log, ocounts[0].readings, false) &&
               check(p.n_log, ocounts[1].readings, false)
          unless ok
            fail "cap=#{cap} stream=#{stream} step=#{step} constant?=#{p.constant?}\n" \
                 "  a got=#{p.a_log.to_a} exp=#{orates[0].rates}\n  b got=#{p.b_log.to_a} exp=#{orates[1].rates}\n" \
                 "  c got=#{p.c_log.to_a} exp=#{orates[2].rates}\n  m got=#{p.m_log.to_a} exp=#{ocounts[0].readings}\n" \
                 "  n got=#{p.n_log.to_a} exp=#{ocounts[1].readings}"
          end
        end
      end

      resizes.should be > 0
    ensure
      LavinMQ::Config.instance.stats_log_size = orig_cap
      LavinMQ::Config.instance.stats_interval = orig_int
    end
  end
end

describe "concurrent readers" do
  it "only see an object's own samples while its rings are dropped and resized" do
    orig = LavinMQ::Config.instance.stats_log_size
    probes = Array.new(32) { LavinMQ::StatsProbe.new }
    own = ->(k : Int32, v : UInt32) { v.zero? || v // 1000 == k.to_u32 + 1 }
    stop = Atomic(Bool).new(false)
    foreign = Atomic(Int32).new(0)
    errors = Atomic(Int32).new(0)
    reads = Atomic(Int64).new(0)
    ctx = Fiber::ExecutionContext::Parallel.new("stat-log-race", 6)
    wg = WaitGroup.new
    begin
      6.times do |r|
        wg.add(1)
        ctx.spawn do
          rng = Xorshift.new(r.to_u32 + 1)
          until stop.get(:relaxed)
            begin
              if r.even? # reads right away
                k = rng.below(probes.size)
                probes[k].y_log.each { |v| foreign.add(1) unless own.call(k, v) }
              else # reads views held while the writer goes on
                views = probes.map(&.y_log)
                sleep rng.below(200).microseconds
                views.each_with_index { |view, k| view.each { |v| foreign.add(1) unless own.call(k, v) } }
              end
              reads.add(1, :relaxed)
            rescue
              errors.add(1)
            end
          end
        ensure
          wg.done
        end
      end

      rng = Xorshift.new(42_u32)
      deadline = Time.instant + 1.second
      sweep = 0
      until Time.instant >= deadline
        sweep += 1
        LavinMQ::Config.instance.stats_log_size = rng.below(4).zero? ? 4 + rng.below(30) : 8 if sweep % 7 == 0
        probes.each_with_index do |p, k|
          base = (k + 1).to_u32 * 1000
          p.bump(0_u64, rng.below(3).zero? ? base + rng.below(1000).to_u32 : p.y) # steady runs drop rings
          p.update_rates
        end
        Fiber.yield
      end
    ensure
      stop.set(true)
      wg.wait
      LavinMQ::Config.instance.stats_log_size = orig
    end
    reads.get.should be > 1000
    errors.get.should eq 0
    foreign.get.should eq 0
  end
end

describe LavinMQ::StatJson do
  it "serializes a log without allocating" do
    io = IO::Memory.new(4096)
    ring = view(Array.new(120) { |i| i * 7 }, 120)
    {ring, const_view(5_u32, 120), LavinMQ::RateLogView.new(ring), LavinMQ::RateLogView.new(const_view(15_u32, 120))}.each do |log|
      allocated = Array.new(5) do
        io.clear
        json = JSON::Builder.new(io)
        before = GC.stats.total_bytes
        json.document { log.to_json(json) }
        GC.stats.total_bytes - before
      end
      allocated.min.should eq 0 # other fibers may allocate meanwhile; the formatter never does
      io.to_s.should eq log.to_a.to_json
    end
  end
end
