require "./spec_helper"

# A buffered column laid out like the pool hands it out, with its header before it.
private def block(cap : Int32) : Pointer(UInt64)
  raw = Pointer(UInt64).malloc(cap + LavinMQ::StatBlockPool::HEADER // 2)
  data = raw + LavinMQ::StatBlockPool::HEADER // 2
  data.as(Pointer(UInt32))[-3] = 1_u32 # generation, as the pool never hands out 0
  data.as(Pointer(UInt32))[-1] = (cap * sizeof(UInt64)).to_u32
  data
end

private def view(values : Array, cap = 4) : LavinMQ::StatLogView
  col = block(cap)
  head = 0
  size = 0
  values.each do |v|
    tail = head + size
    tail -= cap if tail >= cap
    col[tail] = v.to_u64
    if size < cap
      size += 1
    else
      head += 1
      head -= cap if head >= cap
    end
  end
  LavinMQ::StatLogView.new(col, head, size, 0_u64)
end

private def const_view(value : Int, size : Int32) : LavinMQ::StatLogView
  LavinMQ::StatLogView.new(Pointer(UInt64).null, 0, size, value.to_u64)
end

# A window of *cap* samples, with the Deque semantics of the old update_rates: shift until
# size < cap, then push. History starts with an object's second sample, so a sample that
# follows a lone one (at birth, or after a reload left one) reads as itself.
private def push_sample(q : Deque(T), v : T, cap : Int32) forall T
  lone = q.size <= 1
  while q.size >= cap
    q.shift
  end
  q.push v
  q[0] = v if lone && q.size > 1
end

# Own PRNG so the sequence is identical across Crystal versions.
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

  # A read may pair a buffer with stale head/size from before a resize.
  it "clamps stale head and size to the buffer's own capacity" do
    data = block(3)
    data[0] = 10_u64; data[1] = 11_u64; data[2] = 12_u64
    v = LavinMQ::StatLogView.new(data, 999, 200_000_000, 0_u64)
    v.size.should eq 3
    v.to_a.sort.should eq [10, 11, 12]
    expect_raises(IndexError) { v[3] }
  end

  it "reads a buffer whose page was given back (all zeros) as the constant" do
    data = block(4)
    data.as(Pointer(UInt32))[-1] = 0_u32
    v = LavinMQ::StatLogView.new(data, 3, 5, 7_u64)
    v.size.should eq 5
    v.to_a.should eq [7, 7, 7, 7, 7]
    v.to_json.should eq "[7,7,7,7,7]"
  end

  it "serializes to a JSON array" do
    view([1_u32, 2_u32, 3_u32, 4_u32, 5_u32]).to_json.should eq "[2,3,4,5]"
    view([] of UInt32).to_json.should eq "[]"
  end

  it "repeats the constant without a buffer, serializing like an Array" do
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
  end
end

class LavinMQ::StatBlockPool
  class_property? fail_alloc = false

  def alloc : Pointer(UInt8)
    raise "alloc failed" if StatBlockPool.fail_alloc?
    previous_def
  end
end

# A stats loop tick: every object's update, then the pools' trim
private def tick(*probes : LavinMQ::StatsProbe) : Nil
  probes.each &.update_rates
  LavinMQ::StatBlockPool.trim
end

describe LavinMQ::Stats do
  it "stays constant (no buffer) while idle, serving zeros" do
    p = LavinMQ::StatsProbe.new
    10.times { p.bump(0_u64, 0_u32); p.update_rates }
    p.constant?.should be_true
    p.x_log.size.should eq 10
    p.x_log.to_a.should eq Array.new(10, 0.0)
    p.y_log.to_a.should eq Array.new(10, 0_u32)
  end

  it "stays constant for a steady rate" do
    p = LavinMQ::StatsProbe.new
    5.times { p.bump(25_u64, 7_u32); p.update_rates } # steady rate 5.0, y constant 7
    p.constant?.should be_true
    p.x_log.to_a.should eq Array.new(5, 5.0)
    p.y_log.to_a.should eq Array.new(5, 7_u32)
  end

  it "materializes on divergence, replaying the constant history losslessly" do
    p = LavinMQ::StatsProbe.new
    3.times { p.bump(25_u64, 5_u32); p.update_rates } # constant: rate 5.0 x3, y 5 x3
    p.constant?.should be_true
    p.bump(0_u64, 5_u32); p.update_rates # rate -> 0.0 diverges, y stays 5
    p.constant?.should be_false
    p.x_log.to_a.should eq [5.0, 5.0, 5.0, 0.0]
    p.y_log.to_a.should eq [5_u32, 5_u32, 5_u32, 5_u32]
  end

  it "starts history at the second sample, so a first interval's handshake buys no buffer" do
    p = LavinMQ::StatsProbe.new
    cap = LavinMQ::Config.instance.stats_log_size
    used = LavinMQ::StatBlockPool.for(cap, UInt32).@used
    p.bump(25_u64, 0_u32); p.update_rates # rate 5.0 over the first, partial interval
    p.bump(0_u64, 0_u32); p.update_rates  # then nothing
    p.constant?.should be_true
    p.x_log.to_a.should eq [0.0, 0.0]
    p.bump(0_u64, 0_u32); p.update_rates
    p.x_log.to_a.should eq [0.0, 0.0, 0.0]
    LavinMQ::StatBlockPool.for(cap, UInt32).@used.should eq used
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

  it "keeps byte rates past 32-bit tenths and caps them where they still print exactly" do
    p = LavinMQ::StatsProbe.new
    p.bump(0_u64, 0_u32, 1_000_000_000_u64 * 5); p.update_rates # 1 GB/s at the 5 s interval
    p.z_oct_log.to_a.should eq [1.0e9]
    p.bump(0_u64, 0_u32, UInt64::MAX // 2); p.update_rates
    p.z_oct_log.to_a.last.should eq LavinMQ::RateLogView(UInt64).max_tenths / 10.0
    p.current_stats_details[:z_oct_details][:rate].should eq LavinMQ::RateLogView(UInt64).max_tenths / 10.0
  end

  it "stores message rates as 4-byte samples, capped at 32-bit tenths, in a pool of their own" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 29 # pools of their own
      narrow = LavinMQ::StatBlockPool.for(29, UInt32)
      wide = LavinMQ::StatBlockPool.for(29, UInt64)
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 0_u32, 5_u64); tick(p) }
      p.bump(10_u64, 0_u32, 5_u64); tick(p) # x changes, z_oct steady
      {narrow.@used, narrow.bytes, wide.@used}.should eq({1, 29 * 4, 0})
      p.bump(10_u64, 0_u32, 10_u64); tick(p)
      {narrow.@used, wide.@used, wide.bytes}.should eq({1, 1, 29 * 8})
      p.bump(UInt64::MAX // 4, 0_u32, 10_u64); tick(p)
      p.x_log.to_a.last.should eq UInt32::MAX / 10.0
      p.x_log.to_json.should end_with ",429496729.5]"
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "records a count-log change on an otherwise idle object" do
    p = LavinMQ::StatsProbe.new
    3.times { p.bump(0_u64, 0_u32); p.update_rates }
    p.bump(0_u64, 7_u32); p.update_rates
    p.y_log.to_a.should eq [0_u32, 0_u32, 0_u32, 7_u32]
    p.x_log.to_a.should eq [0.0, 0.0, 0.0, 0.0]
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

  it "holds no buffer with stats_log_size = 1, since a one-sample window is always constant" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 1
      used = LavinMQ::StatBlockPool.for(1, UInt32).@used
      p = LavinMQ::StatsProbe.new
      p.bump(5_u64, 0_u32); p.update_rates
      p.bump(10_u64, 0_u32); p.update_rates
      p.constant?.should be_true
      p.x_log.to_a.should eq [2.0]
      LavinMQ::StatBlockPool.for(1, UInt32).@used.should eq used
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "reads the last value from where a view's buffer has been handed out again" do
    orig = LavinMQ::Config.instance.stats_log_size
    LavinMQ::Config.instance.stats_log_size = 8
    begin
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 0_u32); p.update_rates } # rate 1.0
      p.bump(10_u64, 0_u32); p.update_rates            # rate 2.0, buffered
      view = p.x_log
      view.to_a.should eq [1.0, 1.0, 2.0]

      8.times { p.bump(10_u64, 0_u32); p.update_rates } # steady 2.0 until the buffers are dropped
      p.constant?.should be_true

      # A released buffer would be handed out again by its pool
      pool = LavinMQ::StatBlockPool.for(8, UInt32)
      garbage = Array(Pointer(UInt32)).new
      2000.times { garbage << pool.alloc(UInt32).tap { |b| Slice.new(b, 8).fill(99_u32) } }
      view.to_a.should eq [2.0, 2.0, 2.0] # the window it was taken from, all at the last value since
      view.to_json.should eq "[2.0,2.0,2.0]"
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "drops its buffers when a resize fails partway, staying consistent" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 1_u32); p.update_rates }
      p.bump(10_u64, 2_u32); p.update_rates # x and y buffered
      LavinMQ::Config.instance.stats_log_size = 5
      LavinMQ::StatBlockPool.fail_alloc = true
      expect_raises(Exception, "alloc failed") { p.update_rates }
      LavinMQ::StatBlockPool.fail_alloc = false
      p.constant?.should be_true
      p.bump(15_u64, 3_u32); p.update_rates
      p.bump(20_u64, 4_u32); p.update_rates
      p.x_log.to_a.should eq [2.0, 2.0, 2.0, 3.0, 4.0] # history dropped to the last value
      p.y_log.to_a.should eq [2, 2, 2, 3, 4]
    ensure
      LavinMQ::StatBlockPool.fail_alloc = false
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "moves its buffers to the front of a sparse pool, keeping the log" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 37 # a pool of its own
      pool = LavinMQ::StatBlockPool.for(37, UInt32)
      fillers = Array.new(50) { pool.alloc }
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 0_u32); p.update_rates }
      p.bump(10_u64, 0_u32); p.update_rates
      p.@_stats_high[0].should be >= 50

      fillers.each { |b| pool.release(b) }
      pool.trim
      p.bump(15_u64, 0_u32); p.update_rates
      p.@_stats_high[0].should eq 0
      p.x_log.to_a.should eq [1.0, 1.0, 2.0, 3.0]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "renews the lease of its buffers while it updates" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 43 # a pool of its own
      p = LavinMQ::StatsProbe.new
      (LavinMQ::StatBlockPool::LEASE * 3).times { |i| p.bump(5_u64 * (i % 2), 0_u32); tick(p) }
      log = p.x_log.to_a
      log.size.should eq 43
      log.each_cons_pair.all? { |a, b| a != b }.should be_true # alternating all along, never forgotten
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "has its buffers taken back once it stops updating, and forgets them if it updates again" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 47 # a pool of its own
      pool = LavinMQ::StatBlockPool.for(47, UInt32)
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 3_u32); tick(p) }
      p.bump(10_u64, 3_u32); tick(p) # x buffered, y constant
      pool.@used.should eq 1

      (LavinMQ::StatBlockPool::LEASE * 2).times { LavinMQ::StatBlockPool.trim } # p isn't updated
      pool.@used.should eq 0
      reused = pool.alloc(UInt32)
      Slice.new(reused, 47).fill(99_u32)

      p.bump(10_u64, 3_u32); tick(p)
      p.constant?.should be_true
      p.x_log.to_a.uniq.should eq [2.0]
      Slice.new(reused, 47).all?(99_u32).should be_true
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "releases a retired object's buffers when the stats loop drains, keeping the log as its last value" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 37 # a pool of its own
      pool = LavinMQ::StatBlockPool.for(37, UInt32)
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 3_u32); tick(p) }
      p.bump(10_u64, 3_u32); tick(p)
      p.constant?.should be_false
      used = pool.@used
      LavinMQ::Stats.retire(p)
      p.constant?.should be_false # until the stats loop drains
      LavinMQ::Stats.release_retired
      p.constant?.should be_true
      (used - pool.@used).should eq 1
      p.x_log.to_a.should eq [2.0, 2.0, 2.0]
      LavinMQ::Stats.release_retired # a retired object whose buffers are gone is left alone
      (used - pool.@used).should eq 1
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "only forgets a retired object's buffers once its lease has run out, as the pool has them" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 31 # a pool of its own
      pool = LavinMQ::StatBlockPool.for(31, UInt32)
      p = LavinMQ::StatsProbe.new
      2.times { p.bump(5_u64, 3_u32); tick(p) }
      p.bump(10_u64, 3_u32); tick(p)
      block = p.@x_rate_log.block
      (LavinMQ::StatBlockPool::LEASE * 2).times { LavinMQ::StatBlockPool.trim } # p isn't ticked, e.g. its vhost is being deleted
      reused = pool.alloc(UInt32)
      reused.should eq block
      Slice.new(reused, 31).fill(99_u32)
      used = pool.@used
      LavinMQ::Stats.retire(p)
      LavinMQ::Stats.release_retired
      p.constant?.should be_true
      pool.@used.should eq used
      LavinMQ::StatBlockPool.generation(reused).should_not eq 0
      Slice.new(reused, 31).all?(99_u32).should be_true
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  it "leaves a closed object without buffers to the GC instead of holding it for the stats loop" do
    p = LavinMQ::StatsProbe.new
    p.bump(5_u64, 3_u32); tick(p)
    LavinMQ::Stats.retire(p).should be_false # declared and deleted within a tick
    p.bump(5_u64, 3_u32); tick(p)
    p.bump(10_u64, 3_u32); tick(p)
    LavinMQ::Stats.retire(p).should be_true
    LavinMQ::Stats.release_retired
    p.constant?.should be_true
  end

  it "relocates a buffer with its ring layout intact, so a reader's head and size still fit it" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 6 # a pool of its own
      pool = LavinMQ::StatBlockPool.for(6, UInt32)
      fillers = Array.new(50) { pool.alloc }
      p = LavinMQ::StatsProbe.new
      8.times { |i| p.bump(5_u64 * (i + 1), 0_u32); p.update_rates }
      p.x_log.to_a.should eq [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
      p.@_stats_log_head.get.should eq 2
      p.@_stats_high[0].should be >= 50

      fillers.each { |b| pool.release(b) }
      pool.trim
      p.bump(45_u64, 0_u32); p.update_rates
      p.@_stats_high[0].should eq 0
      p.x_log.to_a.should eq [4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
      p.@_stats_log_head.get.should eq 3
      Slice.new(p.@x_rate_log.block, 6).to_a.should eq [70, 80, 90, 40, 50, 60]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
    end
  end

  # The stats loop sleeps an hour after its first sweep; the spec drives the sweeps itself.
  it "releases the buffers of a closed connection's channel and a deleted queue on the next tick" do
    orig = LavinMQ::Config.instance.stats_interval
    LavinMQ::Config.instance.stats_interval = 3_600_000
    begin
      with_amqp_server do |s|
        LavinMQ::Config.instance.stats_interval = 1000
        vhost = s.vhosts["/"]
        chans = [] of LavinMQ::AMQP::Channel
        with_channel(s) do |ch|
          cq = ch.queue("retired")
          chans << vhost.connections.first.channels.first
          [1, 1, 2].each do |n|
            n.times { cq.publish_confirm "m" }
            s.update_stats_rates
          end
        end
        q = vhost.queue("retired")
        chan = chans.first
        q.stats_log_buffered?.should be_true
        chan.stats_log_buffered?.should be_true
        wait_for { vhost.connections.empty? }
        q.delete
        s.update_stats_rates
        q.stats_log_buffered?.should be_false
        chan.stats_log_buffered?.should be_false
        q.publish_log.to_a.should eq [2.0, 2.0, 2.0]
      end
    ensure
      LavinMQ::Config.instance.stats_interval = orig
    end
  end

  it "fills a grown buffer with its own samples, for readers still on the old head and size" do
    orig = LavinMQ::Config.instance.stats_log_size
    begin
      LavinMQ::Config.instance.stats_log_size = 8
      pool = LavinMQ::StatBlockPool.for(20, UInt64)
      Array.new(4) { pool.alloc(UInt64).tap { |b| Slice.new(b, 20).fill(99_u64) } }.each { |b| pool.release(b) }
      p = LavinMQ::StatsProbe.new
      12.times { |i| p.bump(0_u64, (i % 2 + 1).to_u32); p.update_rates } # full, head moved on
      head, size = p.@_stats_log_head.get, p.@_stats_log_size.get

      LavinMQ::Config.instance.stats_log_size = 20
      p.bump(0_u64, 1_u32); p.update_rates
      mid_resize = LavinMQ::StatLogView.new(p.@y_log.block, head, size, p.@y_log.last)
      mid_resize.to_a.should_not contain 99_u64
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
      p.x_log.to_a.should eq [1.0, 1.0, 2.0, 3.0, 4.0]

      LavinMQ::Config.instance.stats_log_size = 3 # shrink (SIGHUP reload)
      p.bump(25_u64, 0_u32); p.update_rates       # rate 5.0
      p.x_log.size.should eq 3
      p.x_log.to_a.should eq [3.0, 4.0, 5.0]

      LavinMQ::Config.instance.stats_log_size = 5 # grow
      p.bump(30_u64, 0_u32); p.update_rates       # rate 6.0
      p.x_log.to_a.should eq [3.0, 4.0, 5.0, 6.0]
    ensure
      LavinMQ::Config.instance.stats_log_size = orig
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

lib LibC
  fun mincore(addr : Void*, length : SizeT, vec : UInt8*) : Int
end

private def resident?(ptr : Pointer) : Bool
  page = LibC.sysconf(LibC::SC_PAGESIZE).to_u64
  vec = 0_u8
  LibC.mincore(Pointer(Void).new(ptr.address & ~(page - 1)), page, pointerof(vec)).should eq 0
  vec.bit(0) == 1
end

describe LavinMQ::StatBlockPool do
  it "hands out capacity-headed blocks outside the GC heap and reuses the lowest released one" do
    pool = LavinMQ::StatBlockPool.new(8, UInt64)
    a = pool.alloc
    b = pool.alloc
    c = pool.alloc
    b.should_not eq a
    LavinMQ::StatBlockPool.bytes(a).should eq 64
    LibGC.is_heap_ptr(a.as(Void*)).should eq 0
    pool.release(c)
    pool.release(a)
    pool.trim
    pool.alloc.should eq a
    LavinMQ::StatBlockPool.bytes(a).should eq 64
  end

  # MADV_DONTNEED drops private pages at once on Linux only
  {% if flag?(:linux) %}
    it "returns the pages of released blocks to the OS on trim" do
      pool = LavinMQ::StatBlockPool.new(120, UInt64)
      blocks = Array.new(100) { pool.alloc }
      blocks.all? { |b| resident?(b) }.should be_true
      blocks.each { |b| pool.release(b) }
      pool.trim
      blocks.none? { |b| resident?(b) }.should be_true
    end

    it "spreads a large release over trims within their deadline, returning every page in the end" do
      pool = LavinMQ::StatBlockPool.new(120, UInt64)
      blocks = Array.new(20_000) { pool.alloc }
      blocks.each { |b| pool.release(b) }
      pool.trim(Time.instant) # no time left: a step only
      blocks.any? { |b| resident?(b) }.should be_true
      trims = 1
      while blocks.any? { |b| resident?(b) }
        pool.trim(Time.instant)
        trims += 1
        trims.should be < 1000
      end
      trims.should be > 2
      pool.alloc.should eq blocks.first
    end

    it "keeps the pages a used block shares with released ones" do
      pool = LavinMQ::StatBlockPool.new(120, UInt64)
      blocks = Array.new(100) { |i| pool.alloc(UInt64).tap { |b| Slice.new(b, 120).fill(i.to_u64) } }
      live = blocks.delete_at(50)
      blocks.each { |b| pool.release(b) }
      pool.trim
      Slice.new(live, 120).all?(50_u64).should be_true
      LavinMQ::StatBlockPool.bytes(live).should eq 960
      page = LibC.sysconf(LibC::SC_PAGESIZE).to_u64
      live_pages = ((live.as(Pointer(UInt32)) - LavinMQ::StatBlockPool::HEADER).address // page)..((live + 120).address - 1) // page
      blocks.reject { |b| live_pages.includes?(b.address // page) }.none? { |b| resident?(b) }.should be_true
    end
  {% end %}

  it "asks the owners of scattered survivors to move below a watermark, within the free room there" do
    pool = LavinMQ::StatBlockPool.new(16, UInt64)
    blocks = Array.new(100) { pool.alloc }
    pool.trim
    blocks.none? { |b| pool.relocate?(LavinMQ::StatBlockPool.ordinal(b), 1) }.should be_true # dense

    survivors = (9...100).step(10).map { |i| blocks[i] }.to_a
    (blocks - survivors).each { |b| pool.release(b) }
    pool.trim
    moved = survivors.select { |b| pool.relocate?(LavinMQ::StatBlockPool.ordinal(b), 1) }
    moved.size.should be > 0
    moved.size.should be <= 12 - 1 # watermark 12, one survivor already below it
    moved.each do |b|
      pool.alloc
      pool.release(b)
    end
    pool.trim
    pool.span.should be <= 12
  end

  it "frees blocks whose lease has run out" do
    pool = LavinMQ::StatBlockPool.new(8, UInt64)
    now = LavinMQ::StatBlockPool.tick
    stale = pool.alloc
    renewed = pool.alloc
    LavinMQ::StatBlockPool.renew(renewed, now &+ 10)
    pool.trim(tick: now &+ LavinMQ::StatBlockPool::LEASE)
    pool.@used.should eq 2
    pool.trim(tick: now &+ LavinMQ::StatBlockPool::LEASE &+ 1)
    pool.@used.should eq 1
    pool.alloc.should eq stale
  end

  it "voids the generation of a released block, so a stale or new view reads its constant" do
    pool = LavinMQ::StatBlockPool.new(8, UInt64)
    a = pool.alloc(UInt64)
    8.times { |i| a[i] = 5_u64 }
    v = LavinMQ::StatLogView.new(a, 0, 8, 1_u64)
    v.to_a.should eq [5] * 8
    pool.release(a)
    v.to_a.should eq [1] * 8
    v.to_json.should eq "[1,1,1,1,1,1,1,1]"
    LavinMQ::StatLogView.new(a, 0, 8, 1_u64).constant?.should be_true
  end

  it "hands out every block with a new generation, also a reused one" do
    pool = LavinMQ::StatBlockPool.new(8, UInt64)
    a = pool.alloc
    generation = LavinMQ::StatBlockPool.generation(a)
    pool.release(a)
    pool.trim # returns its page, zeroing the header
    pool.alloc.should eq a
    LavinMQ::StatBlockPool.generation(a).should_not eq generation
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

            push_sample(orates[0], rate_for(da, interval), cap)
            push_sample(orates[1], rate_for(db, interval), cap)
            push_sample(orates[2], rate_for(dc, interval), cap)
            push_sample(ocounts[0], mv, cap)
            push_sample(ocounts[1], nv, cap)

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
        orates = Array.new(3) { Deque(Float64).new }
        ocounts = Array.new(2) { Deque(UInt32).new }
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

          push_sample(orates[0], rate_for(da, interval), cap)
          push_sample(orates[1], rate_for(db, interval), cap)
          push_sample(orates[2], rate_for(dc, interval), cap)
          push_sample(ocounts[0], mv, cap)
          push_sample(ocounts[1], nv, cap)

          ok = check(p.a_log, orates[0], false) && check(p.b_log, orates[1], false) &&
               check(p.c_log, orates[2], false) && check(p.m_log, ocounts[0], false) &&
               check(p.n_log, ocounts[1], false)
          unless ok
            fail "cap=#{cap} stream=#{stream} step=#{step} constant?=#{p.constant?}\n" \
                 "  a got=#{p.a_log.to_a} exp=#{orates[0].to_a}\n  m got=#{p.m_log.to_a} exp=#{ocounts[0].to_a}"
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
  it "only see an object's own samples while its blocks are dropped, moved and reused" do
    orig = LavinMQ::Config.instance.stats_log_size
    probes = Array.new(32) { LavinMQ::StatsProbe.new }
    own = ->(k : Int32, v : UInt64) { v.zero? || v // 1000 == k.to_u64 + 1 }
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
          p.bump(0_u64, rng.below(3).zero? ? base + rng.below(1000).to_u32 : p.y) # steady runs drop buffers
          p.update_rates
        end
        LavinMQ::StatBlockPool.trim
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
