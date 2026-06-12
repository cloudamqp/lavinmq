require "./spec_helper"

# Build a single buffered column and a StatLogView over it, mirroring how the
# rate_stats macro writes a column.
private def view(values : Array(Float64), cap = 4) : LavinMQ::StatLogView(Float64)
  col = Pointer(Float64).malloc(cap)
  head = 0
  size = 0
  values.each do |v|
    tail = head + size
    tail -= cap if tail >= cap
    col[tail] = v
    if size < cap
      size += 1
    else
      head += 1
      head -= cap if head >= cap
    end
  end
  LavinMQ::StatLogView(Float64).new(col, 0, head, size, cap, 0.0)
end

describe LavinMQ::StatLogView do
  it "reads in order before wrapping" do
    view([1.0, 2.0, 3.0]).to_a.should eq [1.0, 2.0, 3.0]
  end

  it "overwrites oldest when full (wraps)" do
    view([1.0, 2.0, 3.0, 4.0, 5.0]).to_a.should eq [2.0, 3.0, 4.0, 5.0] # cap 4
  end

  it "wraps multiple times" do
    view((1..10).map(&.to_f64)).to_a.should eq [7.0, 8.0, 9.0, 10.0]
  end

  it "indexes by position" do
    v = view([1.0, 2.0, 3.0, 4.0, 5.0])
    v[0].should eq 2.0
    v[3].should eq 5.0
  end

  it "raises IndexError out of range" do
    expect_raises(IndexError) { view([1.0])[-1] }
    expect_raises(IndexError) { view([1.0])[1] }
    expect_raises(IndexError) { view([] of Float64)[0] }
  end

  it "serializes to a JSON array" do
    view([1.0, 2.0, 3.0, 4.0, 5.0]).to_json.should eq "[2.0,3.0,4.0,5.0]"
    view([] of Float64).to_json.should eq "[]"
  end

  it "is Enumerable" do
    view([1.0, 2.0, 3.0, 4.0]).sum.should eq 10.0
  end

  describe "constant mode (null base)" do
    it "yields the constant `size` times with no buffer" do
      v = LavinMQ::StatLogView(Float64).new(Pointer(Float64).null, 0, 0, 5, 120, 3.5)
      v.size.should eq 5
      v.to_a.should eq [3.5, 3.5, 3.5, 3.5, 3.5]
      v[0].should eq 3.5
      v[4].should eq 3.5
      v.to_json.should eq "[3.5,3.5,3.5,3.5,3.5]"
    end

    it "is empty when size is zero" do
      v = LavinMQ::StatLogView(Float64).new(Pointer(Float64).null, 0, 0, 0, 120, 0.0)
      v.to_a.should eq [] of Float64
      v.to_json.should eq "[]"
      expect_raises(IndexError) { v[0] }
    end
  end
end

module LavinMQ
  class StatsProbe
    include Stats
    rate_stats({"x"}, {"y"})

    @y = 0_u32

    def y
      @y
    end

    def bump(dx : UInt64, yv : UInt32)
      @x_count.add(dx)
      @y = yv
    end

    def constant?
      @_stats_rate_buffer.null?
    end
  end
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

  it "compacts buffered -> constant after a full constant window" do
    p = LavinMQ::StatsProbe.new
    cap = LavinMQ::Config.instance.stats_log_size
    p.bump(5_u64, 1_u32); p.update_rates  # rate 1.0
    p.bump(50_u64, 9_u32); p.update_rates # rate 10.0, y 9 -> materialize
    p.constant?.should be_false
    (cap + 2).times { p.bump(50_u64, 9_u32); p.update_rates } # steady -> compact
    p.constant?.should be_true
    p.x_log.to_a.should eq Array.new(cap, 10.0)
    p.y_log.to_a.should eq Array.new(cap, 9_u32)
  end
end

private class AggProbe
  include LavinMQ::HTTP::StatsHelpers
end

describe "add_logs! with a StatLogView source" do
  it "sums a buffered view element-wise into a Deque" do
    a = Deque(Float64).new
    a.concat([1.0, 2.0, 3.0, 4.0])
    AggProbe.new.add_logs!(a, view([10.0, 20.0, 30.0, 40.0, 50.0])) # holds [20,30,40,50]
    a.to_a.should eq [21.0, 32.0, 43.0, 54.0]
  end

  it "sums a constant view element-wise into a Deque" do
    a = Deque(Float64).new
    a.concat([1.0, 2.0, 3.0])
    b = LavinMQ::StatLogView(Float64).new(Pointer(Float64).null, 0, 0, 3, 120, 10.0)
    AggProbe.new.add_logs!(a, b)
    a.to_a.should eq [11.0, 12.0, 13.0]
  end
end
