require "./spec_helper"

private def with_stats_interval(ms : Int32, &)
  cfg = LavinMQ::Config.instance
  orig = cfg.stats_interval
  begin
    cfg.stats_interval = ms
    yield
  ensure
    cfg.stats_interval = orig
  end
end

private def with_stats_logs(size : Int32, &)
  counter_log, gauge_log = LavinMQ::Stats.counter_log, LavinMQ::Stats.gauge_log
  LavinMQ::Stats.counter_log = LavinMQ::StatsLog(UInt32).new(size)
  LavinMQ::Stats.gauge_log = LavinMQ::StatsLog(Int64).new(size, 64)
  begin
    yield LavinMQ::Stats.counter_log, LavinMQ::Stats.gauge_log
  ensure
    LavinMQ::Stats.counter_log, LavinMQ::Stats.gauge_log = counter_log, gauge_log
  end
end

module LavinMQ
  private class DeliverGetProbe
    include Stats
    rate_stats({"deliver", "deliver_no_ack", "get", "get_no_ack", "deliver_get", "ack"})

    def bump(deliver = 0u64, get_no_ack = 0u64)
      @deliver_count.add(deliver)
      @get_no_ack_count.add(get_no_ack)
    end
  end

  private class GaugeProbe
    include Stats
    gauge_stats({"g"})

    def g=(value)
      log_g(value)
    end
  end

  private class IntervalProbe
    include Stats
    rate_stats({"x"})

    def bump(n : UInt64)
      @x_count.add(n)
    end

    def x_rate
      @x_rate
    end
  end

  describe Server do
    describe "#update_system_metrics" do
      it "yields finite rates with sub-second stats_interval" do
        with_stats_interval(500) do
          with_amqp_server do |s|
            s.update_system_metrics(nil)
            s.update_system_metrics(nil)
            {s.user_time_log, s.sys_time_log, s.blocks_out_log, s.blocks_in_log}.each do |log|
              log.each &.finite?.should be_true
            end
          end
        end
      end
    end
  end

  describe Stats do
    describe "#update_rates" do
      [1, 50, 250, 500, 999, 1000, 5000, 30_000].each do |ms|
        it "yields finite rates with stats_interval=#{ms}ms" do
          with_stats_interval(ms) do
            p = IntervalProbe.new
            p.update_rates
            p.x_rate.finite?.should be_true
            p.x_rate.should eq 0.0
            p.bump(1234_u64)
            p.update_rates
            p.x_rate.finite?.should be_true
            p.x_rate.should be > 0.0
          end
        end
      end

      it "only keeps history for keys with non-zero rates" do
        with_stats_logs(3) do |log|
          p = DeliverGetProbe.new
          log.advance
          p.update_rates
          log.series_count.should eq 0
          p.bump(deliver: 5u64)
          log.advance
          p.update_rates
          log.series_count.should eq 1
        end
      end

      it "keeps one log entry per tick since creation, up to the log size" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = IntervalProbe.new
            p.stats_details[:x_details][:log].should be_empty
            log.advance
            p.bump(5u64)
            p.update_rates
            p.stats_details[:x_details][:log].should eq [1.0]
            4.times do
              log.advance
              p.update_rates
            end
            p.stats_details[:x_details][:log].should eq [0.0, 0.0, 0.0]
          end
        end
      end

      it "derives deliver_get from deliver, deliver_no_ack, get and get_no_ack" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = DeliverGetProbe.new
            p.bump(deliver: 10u64, get_no_ack: 5u64)
            log.advance
            p.update_rates
            p.deliver_get_count.should eq 15
            details = p.stats_details
            details[:deliver_get_details][:rate].should eq 3.0
            details[:deliver_get_details][:log].should eq [3.0]
            details[:deliver_details][:log].should eq [2.0]
            p.current_stats_details[:deliver_get_details][:rate].should eq 3.0
          end
        end
      end

      it "caps the logged increase per tick at UInt32::MAX" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = IntervalProbe.new
            log.advance
            p.bump(5_000_000_000u64)
            p.update_rates
            p.x_rate.should eq 1_000_000_000.0
            p.x_log.should eq [(UInt32::MAX / 5).round(1)]
          end
        end
      end
    end

    describe "gauge_stats" do
      it "logs the value of each tick" do
        with_stats_logs(3) do |counter_log, gauge_log|
          p = GaugeProbe.new
          p.g_log.should be_empty
          {7, 0, 9, 4}.each do |v|
            counter_log.advance
            gauge_log.advance
            p.g = v
          end
          p.g_log.should eq [0, 9, 4]
        end
      end

      it "reports events-per-second independent of the sampling interval" do
        {500 => 100.0, 1000 => 50.0, 5000 => 10.0}.each do |ms, expected|
          with_stats_interval(ms) do
            p = IntervalProbe.new
            p.update_rates
            p.bump(50_u64)
            p.update_rates
            p.x_rate.should eq expected
          end
        end
      end
    end
  end

  describe StatsLog do
    it "returns the values of the latest ticks, oldest first" do
      log = StatsLog(UInt32).new(3)
      log.advance
      s = log.write(StatsLog::Series.new, 1u32)
      log.advance
      s = log.write(s, 2u32)
      log.read(2, s, &.itself).should eq [1, 2]
      log.advance
      log.advance
      s = log.write(s, 4u32)
      log.read(3, s, &.itself).should eq [2, 0, 4]
    end

    it "sums series" do
      log = StatsLog(UInt32).new(2)
      log.advance
      a = log.write(StatsLog::Series.new, UInt32::MAX)
      b = log.write(StatsLog::Series.new, 3u32)
      log.read(1, a, b, &.itself).should eq [UInt32::MAX.to_i64 + 3]
      log.read(1, a, b) { |v| v / 2 }.should eq [(UInt32::MAX.to_i64 + 3) / 2]
    end

    it "doesn't allocate a slot for zeros, but overwrites a value with zero" do
      log = StatsLog(Int64).new(2)
      log.advance
      s = log.write(StatsLog::Series.new, 0i64)
      log.series_count.should eq 0
      s = log.write(s, 5i64)
      s = log.write(s, 0i64)
      log.read(1, s, &.itself).should eq [0]
      log.series_count.should eq 1
      2.times { log.advance }
      log.series_count.should eq 0
    end

    it "reclaims a series after a full window of zeros" do
      log = StatsLog(UInt32).new(3)
      log.advance
      s = log.write(StatsLog::Series.new, 1u32)
      log.series_count.should eq 1
      2.times { log.advance }
      log.series_count.should eq 1
      log.read(3, s, &.itself).should eq [1, 0, 0]
      log.advance
      log.series_count.should eq 0
      log.chunk_count.should eq 0
      log.read(3, s, &.itself).should eq [0, 0, 0]
    end

    it "doesn't read another series' values through a reclaimed handle" do
      log = StatsLog(UInt32).new(2)
      log.advance
      stale = log.write(StatsLog::Series.new, 1u32)
      2.times { log.advance }
      other = log.write(StatsLog::Series.new, 7u32)
      other.slot.should eq stale.slot
      log.read(1, stale, &.itself).should eq [0]
      renewed = log.write(stale, 3u32)
      renewed.should_not eq stale
      log.read(1, renewed, &.itself).should eq [3]
      log.read(1, other, &.itself).should eq [7]
    end

    it "keeps the latest values when resized" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      1u32.upto(4u32) do |i|
        log.advance
        s = log.write(s, i)
      end
      log.resize(5)
      log.read(5, s, &.itself).should eq [0, 0, 2, 3, 4]
      log.advance
      s = log.write(s, 5u32)
      log.read(5, s, &.itself).should eq [0, 2, 3, 4, 5]
      log.resize(2)
      log.read(2, s, &.itself).should eq [4, 5]
      log.advance
      log.read(2, s, &.itself).should eq [5, 0]
      log.advance
      log.series_count.should eq 0
    end

    it "fills the lowest slots first and unmaps chunks that become unused" do
      log = StatsLog(UInt32).new(2, chunk_slots: 4)
      log.advance
      series = Array.new(5) { log.write(StatsLog::Series.new, 1u32) }
      log.chunk_count.should eq 2
      series.last.slot.should eq 4
      2.times do
        log.advance
        series.first(4).each { |s| log.write(s, 1u32) }
      end
      log.chunk_count.should eq 1
      log.series_count.should eq 4
    end
  end
end
