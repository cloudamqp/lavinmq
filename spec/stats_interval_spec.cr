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

private def with_stats_log(size : Int32, &)
  orig = LavinMQ::StatsLog.instance
  log = LavinMQ::StatsLog.new(size)
  LavinMQ::StatsLog.instance = log
  begin
    yield log
  ensure
    LavinMQ::StatsLog.instance = orig
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
        with_stats_log(3) do |log|
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
          with_stats_log(3) do |log|
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
          with_stats_log(3) do |log|
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
      log = StatsLog.new(3)
      log.advance
      s = log.write(StatsLog::Series.new, 1.0)
      log.advance
      s = log.write(s, 2.0)
      log.read(2, s).should eq [1.0, 2.0]
      log.advance
      log.advance
      s = log.write(s, 4.0)
      log.read(3, s).should eq [2.0, 0.0, 4.0]
    end

    it "sums series before rounding" do
      log = StatsLog.new(2)
      log.advance
      a = log.write(StatsLog::Series.new, 0.04)
      b = log.write(StatsLog::Series.new, 0.04)
      log.read(1, a, b).should eq [0.1]
    end

    it "reclaims a series after a full window of zeros" do
      log = StatsLog.new(3)
      log.advance
      s = log.write(StatsLog::Series.new, 1.0)
      log.series_count.should eq 1
      2.times { log.advance }
      log.series_count.should eq 1
      log.read(3, s).should eq [1.0, 0.0, 0.0]
      log.advance
      log.series_count.should eq 0
      log.chunk_count.should eq 0
      log.read(3, s).should eq [0.0, 0.0, 0.0]
    end

    it "doesn't read another series' values through a reclaimed handle" do
      log = StatsLog.new(2)
      log.advance
      stale = log.write(StatsLog::Series.new, 1.0)
      2.times { log.advance }
      other = log.write(StatsLog::Series.new, 7.0)
      other.slot.should eq stale.slot
      log.read(1, stale).should eq [0.0]
      renewed = log.write(stale, 3.0)
      renewed.should_not eq stale
      log.read(1, renewed).should eq [3.0]
      log.read(1, other).should eq [7.0]
    end

    it "keeps the latest values when resized" do
      log = StatsLog.new(3)
      s = StatsLog::Series.new
      1.upto(4) do |i|
        log.advance
        s = log.write(s, i.to_f)
      end
      log.resize(5)
      log.read(5, s).should eq [0.0, 0.0, 2.0, 3.0, 4.0]
      log.advance
      s = log.write(s, 5.0)
      log.read(5, s).should eq [0.0, 2.0, 3.0, 4.0, 5.0]
      log.resize(2)
      log.read(2, s).should eq [4.0, 5.0]
      log.advance
      log.read(2, s).should eq [5.0, 0.0]
      log.advance
      log.series_count.should eq 0
    end

    it "fills the lowest slots first and unmaps chunks that become unused" do
      log = StatsLog.new(2)
      log.advance
      series = Array.new(StatsLog::CHUNK_SLOTS + 1) { log.write(StatsLog::Series.new, 1.0) }
      log.chunk_count.should eq 2
      series.last.slot.should eq StatsLog::CHUNK_SLOTS
      2.times do
        log.advance
        series.first(StatsLog::CHUNK_SLOTS).each { |s| log.write(s, 1.0) }
      end
      log.chunk_count.should eq 1
      log.series_count.should eq StatsLog::CHUNK_SLOTS
    end
  end
end
