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
  counter_log, byte_log, gauge_log = LavinMQ::Stats.counter_log, LavinMQ::Stats.byte_log, LavinMQ::Stats.gauge_log
  LavinMQ::Stats.counter_log = LavinMQ::StatsLog(UInt32).new(size)
  LavinMQ::Stats.byte_log = LavinMQ::StatsLog(UInt64).new(size, 256)
  LavinMQ::Stats.gauge_log = LavinMQ::StatsLog(Int64).new(size, 64)
  LavinMQ::Stats.log_interval = nil # new logs, collected at the configured interval
  begin
    yield LavinMQ::Stats.counter_log
  ensure
    LavinMQ::Stats.counter_log, LavinMQ::Stats.byte_log, LavinMQ::Stats.gauge_log = counter_log, byte_log, gauge_log
    LavinMQ::Stats.log_interval = nil
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

  private class ByteProbe
    include Stats
    rate_stats({"recv_oct"})

    def bump(n : UInt64)
      @recv_oct_count.add(n)
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

  describe Server do
    it "keeps checking free disk space while out of file descriptors" do
      with_stats_interval(50) do
        with_amqp_server do |s|
          without_free_file_descriptors do
            # Publishers are stopped when the disk is about to fill up
            size = s.disk_free_log.size
            wait_for(timeout: 5.seconds) { s.disk_free_log.size > size }
          end
          size = s.rss_log.size
          wait_for { s.rss_log.size > size }
        end
      end
    end
  end

  describe Stats do
    describe "#update_rates" do
      [1, 50, 250, 500, 999, 1000, 5000, 30_000].each do |ms|
        it "yields finite rates with stats_interval=#{ms}ms" do
          with_stats_interval(ms) do
            with_stats_logs(3) do
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
      end

      it "only keeps history for keys with non-zero rates" do
        with_stats_logs(3) do |log|
          p = DeliverGetProbe.new
          log.advance { p.update_rates }
          log.series_count.should eq 0
          p.bump(deliver: 5u64)
          log.advance { p.update_rates }
          log.series_count.should eq 1
        end
      end

      it "keeps one log entry per tick since creation, up to the log size" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = IntervalProbe.new
            p.stats_details[:x_details][:log].should be_empty
            p.bump(5u64)
            log.advance { p.update_rates }
            p.stats_details[:x_details][:log].to_a.should eq [1.0]
            4.times { log.advance { p.update_rates } }
            p.stats_details[:x_details][:log].to_a.should eq [0.0, 0.0, 0.0]
          end
        end
      end

      it "derives deliver_get from deliver, deliver_no_ack, get and get_no_ack" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = DeliverGetProbe.new
            p.bump(deliver: 10u64, get_no_ack: 5u64)
            log.advance { p.update_rates }
            p.deliver_get_count.should eq 15
            details = p.stats_details
            details[:deliver_get_details][:rate].should eq 3.0
            details[:deliver_get_details][:log].to_a.should eq [3.0]
            details[:deliver_details][:log].to_a.should eq [2.0]
            p.current_stats_details[:deliver_get_details][:rate].should eq 3.0
          end
        end
      end

      it "adds rate logs to a sum, aligned at the latest tick" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            a = IntervalProbe.new
            log.advance { }
            b = IntervalProbe.new
            a.bump(1u64)
            b.bump(2u64)
            log.advance do
              a.update_rates
              b.update_rates
            end
            rates = [] of Float64
            a.add_x_log(rates)
            b.add_x_log(rates)
            rates.should eq [0.0, 0.6]
          end
        end
      end

      it "caps the logged increase per tick at UInt32::MAX" do
        with_stats_interval(5000) do
          with_stats_logs(3) do |log|
            p = IntervalProbe.new
            p.bump(5_000_000_000u64)
            log.advance { p.update_rates }
            p.x_rate.should eq 1_000_000_000.0
            p.x_log.to_a.should eq [(UInt32::MAX / 5).round(1)]
          end
        end
      end

      it "logs byte counters without a cap" do
        with_stats_interval(5000) do
          with_stats_logs(3) do
            p = ByteProbe.new
            p.bump(50_000_000_000u64)
            Stats.tick(3, 5000) { p.update_rates }
            p.recv_oct_log.to_a.should eq [10_000_000_000.0]
            Stats.byte_log.series_count.should eq 1
            Stats.counter_log.series_count.should eq 0
            rates = [1.0]
            p.add_recv_oct_log(rates)
            rates.should eq [10_000_000_001.0]
          end
        end
      end

      it "restarts the rate logs, but not the gauge logs, when stats_interval changes" do
        with_stats_logs(3) do
          p = IntervalProbe.new
          g = GaugeProbe.new
          with_stats_interval(5000) do
            1.upto(2) do |i|
              p.bump(5u64)
              Stats.tick(3, 5000) do
                p.update_rates
                g.g = i
              end
            end
            p.x_log.to_a.should eq [1.0, 1.0]
          end
          with_stats_interval(1000) do
            p.bump(5u64)
            Stats.tick(3, 1000) do
              p.update_rates
              g.g = 3
            end
            p.x_log.to_a.should eq [5.0]
            g.g_log.to_a.should eq [1, 2, 3]
          end
        end
      end

      it "divides by the interval a tick was collected over when stats_interval changes during it" do
        with_stats_logs(3) do
          p = IntervalProbe.new
          with_stats_interval(5000) do
            2.times do
              p.bump(5u64)
              Stats.tick(3, 5000) { p.update_rates }
            end
            p.x_log.to_a.should eq [1.0, 1.0]
          end
          with_stats_interval(1000) do
            # reloaded while the stats loop slept the old interval
            p.bump(5u64)
            Stats.tick(3, 5000) { p.update_rates }
            p.x_rate.should eq 1.0
            p.x_log.to_a.should eq [1.0, 1.0, 1.0]
            p.bump(5u64)
            Stats.tick(3, 1000) { p.update_rates }
            p.x_rate.should eq 5.0
            p.x_log.to_a.should eq [5.0]
          end
        end
      end

      it "reports events-per-second independent of the sampling interval" do
        {500 => 100.0, 1000 => 50.0, 5000 => 10.0}.each do |ms, expected|
          with_stats_interval(ms) do
            with_stats_logs(3) do
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

    describe "gauge_stats" do
      it "logs the value of each tick" do
        with_stats_logs(3) do
          p = GaugeProbe.new
          p.g_log.should be_empty
          {7, 0, 9, 4}.each do |v|
            Stats.tick(3, Config.instance.stats_interval) { p.g = v }
          end
          p.g_log.to_a.should eq [0, 9, 4]
          values = [1i64, 1i64, 1i64, 1i64]
          p.add_g_log(values)
          values.should eq [1, 1, 10, 5]
        end
      end
    end
  end

  describe StatsLog do
    it "returns the values of the latest ticks, oldest first" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      log.advance { s = log.write(s, 1u32) }
      log.advance { s = log.write(s, 2u32) }
      log.values(2, s).to_a.should eq [1, 2]
      log.advance { }
      log.advance { s = log.write(s, 4u32) }
      log.values(3, s).to_a.should eq [2, 0, 4]
    end

    it "shows a tick only once it has been written" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      log.advance { s = log.write(s, 1u32) }
      log.advance do
        s = log.write(s, 2u32)
        log.tick.should eq 1
        log.values(2, s).to_a.should eq [0, 1]
      end
      log.tick.should eq 2
      log.values(2, s).to_a.should eq [1, 2]
    end

    it "shows a tick that raised while being written" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      expect_raises(Exception, "collection failed") do
        log.advance do
          s = log.write(s, 1u32)
          raise "collection failed"
        end
      end
      log.tick.should eq 1
      log.values(1, s).to_a.should eq [1]
    end

    it "sums series" do
      log = StatsLog(UInt32).new(2)
      a = b = StatsLog::Series.new
      log.advance do
        a = log.write(a, UInt32::MAX)
        b = log.write(b, 3u32)
      end
      log.values(1, a, b).to_a.should eq [UInt32::MAX.to_i64 + 3]
      log.rates(1, a, b, interval: 2.0, digits: 1).to_a.should eq [(UInt32::MAX.to_i64 + 3) / 2]
    end

    it "merges series into existing values, padding them at the front" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      log.advance { s = log.write(s, 1u32) }
      log.advance { s = log.write(s, 2u32) }
      sums = [10i64]
      log.merge_into(sums, 2, s) { |v, sum| v + sum }
      sums.should eq [1, 12]
      log.merge_into(sums, 1, s) { |v, sum| v * sum }
      sums.should eq [1, 24]
    end

    it "doesn't allocate a slot for zeros, but overwrites a value with zero" do
      log = StatsLog(Int64).new(2)
      s = StatsLog::Series.new
      log.advance do
        s = log.write(s, 0i64)
        log.series_count.should eq 0
        s = log.write(s, 5i64)
        s = log.write(s, 0i64)
      end
      log.values(1, s).to_a.should eq [0]
      log.series_count.should eq 1
      3.times { log.advance { } }
      log.series_count.should eq 0
    end

    it "reclaims a series once all its rows are zero" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      log.advance { s = log.write(s, 1u32) }
      log.series_count.should eq 1
      3.times { log.advance { } }
      log.series_count.should eq 1
      log.values(3, s).to_a.should eq [0, 0, 0]
      log.advance { }
      log.series_count.should eq 0
      log.chunk_count.should eq 0
    end

    it "doesn't read another series' values through a reclaimed handle" do
      log = StatsLog(UInt32).new(2)
      stale = StatsLog::Series.new
      log.advance { stale = log.write(stale, 1u32) }
      3.times { log.advance { } }
      other = StatsLog::Series.new
      log.advance { other = log.write(other, 7u32) }
      other.slot.should eq stale.slot
      log.values(1, stale).to_a.should eq [0]
      renewed = log.write(stale, 3u32)
      renewed.should_not eq stale
      log.values(1, renewed).to_a.should eq [3]
      log.values(1, other).to_a.should eq [7]
    end

    it "restarts the logs at the latest tick when cleared" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      2.times { log.advance { s = log.write(s, 1u32) } }
      log.ticks_since(0).should eq 2
      log.clear
      log.chunk_count.should eq 0
      log.ticks_since(0).should eq 0
      log.values(3, s).to_a.should eq [0, 0, 0]
      log.advance { s = log.write(s, 2u32) }
      log.ticks_since(0).should eq 1
      log.values(1, s).to_a.should eq [2]
    end

    it "keeps the latest values when resized" do
      log = StatsLog(UInt32).new(3)
      s = StatsLog::Series.new
      1u32.upto(4u32) do |i|
        log.advance { s = log.write(s, i) }
      end
      log.values(3, s).to_a.should eq [2, 3, 4]
      log.resize(5)
      log.values(5, s).to_a.should eq [0, 1, 2, 3, 4]
      log.advance { s = log.write(s, 5u32) }
      log.values(5, s).to_a.should eq [1, 2, 3, 4, 5]
      log.resize(2)
      log.values(2, s).to_a.should eq [4, 5]
      log.advance { }
      log.values(2, s).to_a.should eq [5, 0]
      log.advance { }
      log.series_count.should eq 1
      log.advance { }
      log.series_count.should eq 0
    end

    it "fills the lowest slots first and unmaps chunks that become unused" do
      log = StatsLog(UInt32).new(2, chunk_slots: 4)
      series = [] of StatsLog::Series
      log.advance do
        5.times { series << log.write(StatsLog::Series.new, 1u32) }
      end
      log.chunk_count.should eq 2
      series.last.slot.should eq 4
      3.times do
        log.advance do
          series.first(4).each { |s| log.write(s, 1u32) }
        end
      end
      log.chunk_count.should eq 1
      log.series_count.should eq 4
    end

    describe StatsLog::View do
      it "serializes like an Array of its values" do
        log = StatsLog(UInt32).new(4)
        a = b = StatsLog::Series.new
        log.advance { a = log.write(a, 5u32) }
        log.advance { b = log.write(b, 10u32) }
        log.advance { }
        rates = log.rates(3, a, b, interval: 2.0, digits: 1)
        rates.to_json.should eq [2.5, 5.0, 0.0].to_json
        log.values(3, a, b).to_json.should eq [5, 10, 0].to_json
      end

      it "serializes the log of series without slots as zeros of its size" do
        log = StatsLog(UInt32).new(3)
        s = StatsLog::Series.new
        log.rates(0, s, interval: 1.0, digits: 1).to_json.should eq "[]"
        log.advance { }
        log.rates(1, s, interval: 1.0, digits: 1).to_json.should eq "[0.0]"
        log.values(1, s).to_json.should eq "[0]"
        log.advance { }
        log.rates(2, s, interval: 1.0, digits: 1).to_json.should eq "[0.0,0.0]"
        log.values(2, s).to_json.should eq "[0,0]"
        # a reclaimed series reads as zeros too
        log.advance { s = log.write(s, 1u32) }
        4.times { log.advance { } }
        log.series_count.should eq 0
        log.values(3, s).to_json.should eq "[0,0,0]"
      end

      it "serializes a log longer than one batch" do
        size = StatsLog::View::BATCH * 2 + 3
        log = StatsLog(UInt32).new(size)
        s = StatsLog::Series.new
        1.upto(size) { |i| log.advance { s = log.write(s, i.to_u32) } }
        log.values(size, s).to_a.should eq (1i64..size.to_i64).to_a
        JSON.parse(log.values(size, s).to_json).as_a.map(&.as_i64).should eq (1i64..size.to_i64).to_a
      end

      it "reads at the tick it was taken at" do
        log = StatsLog(UInt32).new(3)
        s = StatsLog::Series.new
        log.advance { s = log.write(s, 1u32) }
        view = log.values(1, s)
        log.advance { s = log.write(s, 2u32) }
        view.to_a.should eq [1]
        log.values(2, s).to_a.should eq [1, 2]
      end
    end
  end
end
