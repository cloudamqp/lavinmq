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

module LavinMQ
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
end
