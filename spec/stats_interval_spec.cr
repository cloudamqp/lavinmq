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

  private class DeliverGetProbe
    include Stats
    rate_stats({"deliver", "deliver_no_ack", "get", "get_no_ack", "deliver_get"})

    def bump(deliver = 0_u64, deliver_no_ack = 0_u64, get = 0_u64, get_no_ack = 0_u64)
      @deliver_count.add(deliver)
      @deliver_no_ack_count.add(deliver_no_ack)
      @get_count.add(get)
      @get_no_ack_count.add(get_no_ack)
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

      it "derives deliver_get from deliver, deliver_no_ack, get and get_no_ack" do
        with_stats_interval(5000) do
          p = DeliverGetProbe.new
          p.bump(deliver: 10_u64, get_no_ack: 5_u64)
          p.update_rates
          p.deliver_get_count.should eq 15
          details = p.stats_details
          details[:deliver_get_details][:rate].should eq 3.0
          details[:deliver_get_details][:log].to_a.should eq [3.0]
          details[:deliver_details][:log].to_a.should eq [2.0]
          p.current_stats_details[:deliver_get_details][:rate].should eq 3.0

          p.bump(deliver_no_ack: 1_u64, get: 2_u64)
          p.update_rates
          p.deliver_get_count.should eq 18
          p.stats_details[:deliver_get_details][:log].to_a.should eq [3.0, 0.6]

          p.update_rates
          p.deliver_get_count.should eq 18
          p.current_stats_details[:deliver_get_details][:rate].should eq 0.0
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
