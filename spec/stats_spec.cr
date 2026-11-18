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
