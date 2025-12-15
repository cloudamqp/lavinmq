require "../spec_helper"

module SparkplugSpecs
  describe LavinMQ::MQTT::Sparkplug::StateTracker do
    it "tracks edge node online state after BIRTH" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.online?("group1", "node1").should be_true
    end

    it "tracks edge node offline state after DEATH" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.online?("group1", "node1").should be_true

      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false
    end

    it "maintains state per edge node" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group1", "node2")

      tracker.online?("group1", "node1").should be_true
      tracker.online?("group1", "node2").should be_true

      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false
      tracker.online?("group1", "node2").should be_true
    end

    it "maintains state per group" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group2", "node1")

      tracker.online?("group1", "node1").should be_true
      tracker.online?("group2", "node1").should be_true

      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false
      tracker.online?("group2", "node1").should be_true
    end

    it "returns false for unknown edge node" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.online?("group1", "node1").should be_false
    end

    it "handles DEATH before BIRTH" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false

      state = tracker.state("group1", "node1")
      state.should_not be_nil
      state.not_nil!.online?.should be_false
    end

    it "updates BIRTH timestamp on each BIRTH" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      state1 = tracker.state("group1", "node1")
      timestamp1 = state1.not_nil!.last_birth_timestamp

      sleep 0.002.seconds # Ensure timestamp difference

      tracker.track_birth("group1", "node1")
      state2 = tracker.state("group1", "node1")
      timestamp2 = state2.not_nil!.last_birth_timestamp

      timestamp2.should be >= timestamp1
    end

    it "updates DEATH timestamp on each DEATH" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.track_death("group1", "node1")
      state1 = tracker.state("group1", "node1")
      timestamp1 = state1.not_nil!.last_death_timestamp.not_nil!

      sleep 0.002.seconds # Ensure timestamp difference

      tracker.track_death("group1", "node1")
      state2 = tracker.state("group1", "node1")
      timestamp2 = state2.not_nil!.last_death_timestamp.not_nil!

      timestamp2.should be >= timestamp1
    end

    it "returns nil state for unknown edge node" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      state = tracker.state("group1", "node1")
      state.should be_nil
    end

    it "counts online edge nodes" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.online_count.should eq(0)

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group1", "node2")
      tracker.online_count.should eq(2)

      tracker.track_death("group1", "node1")
      tracker.online_count.should eq(1)
    end

    it "counts total tracked edge nodes" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.total_count.should eq(0)

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group1", "node2")
      tracker.total_count.should eq(2)

      tracker.track_death("group1", "node1")
      tracker.total_count.should eq(2) # Still tracked, just offline
    end

    it "iterates over all edge node states" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group1", "node2")
      tracker.track_death("group1", "node1")

      states = Hash(String, LavinMQ::MQTT::Sparkplug::EdgeNodeState).new
      tracker.each do |key, state|
        states[key] = state
      end

      states.size.should eq(2)
      states.has_key?("group1/node1").should be_true
      states.has_key?("group1/node2").should be_true
      states["group1/node1"].online?.should be_false
      states["group1/node2"].online?.should be_true
    end

    it "removes edge node from tracking" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.online?("group1", "node1").should be_true

      tracker.remove("group1", "node1")
      tracker.online?("group1", "node1").should be_false
      tracker.total_count.should eq(0)
    end

    it "clears all tracked states" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      tracker.track_birth("group1", "node1")
      tracker.track_birth("group1", "node2")
      tracker.total_count.should eq(2)

      tracker.clear
      tracker.total_count.should eq(0)
      tracker.online?("group1", "node1").should be_false
      tracker.online?("group1", "node2").should be_false
    end

    it "handles BIRTH/DEATH cycles correctly" do
      tracker = LavinMQ::MQTT::Sparkplug::StateTracker.new

      # Initial BIRTH
      tracker.track_birth("group1", "node1")
      tracker.online?("group1", "node1").should be_true

      # DEATH
      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false

      # REBIRTH
      tracker.track_birth("group1", "node1")
      tracker.online?("group1", "node1").should be_true

      # Another DEATH
      tracker.track_death("group1", "node1")
      tracker.online?("group1", "node1").should be_false
    end
  end
end
