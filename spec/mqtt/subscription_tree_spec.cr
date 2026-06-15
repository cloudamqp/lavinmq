require "./spec_helper"
require "../../src/lavinmq/mqtt/subscription_tree"

describe LavinMQ::MQTT::SubscriptionTree do
  describe "#any?" do
    it "returns false for empty tree" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.any?("a").should be_false
    end

    describe "with subs" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      before_each do
        test_data = [
          "a/b",
          "a/+/b",
          "a/b/c/d/#",
          "a/+/c/d/#",
        ]
        target = "target"

        test_data.each do |topic|
          tree.subscribe(topic, target, 0u8)
        end
      end

      it "returns false for no matching subscriptions" do
        tree.any?("a").should be_false
      end

      it "returns true for matching non-wildcard subs" do
        tree.any?("a/b").should be_true
      end

      it "returns true for matching '+'-wildcard subs" do
        tree.any?("a/r/b").should be_true
      end

      it "returns true for matching '#'-wildcard subs" do
        tree.any?("a/b/c/d/e/f").should be_true
      end
    end
  end

  describe "#empty?" do
    it "returns true before any subscribe" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.empty?.should be_true
    end

    it "returns false after a non-wildcard subscribe" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "target"
      tree.subscribe("topic", session, 0u8)
      tree.empty?.should be_false
    end

    it "returns false after a +-wildcard subscribe" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "target"
      tree.subscribe("a/+/topic", session, 0u8)
      tree.empty?.should be_false
    end

    it "returns false after a #-wildcard subscribe" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "session"
      tree.subscribe("a/#/topic", session, 0u8)
      tree.empty?.should be_false
    end

    it "returns true after unsubscribing only existing non-wildcard subscription" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "session"
      tree.subscribe("topic", session, 0u8)
      tree.unsubscribe("topic", session)
      tree.empty?.should be_true
    end

    it "doesn't keep entries for unsubscribed non-wildcard filters" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "session"
      10.times do |i|
        tree.subscribe("topic#{i}", session, 0u8)
        tree.unsubscribe("topic#{i}", session)
      end
      tree.@non_wildcards.size.should eq 0
    end

    it "keeps the entry when other subscribers remain on the filter" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("topic", "session1", 0u8)
      tree.subscribe("topic", "session2", 0u8)
      tree.unsubscribe("topic", "session1")
      tree.@non_wildcards.size.should eq 1
      matched = 0
      tree.each_entry("topic") { matched += 1 }
      matched.should eq 1
    end

    it "returns true after unsubscribing only existing +-wildcard subscription" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "session"
      tree.subscribe("a/+/topic", session, 0u8)
      tree.unsubscribe("a/+/topic", session)
      tree.empty?.should be_true
    end

    it "returns true after unsubscribing only existing #+-wildcard subscription" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      session = "session"
      tree.subscribe("a/b/#", session, 0u8)
      tree.unsubscribe("a/b/#", session)
      tree.empty?.should be_true
    end

    it "returns true after unsubscribing many different subscriptions" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      test_data = [
        {"session", "a/b"},
        {"session", "a/+/b"},
        {"session", "a/b/c/d#"},
        {"session", "a/+/c/d/#"},
        {"session", "#"},
      ]

      test_data.each do |session, topic|
        tree.subscribe(topic, session, 0u8)
      end

      test_data.shuffle.each do |session, topic|
        tree.unsubscribe(topic, session)
      end

      tree.empty?.should be_true
    end
  end

  describe "#size" do
    it "returns 0 for empty tree" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.size.should eq 0
    end

    it "counts non-wildcard, + and # subscriptions" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("a/b", "session", 0u8)
      tree.subscribe("a/+/b", "session", 0u8)
      tree.subscribe("a/b/#", "session", 0u8)
      tree.size.should eq 3
    end

    it "counts multiple sessions on the same filter separately" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("a/b", "session1", 0u8)
      tree.subscribe("a/b", "session2", 0u8)
      tree.subscribe("a/+", "session1", 0u8)
      tree.subscribe("a/+", "session2", 0u8)
      tree.size.should eq 4
    end

    it "does not double-count when changing qos of an existing subscription" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("a/b", "session", 0u8)
      tree.subscribe("a/b", "session", 1u8)
      tree.size.should eq 1
    end

    it "decreases after unsubscribing" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("a/b", "session", 0u8)
      tree.subscribe("a/+/b", "session", 0u8)
      tree.unsubscribe("a/b", "session")
      tree.size.should eq 1
    end
  end

  it "subscriptions is found" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    test_data = [
      {"session1", [{"a/b", 0u8}]},
      {"session2", [{"a/b", 0u8}]},
      {"session3", [{"a/c", 0u8}]},
      {"session4", [{"a/+", 0u8}]},
      {"session5", [{"#", 0u8}]},
    ]

    test_data.each do |s|
      session, subscriptions = s
      subscriptions.each do |tq|
        t, q = tq
        tree.subscribe(t, session, q)
      end
    end

    calls = 0
    tree.each_entry "a/b" do |_session, qos, _filter|
      qos.should eq 0u8
      calls += 1
    end
    calls.should eq 4
  end

  it "unsubscribe unsubscribes" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    test_data = [
      {"session1", [{"a/b", 0u8}]},
      {"session2", [{"a/b", 0u8}]},
      {"session3", [{"a/c", 0u8}]},
      {"session4", [{"a/+", 0u8}]},
      {"session5", [{"#", 0u8}]},
    ]

    test_data.each do |session, subscriptions|
      subscriptions.each do |topic, qos|
        tree.subscribe(topic, session, qos)
      end
    end

    test_data[1, 3].each do |session, subscriptions|
      subscriptions.each do |topic, _qos|
        tree.unsubscribe(topic, session)
      end
    end
    calls = 0
    tree.each_entry "a/b" do |_session, _qos, _filter|
      calls += 1
    end
    calls.should eq 2
  end

  it "changes qos level" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    session = "session"
    tree.subscribe("a/b", session, 0u8)
    tree.each_entry "a/b" { |_sess, qos, _filter| qos.should eq 0u8 }
    tree.subscribe("a/b", session, 1u8)
    tree.each_entry "a/b" { |_sess, qos, _filter| qos.should eq 1u8 }
  end

  it "can iterate all entries" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    test_data = [
      {"session", [{"a/b", 0u8}]},
      {"session", [{"a/b/c/d/e", 0u8}]},
      {"session", [{"+/c", 0u8}]},
      {"session", [{"a/+", 0u8}]},
      {"session", [{"#", 0u8}]},
      {"session", [{"a/b/#", 0u8}]},
      {"session", [{"a/+/c", 0u8}]},
    ]

    test_data.each do |session, subscriptions|
      subscriptions.each do |topic, qos|
        tree.subscribe(topic, session, qos)
      end
    end

    calls = 0
    tree.each_entry do |_session, _qos, _filter|
      calls += 1
    end
    calls.should eq 7
  end
end
