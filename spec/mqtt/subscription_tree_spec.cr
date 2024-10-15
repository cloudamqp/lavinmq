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

  it "subscriptions is found" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    test_data = [
      {"session", [{"a/b", 0u8}]},
      {"session", [{"a/b", 0u8}]},
      {"session", [{"a/c", 0u8}]},
      {"session", [{"a/+", 0u8}]},
      {"session", [{"#", 0u8}]},
    ]

    test_data.each do |s|
      session, subscriptions = s
      subscriptions.each do |tq|
        t, q = tq
        tree.subscribe(t, session, q)
      end
    end

    calls = 0
    tree.each_entry "a/b" do |_session, qos|
      qos.should eq 0u8
      calls += 1
    end
    calls.should eq 4
  end

  it "unsubscribe unsubscribes" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    test_data = [
      {"session", [{"a/b", 0u8}]},
      {"session", [{"a/b", 0u8}]},
      {"session", [{"a/c", 0u8}]},
      {"session", [{"a/+", 0u8}]},
      {"session", [{"#", 0u8}]},
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
    tree.each_entry "a/b" do |_session, _qos|
      calls += 1
    end
    calls.should eq 2
  end

  it "changes qos level" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    session = "session"
    tree.subscribe("a/b", session, 0u8)
    tree.each_entry "a/b" { |_sess, qos| qos.should eq 0u8 }
    tree.subscribe("a/b", session, 1u8)
    tree.each_entry "a/b" { |_sess, qos| qos.should eq 1u8 }
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
    tree.each_entry do |_session, _qos|
      calls += 1
    end
    calls.should eq 7
  end
end
