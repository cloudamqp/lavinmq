require "./spec_helper"
require "../../src/lavinmq/mqtt/subscription_tree"

# Equal to every other instance, as a subscriber's superclass could be
class ValueEqualBase
  def ==(other : ValueEqualBase)
    true
  end

  def hash(hasher)
    hasher
  end
end

class ValueEqualSubscriber < ValueEqualBase
  include LavinMQ::MQTT::Subscriber

  def deliver(msg : LavinMQ::Message, filter : String, ctx : LavinMQ::MQTT::PublishContext) : Bool
    true
  end
end

describe LavinMQ::MQTT::SubscriptionTree do
  it "keeps subscribers apart that their superclass considers equal" do
    tree = LavinMQ::MQTT::SubscriptionTree(LavinMQ::MQTT::Subscriber).new
    subscribers = Array.new(LavinMQ::MQTT::SubscriberSet::ARRAY_MAX + 1) { ValueEqualSubscriber.new }
    count = -> do
      n = 0
      tree.each_entry("a/b") { n += 1 }
      n
    end
    # The first two in array mode, then all of them in map mode
    tree.subscribe("a/b", subscribers[0], 0u8)
    tree.subscribe("a/b", subscribers[1], 0u8)
    count.call.should eq 2
    subscribers.each { |s| tree.subscribe("a/b", s, 0u8) }
    count.call.should eq subscribers.size
    tree.unsubscribe("a/b", subscribers[0])
    count.call.should eq subscribers.size - 1
  end

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
      tree.@root.get.non_wildcards.size.should eq 0
    end

    it "keeps the entry when other subscribers remain on the filter" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("topic", "session1", 0u8)
      tree.subscribe("topic", "session2", 0u8)
      tree.unsubscribe("topic", "session1")
      tree.@root.get.non_wildcards.size.should eq 1
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

  describe "#each_entry" do
    it "'a/#' matches the parent topic 'a'" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("sport/#", "session", 0u8)
      matched = Array(String).new
      tree.each_entry("sport") { |_s, _q, filter| matched << filter }
      matched.should eq ["sport/#"]
    end

    it "'+/#' matches the single-level topic 'a'" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("+/#", "session", 0u8)
      matched = Array(String).new
      tree.each_entry("sport") { |_s, _q, filter| matched << filter }
      matched.should eq ["+/#"]
    end

    it "'a/#' does not match the sibling topic 'b'" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("sport/#", "session", 0u8)
      matched = Array(String).new
      tree.each_entry("other") { |_s, _q, filter| matched << filter }
      matched.should be_empty
    end

    it "'#' and '+/y' do not match topics starting with '$'" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("#", "hash", 0u8)
      tree.subscribe("+/y", "plus", 0u8)
      matched = Array(String).new
      tree.each_entry("$x/y") { |session, _q, _f| matched << session }
      matched.should be_empty
    end

    it "'$x/#' and '$x/+' match '$x/y'" do
      tree = LavinMQ::MQTT::SubscriptionTree(String).new
      tree.subscribe("$x/#", "hash", 0u8)
      tree.subscribe("$x/+", "plus", 0u8)
      matched = Array(String).new
      tree.each_entry("$x/y") { |session, _q, _f| matched << session }
      matched.sort.should eq ["hash", "plus"]
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

  it "matches topic levels with multibyte UTF-8 characters" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    tree.subscribe("café/naïve/日本", "exact", 0u8)
    tree.subscribe("café/+/日本", "wildcard", 0u8)

    matched = [] of String
    tree.each_entry("café/naïve/日本") { |session, _qos| matched << session }
    matched.sort.should eq ["exact", "wildcard"]

    # A different (also multibyte) middle level only hits the wildcard
    matched.clear
    tree.each_entry("café/résumé/日本") { |session, _qos| matched << session }
    matched.should eq ["wildcard"]
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

  # Publishes walk the tree on several threads while subscribes and
  # unsubscribes on another thread replace it
  it "matches a stable subscription while other subscriptions churn", tags: "slow" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    stable = "stable"
    tree.subscribe("a/+/c", stable, 1u8)
    filters = Array(String).new(200) do |i|
      case i % 4
      when 0 then "a/b#{i}/c"
      when 1 then "a/+/c/#{i}"
      when 2 then "a/b/#"
      else        "a/b/c"
      end
    end
    churners = Array(String).new(50) { |i| "churner#{i}" }
    stop = Atomic(Bool).new(false)
    misses = Atomic(Int32).new(0)
    ctx = Fiber::ExecutionContext::Parallel.new("tree-churn", 4)
    wg = WaitGroup.new(4)
    4.times do
      ctx.spawn do
        i = 0
        until stop.get(:relaxed)
          found = 0
          tree.each_entry("a/b/c") do |s, qos, filter|
            found += 1 if s.same?(stable) && qos == 1u8 && filter == "a/+/c"
          end
          misses.add(1) unless found == 1
          Fiber.yield if (i += 1) % 64 == 0
        end
      ensure
        wg.done
      end
    end
    20.times do
      churners.each { |c| filters.each { |f| tree.subscribe(f, c, 0u8) } }
      churners.each { |c| filters.each { |f| tree.unsubscribe(f, c) } }
    end
    stop.set(true)
    wg.wait
    misses.get.should eq 0
    tree.size.should eq 1
  end

  it "doesn't lose subscriptions from concurrent subscribes and unsubscribes", tags: "slow" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    sessions = Array(String).new(8) { |t| "session#{t}" }
    ctx = Fiber::ExecutionContext::Parallel.new("tree-writers", 4)
    wg = WaitGroup.new(8)
    8.times do |t|
      ctx.spawn do
        100.times do |i|
          filter = i.even? ? "t#{t}/k#{i}/+" : "t#{t}/k#{i}"
          tree.subscribe(filter, sessions[t], 0u8)
          tree.unsubscribe(filter, sessions[t]) if i % 4 >= 2
        end
      ensure
        wg.done
      end
    end
    wg.wait
    tree.size.should eq 8 * 50
    8.times do |t|
      100.times do |i|
        topic = i.even? ? "t#{t}/k#{i}/x" : "t#{t}/k#{i}"
        count = 0
        tree.each_entry(topic) { count += 1 }
        count.should eq(i % 4 >= 2 ? 0 : 1)
      end
    end
  end

  it "keeps a tree read before a change unchanged" do
    tree = LavinMQ::MQTT::SubscriptionTree(String).new
    tree.subscribe("a/+", "s1", 0u8)
    before = tree.@root.get
    tree.subscribe("a/+", "s2", 1u8)
    tree.unsubscribe("a/+", "s1")
    seen = [] of String
    before.each_entry("a/b") { |s, _, _| seen << s }
    seen.should eq ["s1"]
    seen.clear
    tree.each_entry("a/b") { |s, _, _| seen << s }
    seen.should eq ["s2"]
  end
end

describe LavinMQ::MQTT::SubscriberSet do
  it "switches between array and map at the size limits, keeping its contents" do
    limit = LavinMQ::MQTT::SubscriberSet::ARRAY_MAX
    subscribers = Array(String).new(limit + 10) { |i| "s#{i}" }
    ref = Hash(String, UInt8).new
    set = LavinMQ::MQTT::SubscriberSet(String).empty
    check = ->(current : LavinMQ::MQTT::SubscriberSet(String)) do
      current.size.should eq ref.size
      seen = Hash(String, UInt8).new
      current.each { |s, q| seen[s] = q }
      seen.should eq ref
    end
    subscribers.each_with_index do |s, i|
      ref[s] = (i % 3).to_u8
      set = set.put(s, (i % 3).to_u8)
      check.call(set)
    end
    set.@map.should_not be_nil
    # Changing the QoS of a subscriber replaces it, the same QoS is a no-op
    set.put(subscribers[0], 0u8).should be(set)
    ref[subscribers[0]] = 2u8
    set = set.put(subscribers[0], 2u8)
    check.call(set)
    set.delete("missing").should be(set)
    subscribers.each do |s|
      ref.delete(s)
      set = set.delete(s)
      check.call(set)
    end
    set.@entries.should_not be_nil
    set.empty?.should be_true
  end
end
