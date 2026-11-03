require "spec"
require "wait_group"
require "../src/lavinmq/persistent_map"

private module Named
  abstract def name : String
end

private class Other
end

private class Value
  include Named
  getter name : String

  def initialize(@name)
  end
end

# A key with a chosen hash, to force shared fragments and full collisions
private class FixedHashKey
  getter name : String

  def initialize(@name, @hash : UInt64)
  end

  def hash : UInt64
    @hash
  end

  def ==(other : FixedHashKey)
    @name == other.name
  end
end

private def assert_same_contents(map : LavinMQ::PersistentMap(K, V), ref : Hash(K, V)) forall K, V
  map.size.should eq ref.size
  ref.each { |k, v| map[k]?.should be(v) }
  seen = 0
  map.each do |k, v|
    ref[k].should be(v)
    seen += 1
  end
  seen.should eq ref.size
end

describe LavinMQ::PersistentMap do
  it "matches a Hash under random puts, replaces and deletes" do
    rnd = Random.new(1)
    ref = Hash(String, Value).new
    map = LavinMQ::PersistentMap(String, Value).new
    20_000.times do
      key = "k#{rnd.rand(2_000)}"
      if rnd.rand(3) == 0
        ref.delete(key)
        map = map.delete(key)
      else
        value = Value.new(key)
        ref[key] = value
        map = map.put(key, value)
      end
      map.size.should eq ref.size
    end
    assert_same_contents(map, ref)
    map["missing"]?.should be_nil
  end

  it "leaves earlier versions unchanged" do
    a = Value.new("a")
    v1 = LavinMQ::PersistentMap(String, Value).new.put("a", a)
    v2 = v1.put("b", Value.new("b"))
    v3 = v2.delete("a")
    a2 = Value.new("a2")
    v4 = v2.put("a", a2)
    v1.keys.should eq ["a"]
    v2.keys.sort!.should eq ["a", "b"]
    v3.keys.should eq ["b"]
    v1["a"].should be(a)
    v2["a"].should be(a)
    v4["a"].should be(a2)
    v4.size.should eq 2
    v3["a"]?.should be_nil
  end

  it "leaves earlier versions unchanged when replacing in a collision node" do
    k0 = FixedHashKey.new("x", 42u64)
    k1 = FixedHashKey.new("y", 42u64)
    a = Value.new("a")
    v1 = LavinMQ::PersistentMap(FixedHashKey, Value).new.put(k0, a).put(k1, Value.new("b"))
    v2 = v1.put(k0, Value.new("a2"))
    v1[k0].should be(a)
    v2[k0].name.should eq "a2"
  end

  it "returns the removed value" do
    a = Value.new("a")
    map = LavinMQ::PersistentMap(String, Value).new.put("a", a)
    rest, value = map.delete_with_value("a")
    value.should be(a)
    rest.empty?.should be_true
    same, missing = map.delete_with_value("b")
    missing.should be_nil
    same.should be(map)
  end

  it "raises KeyError for a missing key" do
    expect_raises(KeyError) { LavinMQ::PersistentMap(String, Value).new["a"] }
  end

  it "returns itself when putting the value a key already has" do
    a = Value.new("a")
    k = FixedHashKey.new("x", 42u64)
    map = LavinMQ::PersistentMap(FixedHashKey, Value).new
      .put(k, a).put(FixedHashKey.new("y", 42u64), Value.new("b"))
      .put(FixedHashKey.new("z", 7u64), Value.new("c"))
    map.put(k, a).should be(map)
    s = LavinMQ::PersistentMap(String, Value).new
    100.times { |i| s = s.put("k#{i}", Value.new("v")) }
    s.put("k5", s["k5"]).should be(s)
    s.put("k5", Value.new("v")).should_not be(s)
  end

  it "holds unions and modules of reference types" do
    map = LavinMQ::PersistentMap(String, Value | Other).new
      .put("v", Value.new("v")).put("o", Other.new)
    map["v"].as(Value).name.should eq "v"
    map["o"].should be_a(Other)
    named = LavinMQ::PersistentMap(String, Named).new.put("v", Value.new("v"))
    named["v"].name.should eq "v"
  end

  it "returns itself when deleting a missing key" do
    map = LavinMQ::PersistentMap(String, Value).new.put("a", Value.new("a"))
    map.delete("b").should be(map)
  end

  it "handles keys whose hashes share fragments or collide completely" do
    ref = Hash(FixedHashKey, Value).new
    map = LavinMQ::PersistentMap(FixedHashKey, Value).new
    keys = [] of FixedHashKey
    # Same low 60 bits, different top bits: a chain of single-child nodes
    4.times { |i| keys << FixedHashKey.new("deep#{i}", (i.to_u64 << 60) | 0xABCDEF) }
    # Identical 64-bit hashes: a collision node
    5.times { |i| keys << FixedHashKey.new("same#{i}", 42u64) }
    # Sharing low fragments with the collision node's hash: splits it
    2.times { |i| keys << FixedHashKey.new("near#{i}", 42u64 | ((i.to_u64 + 1) << 20)) }
    keys.each do |k|
      ref[k] = v = Value.new(k.name)
      map = map.put(k, v)
    end
    assert_same_contents(map, ref)
    # Replace in a collision node
    k = keys[-3]
    ref[k] = v = Value.new("new")
    map = map.put(k, v)
    assert_same_contents(map, ref)
    # Delete everything, checking after every step
    keys.shuffle(Random.new(2)).each do |key|
      ref.delete(key)
      map = map.delete(key)
      assert_same_contents(map, ref)
    end
    map.empty?.should be_true
  end
end

describe "LavinMQ::PersistentMap#find_value" do
  it "finds a key by its hash and an equality block" do
    map = LavinMQ::PersistentMap(FixedHashKey, Value).new
    keys = Array(FixedHashKey).new(40) { |i| FixedHashKey.new("k#{i}", (i % 7).to_u64 &* 0x9E3779B97F4A7C15u64) }
    keys.each { |k| map = map.put(k, Value.new(k.name)) }
    keys.each do |k|
      map.find_value(hash: k.hash) { |key| key.name == k.name }.should_not be_nil
      map.find_value(hash: k.hash) { |key| key.name == k.name }.try(&.name).should eq k.name
    end
    map.find_value(hash: keys[0].hash) { |key| key.name == "missing" }.should be_nil
    map.find_value(hash: 12345u64) { true }.should be_nil
  end

  it "picks the right key in a collision node" do
    map = LavinMQ::PersistentMap(FixedHashKey, Value).new
    5.times { |i| map = map.put(FixedHashKey.new("same#{i}", 42u64), Value.new("v#{i}")) }
    5.times do |i|
      map.find_value(hash: 42u64) { |key| key.name == "same#{i}" }.try(&.name).should eq "v#{i}"
    end
    map.find_value(hash: 42u64) { |key| key.name == "same9" }.should be_nil
  end
end

describe LavinMQ::CowMap do
  it "supports Hash-like reads and writes" do
    map = LavinMQ::CowMap(String, Value).new
    a = Value.new("a")
    (map["a"] = a).should be(a)
    map["a"].should be(a)
    map.has_key?("a").should be_true
    map.size.should eq 1
    map.values.should eq [a]
    map.keys.should eq ["a"]
    map.any? { |(k, _)| k == "a" }.should be_true
    map.to_a.should eq [{"a", a}]
    map.count { true }.should eq 1
    seen = [] of String
    map.each { |(k, _)| seen << k }
    map.each_value { |v| seen << v.name }
    seen.should eq ["a", "a"]
    map.delete("a").should be(a)
    map.delete("a").should be_nil
    map.empty?.should be_true
  end

  it "keeps a snapshot unchanged while writers continue" do
    map = LavinMQ::CowMap(String, Value).new
    10.times { |i| map["k#{i}"] = Value.new("k#{i}") }
    snapshot = map.snapshot
    map.clear
    snapshot.size.should eq 10
    map.size.should eq 0
  end

  it "can be read from many threads while a writer updates it", tags: "slow" do
    map = LavinMQ::CowMap(String, Value).new
    stable = Array(Value).new(1_000) { |i| Value.new("stable#{i}") }
    stable.each { |v| map[v.name] = v }
    stop = Atomic(Bool).new(false)
    errors = Atomic(Int32).new(0)
    ctx = Fiber::ExecutionContext::Parallel.new("persistent-map-spec", 4)
    wg = WaitGroup.new(10)
    8.times do |r|
      ctx.spawn do
        i = r
        until stop.get(:relaxed)
          v = stable.unsafe_fetch(i % stable.size)
          errors.add(1) unless map[v.name]?.same?(v)
          i += 1
          Fiber.yield if i % 1024 == 0
        end
      ensure
        wg.done
      end
    end
    # Iterating snapshots: every stable key, and at most the one tmp key
    2.times do
      ctx.spawn do
        until stop.get(:relaxed)
          stable_seen = tmp_seen = 0
          map.snapshot.each { |(k, _)| k.starts_with?("tmp") ? (tmp_seen += 1) : (stable_seen += 1) }
          errors.add(1) unless stable_seen == stable.size && tmp_seen <= 1
          Fiber.yield
        end
      ensure
        wg.done
      end
    end
    20_000.times do |i|
      map["tmp#{i}"] = Value.new("tmp")
      map.delete("tmp#{i}")
    end
    stop.set(true)
    wg.wait
    errors.get.should eq 0
    map.size.should eq stable.size
  end
  it "doesn't lose updates from concurrent writers", tags: "slow" do
    map = LavinMQ::CowMap(String, Value).new
    ctx = Fiber::ExecutionContext::Parallel.new("persistent-map-writers", 4)
    wg = WaitGroup.new(4)
    4.times do |w|
      ctx.spawn do
        2_000.times do |i|
          map["w#{w}-#{i}"] = Value.new("v")
          map.delete("w#{w}-#{i}") if i.odd?
        end
      ensure
        wg.done
      end
    end
    wg.wait
    map.size.should eq 4 * 1_000
    4.times do |w|
      2_000.times do |i|
        if i.odd?
          map["w#{w}-#{i}"]?.should be_nil
        else
          map["w#{w}-#{i}"]?.should_not be_nil
        end
      end
    end
  end
end
