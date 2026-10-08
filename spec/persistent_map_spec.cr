require "spec"
require "wait_group"
require "../src/lavinmq/persistent_map"

private class Value
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
    v1.keys.should eq ["a"]
    v2.keys.sort!.should eq ["a", "b"]
    v3.keys.should eq ["b"]
    v1["a"].should be(a)
    v3["a"]?.should be_nil
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
    keys.each do |k|
      ref[k] = v = Value.new(k.name)
      map = map.put(k, v)
    end
    assert_same_contents(map, ref)
    # Replace in a collision node
    k = keys.last
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

describe LavinMQ::CowMap do
  it "supports Hash-like reads and writes" do
    map = LavinMQ::CowMap(String, Value).new
    a = Value.new("a")
    (map["a"] = a).should be(a)
    map["a"].should be(a)
    map.has_key?("a").should be_true
    map.size.should eq 1
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
    wg = WaitGroup.new(8)
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
    20_000.times do |i|
      map["tmp#{i}"] = Value.new("tmp")
      map.delete("tmp#{i}")
    end
    stop.set(true)
    wg.wait
    errors.get.should eq 0
    map.size.should eq stable.size
  end
end
