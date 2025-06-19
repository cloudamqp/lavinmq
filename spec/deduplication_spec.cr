require "./spec_helper"
require "../src/lavinmq/deduplication.cr"

describe LavinMQ::Deduplication do
  describe LavinMQ::Deduplication::MemoryCache do
    it "should default cache size" do
      cache = LavinMQ::Deduplication::MemoryCache(String).new
      (cache.@size + 1).times do |n|
        cache.insert("#{n}")
      end
      cache.contains?("0").should be_false
      cache.contains?("1").should be_true
      cache.contains?("#{cache.@size}").should be_true
      cache.contains?("#{cache.@size + 1}").should be_false
      cache.@store.@size.should eq cache.@size
    end

    it "should have max size as optional setting" do
      cache = LavinMQ::Deduplication::MemoryCache(String).new(2)
      cache.insert("item1")
      cache.insert("item2")
      cache.insert("item3")
      cache.contains?("item1").should be_false
      cache.contains?("item2").should be_true
      cache.contains?("item3").should be_true
    end

    it "should store item without ttl" do
      cache = LavinMQ::Deduplication::MemoryCache(String).new(10)
      cache.insert("item1")
      cache.contains?("item1").should be_true
      cache.contains?("item2").should be_false
    end

    it "should respect ttl" do
      RoughTime.paused do |t|
        cache = LavinMQ::Deduplication::MemoryCache(String).new(3)
        cache.insert("item1", 1)
        cache.insert("item2", 300)
        cache.insert("item3")
        t.travel 0.2.seconds
        cache.contains?("item1").should be_false
        cache.contains?("item2").should be_true
        cache.contains?("item3").should be_true
      end
    end
  end
end

class MockCache < LavinMQ::Deduplication::Cache(AMQ::Protocol::Field)
  @counter = Hash(String, Array({String, UInt32?})).new do |h, k|
    h[k] = Array({String, UInt32?}).new
  end

  def contains?(key) : Bool
    @counter["contains?"] << {key.as(String), nil}
    false
  end

  def insert(key, ttl = nil)
    @counter["insert"] << {key.as(String), ttl}
  end

  def calls(key : String)
    @counter[key]
  end
end

describe LavinMQ::Deduplication::Deduper do
  describe "duplicate?" do
    it "should return false if \"x-deduplication-header\" is missing (no identifier, always unique)" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock)
      props = LavinMQ::AMQP::Properties.new
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      res = deduper.duplicate?(msg)
      res.should be_false
    end

    it "should check cache if entry exists" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.duplicate?(msg)
      mock.calls("contains?").size.should eq 1
    end

    it "should only insert into cache if header has a value" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new)
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      mock.calls("insert").size.should eq 0
    end

    it "should only insert into cache if header has a value" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      mock.calls("insert").size.should eq 1
    end

    it "should respect x-cache-ttl on message" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
        "x-cache-ttl"            => 10,
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      calls = mock.calls("insert")
      calls.first[0].should eq "msg1"
      calls.first[1].should eq 10
    end

    it "should fallback to default ttl" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock, 12)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      calls = mock.calls("insert")
      calls.first[0].should eq "msg1"
      calls.first[1].should eq 12
    end

    it "should prio message ttl over default ttl" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock, 12)
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
        "x-cache-ttl"            => 10,
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      calls = mock.calls("insert")
      calls.first[0].should eq "msg1"
      calls.first[1].should eq 10
    end
    it "should allow checking any header for dedups" do
      mock = MockCache.new
      deduper = LavinMQ::Deduplication::Deduper.new(mock, 10, "custom")
      props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "custom" => "msg1",
      }))
      msg = LavinMQ::Message.new("ex", "rk", "body", props)
      deduper.add(msg)
      calls = mock.calls("insert")
      calls.first[0].should eq "msg1"
      calls.first[1].should eq 10
    end
  end
end
