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

  describe LavinMQ::Deduplication::SetCache do
    it "insert? returns true the first time and false for a duplicate" do
      cache = LavinMQ::Deduplication::SetCache(String).new
      cache.insert?("a").should be_true
      cache.insert?("a").should be_false
    end

    it "delete makes a key insertable again" do
      cache = LavinMQ::Deduplication::SetCache(String).new
      cache.insert?("a")
      cache.delete("a")
      cache.contains?("a").should be_false
      cache.insert?("a").should be_true
    end

    it "clear removes all keys" do
      cache = LavinMQ::Deduplication::SetCache(String).new
      cache.insert?("a")
      cache.insert?("b")
      cache.clear
      cache.contains?("a").should be_false
      cache.contains?("b").should be_false
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

  def delete(key)
    @counter["delete"] << {key.as(String), nil}
  end

  def clear
  end

  def calls(key : String)
    @counter[key]
  end
end

describe LavinMQ::Deduplication::Deduper do
  describe "dedup_key" do
    it "returns the dedup header value, or nil when absent" do
      deduper = LavinMQ::Deduplication::Deduper.new(MockCache.new)
      with_header = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
        "x-deduplication-header" => "msg1",
      }))
      without = LavinMQ::AMQP::Properties.new
      deduper.dedup_key(LavinMQ::Message.new("ex", "rk", "body", with_header)).should eq "msg1"
      deduper.dedup_key(LavinMQ::Message.new("ex", "rk", "body", without)).should be_nil
    end
  end

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

    it "should not reset x-cache-ttl on expire when using delayed exchange" do
      # RoughTime.unix_ms has 100ms granularity and can lag real time by up to
      # ~200ms, so a message with a short x-delay can be expired out of the
      # delayed queue (near) instantly in real time. Pause RoughTime and travel
      # explicitly (in multiples of 100ms) instead of racing the wall clock.
      cache_ttl = 500
      delay = 100
      x_args = AMQP::Client::Arguments.new({
        "x-delayed-exchange"      => true,
        "x-delayed-type"          => "topic",
        "x-message-deduplication" => true,
        "x-cache-ttl"             => cache_ttl,
        "x-cache-size"            => 1000,
      })
      q_name = "delayed_q"
      with_amqp_server do |s|
        RoughTime.paused do |time|
          with_channel(s) do |ch|
            x = ch.exchange("delayed_ex", "topic", args: x_args)
            q = ch.queue(q_name)
            q.bind(x.name, "#")
            hdrs = AMQP::Client::Arguments.new({
              "x-delay"                => delay,
              "x-deduplication-header" => "msg1",
            })

            exchange = s.vhosts["/"].exchange("delayed_ex").should be_a(LavinMQ::AMQP::Exchange)
            delay_q = exchange.@delayed_queue.should be_a(LavinMQ::AMQP::DelayedExchangeQueue)
            dest_q = s.vhosts["/"].queue(q_name)

            # Publish a message and verify it has been delayed and not thrown
            # away. Time is paused, so it can't expire before we observe it.
            x.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
            wait_for { !delay_q.empty? }
            exchange.dedup_count.should eq 0

            # Publish a second message, verify that it's thrown away
            x.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
            wait_for { exchange.dedup_count == 1 }
            delay_q.message_count.should eq 1

            # Move past the delay but stay within the cache ttl: the delayed
            # message is delivered, and must neither be deduplicated against
            # its own cache entry nor refresh that entry's expire time.
            # There was a bug where the latter happened, see issue #1153.
            time.travel (2 * delay).milliseconds
            wait_for { delay_q.empty? }
            wait_for { dest_q.message_count == 1 }
            exchange.dedup_count.should eq 1

            # Move past the cache ttl, counted from the first publish. If the
            # delivery above had reset it, the entry would still be alive here
            # and the third message would (incorrectly) be deduplicated.
            time.travel (cache_ttl - delay).milliseconds
            x.publish_confirm "test message", "rk", props: AMQP::Client::Properties.new(headers: hdrs)
            exchange.dedup_count.should eq 1
            wait_for { !delay_q.empty? }
          end
        end
      end
    end
  end
end
