require "./spec_helper"

describe LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore do
  describe "#shift?" do
    it "should return nil when empty" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      store.shift?.should be_nil
    end

    it "should return and remove first element" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.shift?.should eq sp1
      store.shift?.should eq sp2
      store.shift?.should be_nil
    end
  end

  describe "#first?" do
    it "should return nil when empty" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      store.first?.should be_nil
    end

    it "should return first element without removing" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.first?.should eq sp1
      store.first?.should eq sp1
    end
  end

  describe "#insert" do
    it "should raise error for basic insert" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      expect_raises(Exception, /BUG:/) do
        store.insert(sp)
      end
    end

    it "should maintain sorted order by expiration time" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32, delay: 100u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32, delay: 50u32)
      sp3 = LavinMQ::SegmentPosition.new(1u32, 3u32, 10u32, delay: 200u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.insert(sp3, timestamp)
      store.shift?.should eq sp2
      store.shift?.should eq sp1
      store.shift?.should eq sp3
    end

    it "should order by segment position when expiration times are equal" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp3 = LavinMQ::SegmentPosition.new(1u32, 3u32, 10u32, delay: 100u32)
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32, delay: 100u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32, delay: 100u32)
      timestamp = 1000i64
      store.insert(sp3, timestamp)
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.shift?.should eq sp1
      store.shift?.should eq sp2
      store.shift?.should eq sp3
    end
  end

  describe "#time_to_next_expiration?" do
    it "should return nil when empty" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      store.time_to_next_expiration?.should be_nil
    end

    it "should return time span to next expiration" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32, delay: 1000u32)
      timestamp = RoughTime.unix_ms
      store.insert(sp, timestamp)
      time_to_expiration = store.time_to_next_expiration?
      time_to_expiration.should_not be_nil
      time_to_expiration.not_nil!.should be_close(1000.milliseconds, 100.milliseconds)
    end
  end

  describe "#clear" do
    it "should empty the store" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.clear
      store.first?.should be_nil
      store.shift?.should be_nil
      store.time_to_next_expiration?.should be_nil
    end
  end

  describe "#size" do
    it "should return 0 when empty" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      store.size.should eq 0
    end

    it "should return the number of items in the store" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.size.should eq 1
      store.insert(sp2, timestamp)
      store.size.should eq 2
    end

    it "should decrease after shift" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.shift?
      store.size.should eq 1
      store.shift?
      store.size.should eq 0
    end

    it "should be 0 after clear" do
      store = LavinMQ::AMQP::DelayedExchangeQueue::DelayedMessageStore::DelayedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      timestamp = 1000i64
      store.insert(sp1, timestamp)
      store.insert(sp2, timestamp)
      store.clear
      store.size.should eq 0
    end
  end
end
