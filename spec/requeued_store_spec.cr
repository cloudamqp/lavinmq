require "spec"
require "../src/lavinmq/message_store/requeued_store"

pending LavinMQ::MessageStore::PublishOrderedRequeuedStore do
  describe "#shift?" do
    it "should return nil when empty" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      store.shift?.should be_nil
    end

    it "should return and remove first element" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      store.insert(sp1)
      store.insert(sp2)
      store.shift?.should eq sp1
      store.shift?.should eq sp2
      store.shift?.should be_nil
    end
  end

  describe "#first?" do
    it "should return nil when empty" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      store.first?.should be_nil
    end

    it "should return first element without removing" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      store.insert(sp1)
      store.insert(sp2)
      store.first?.should eq sp1
      store.first?.should eq sp1
    end
  end

  describe "#insert" do
    it "should maintain sorted order by segment position" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      sp3 = LavinMQ::SegmentPosition.new(1u32, 3u32, 10u32)
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      store.insert(sp3)
      store.insert(sp1)
      store.insert(sp2)
      store.shift?.should eq sp1
      store.shift?.should eq sp2
      store.shift?.should eq sp3
    end

    it "should handle multiple segments" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      sp2_1 = LavinMQ::SegmentPosition.new(2u32, 1u32, 10u32)
      sp1_2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      sp1_1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      store.insert(sp2_1)
      store.insert(sp1_2)
      store.insert(sp1_1)
      store.shift?.should eq sp1_1
      store.shift?.should eq sp1_2
      store.shift?.should eq sp2_1
    end
  end

  describe "#clear" do
    it "should empty the store" do
      store = LavinMQ::MessageStore::PublishOrderedRequeuedStore.new
      sp1 = LavinMQ::SegmentPosition.new(1u32, 1u32, 10u32)
      sp2 = LavinMQ::SegmentPosition.new(1u32, 2u32, 10u32)
      store.insert(sp1)
      store.insert(sp2)
      store.clear
      store.first?.should be_nil
      store.shift?.should be_nil
    end
  end
end
