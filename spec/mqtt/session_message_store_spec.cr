require "../spec_helper"

describe LavinMQ::MQTT::SessionMessageStore::OriginalPacketIds do
  it "frees an id once no message holds it" do
    ids = LavinMQ::MQTT::SessionMessageStore::OriginalPacketIds.new
    a = LavinMQ::SegmentPosition.new(1u32, 4u32, 10u32)
    b = LavinMQ::SegmentPosition.new(1u32, 14u32, 10u32)
    ids[a] = 7u16
    ids[b] = 7u16
    ids.delete(a).should eq 7u16
    ids.in_use?(7u16).should be_true
    ids.delete(b)
    ids.in_use?(7u16).should be_false
  end

  it "frees the old id when a message gets a new one" do
    ids = LavinMQ::MQTT::SessionMessageStore::OriginalPacketIds.new
    sp = LavinMQ::SegmentPosition.new(1u32, 4u32, 10u32)
    ids[sp] = 7u16
    ids[sp] = 8u16
    ids.in_use?(7u16).should be_false
    ids.in_use?(8u16).should be_true
    ids.size.should eq 1
  end
end

describe LavinMQ::MQTT::SessionMessageStore do
  it "releases a dropped packet id only once its delete is written" do
    dir = File.tempname("session_message_store")
    Dir.mkdir_p(dir)
    store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
    store.push(LavinMQ::Message.new("ex", "rk", "body"))
    sp = store.shift?.should_not be_nil
    sp = sp.segment_position
    store.remember_original_packet_id(sp, 5u16)
    acks_at_release = nil
    store.on_original_packet_id_dropped = ->(_sp : LavinMQ::SegmentPosition, _id : UInt16) { true }
    store.on_original_packet_id_released = ->(_id : UInt16) do
      acks_at_release = store.@acks[sp.segment].size
      nil
    end
    store.delete(sp)
    acks_at_release.should eq sizeof(UInt32)
  ensure
    store.try &.close
    FileUtils.rm_rf(dir) if dir
  end
end
