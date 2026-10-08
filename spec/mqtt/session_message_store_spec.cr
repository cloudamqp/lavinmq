require "../spec_helper"

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
