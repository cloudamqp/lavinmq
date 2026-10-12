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

private def msgs_files(dir) : Array(String)
  Dir.children(dir).select(&.starts_with?("msgs.")).sort!
end

private def with_store_dir(&)
  dir = File.tempname("session_message_store")
  Dir.mkdir_p(dir)
  yield dir
ensure
  FileUtils.rm_rf(dir) if dir
end

describe LavinMQ::MQTT::SessionMessageStore do
  describe "without a message" do
    it "creates and maps no segment" do
      with_store_dir do |dir|
        mmaps = MFile.mmap_count
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        MFile.mmap_count.should eq mmaps
        msgs_files(dir).should be_empty
        store.@segments.should be_empty
        store.close
        MFile.mmap_count.should eq mmaps
        msgs_files(dir).should be_empty
      end
    end

    it "reads as empty" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.empty?.should be_true
        store.size.should eq 0
        store.first?.should be_nil
        store.shift?.should be_nil
        store.shift_with_lease?(Mutex.new) { }.should be_false
        store.includes?(LavinMQ::SegmentPosition.new(1u32, 4u32, 10u32)).should be_false
        store.purge.should eq 0
        store.purge_all
        store.empty?.should be_true
        msgs_files(dir).should be_empty
        store.close
      end
    end

    it "can be deleted" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.delete
        Dir.exists?(dir).should be_false
      end
    end

    it "stays without a segment when reopened" do
      with_store_dir do |dir|
        LavinMQ::MQTT::SessionMessageStore.new(dir, nil).close
        mmaps = MFile.mmap_count
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        MFile.mmap_count.should eq mmaps
        store.empty?.should be_true
        store.close
        msgs_files(dir).should be_empty
      end
    end
  end

  describe "first message" do
    it "creates segment 1 and reads back" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        empty = store.empty.when_false
        mmaps = MFile.mmap_count
        sp = store.push(LavinMQ::Message.new("ex", "rk", "body"))
        MFile.mmap_count.should eq mmaps + 1
        {sp.segment, sp.position}.should eq({1u32, 4u32})
        msgs_files(dir).should eq ["msgs.0000000001"]
        select
        when empty.receive?
        when timeout(1.second) then fail "empty never went false"
        end
        store.includes?(sp).should be_true
        String.new(store.first?.not_nil!.message.body).should eq "body"
        env = store.shift?.should_not be_nil
        env.segment_position.should eq sp
        String.new(env.message.body).should eq "body"
        store.shift?.should be_nil
        store.requeue(sp)
        store.shift?.not_nil!.segment_position.should eq sp
        store.delete(sp)
        store.empty?.should be_true
        store.close
      end
    end

    it "is kept across a reopen" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.push(LavinMQ::Message.new("ex", "rk", "1"))
        store.push(LavinMQ::Message.new("ex", "rk", "2"))
        store.close
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.size.should eq 2
        String.new(store.shift?.not_nil!.message.body).should eq "1"
        String.new(store.shift?.not_nil!.message.body).should eq "2"
        store.close
      end
    end

    # The packet id log refers to messages by position, and a missing one
    # is taken as deleted (see #includes?)
    it "never reuses a position once the store has emptied" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        sp = store.push(LavinMQ::Message.new("ex", "rk", "1"))
        store.shift?
        store.delete(sp)
        store.close
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.empty?.should be_true
        store.includes?(sp).should be_false
        sp2 = store.push(LavinMQ::Message.new("ex", "rk", "2"))
        {sp2.segment, sp2.position}.should_not eq({sp.segment, sp.position})
        store.includes?(sp).should be_false
        store.close
      end
    end

    it "rolls over into later segments" do
      segment_size = LavinMQ::Config.instance.segment_size
      LavinMQ::Config.instance.segment_size = 64 # one message per segment
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        sps = (1..3).map { |i| store.push(LavinMQ::Message.new("ex", "rk", "message #{i}")) }
        sps.map(&.segment).should eq [1u32, 2u32, 3u32]
        bodies = Array(String).new
        while env = store.shift?
          bodies << String.new(env.message.body)
          store.delete(env.segment_position)
        end
        bodies.should eq ["message 1", "message 2", "message 3"]
        msgs_files(dir).should eq ["msgs.0000000003"] # the one still written to
        store.close
      end
    ensure
      LavinMQ::Config.instance.segment_size = segment_size.not_nil!
    end

    it "can be created before the first message" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil)
        store.open_first_segment
        msgs_files(dir).should eq ["msgs.0000000001"]
        mmaps = MFile.mmap_count
        store.open_first_segment
        MFile.mmap_count.should eq mmaps
        store.shift?.should be_nil
        sp = store.push(LavinMQ::Message.new("ex", "rk", "body"))
        {sp.segment, sp.position}.should eq({1u32, 4u32})
        String.new(store.shift?.not_nil!.message.body).should eq "body"
        store.close
        expect_raises(LavinMQ::MessageStore::ClosedError) { store.open_first_segment }
      end
    end

    it "leaves no files behind when not durable" do
      with_store_dir do |dir|
        store = LavinMQ::MQTT::SessionMessageStore.new(dir, nil, durable: false)
        store.push(LavinMQ::Message.new("ex", "rk", "body"))
        msgs_files(dir).should be_empty # unlinked at creation
        String.new(store.shift?.not_nil!.message.body).should eq "body"
        store.close
      end
    end
  end
end

describe LavinMQ::MessageStore do
  it "creates its first segment up front, unlike a session's store" do
    with_store_dir do |dir|
      store = LavinMQ::MessageStore.new(dir, nil)
      msgs_files(dir).should eq ["msgs.0000000001"]
      store.close
    end
  end
end
