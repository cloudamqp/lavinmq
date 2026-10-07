require "../spec_helper"

alias PacketIdLog = LavinMQ::MQTT::PacketIdLog

# Lets a spec fail the directory fsync that follows a rename
module LavinMQ::FileSystem
  class_property? fail_fsync_dir = false

  def self.fsync_dir(path : String) : Nil
    raise IO::Error.new("injected fsync_dir failure") if fail_fsync_dir?
    previous_def
  end
end

private def with_log_path(&)
  dir = File.tempname("packet_id_log")
  Dir.mkdir_p(dir)
  yield File.join(dir, "packet_ids.log")
ensure
  FileUtils.rm_rf(dir) if dir
end

describe LavinMQ::MQTT::PacketIdLog do
  it "reloads held ids in both directions" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      log.publish_received(2u16)
      log.pubrel_received(1u16)
      log.publish_sent(1u16, LavinMQ::SegmentPosition.new(3u32, 40u32, 0u32))
      log.publish_sent(9u16, LavinMQ::SegmentPosition.new(3u32, 80u32, 0u32))
      log.pubcomp_received(9u16)
      log.close

      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should eq Set{2u16}
      log.publish_sent.keys.should eq [1u16]
      sp = log.publish_sent[1u16]
      {sp.segment, sp.position}.should eq({3u32, 40u32})
      log.close
    end
  end

  it "keeps the same id held inbound and outbound apart" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(5u16)
      log.publish_sent(5u16, LavinMQ::SegmentPosition.new(1u32, 4u32, 0u32))
      log.pubrel_received(5u16)
      log.close
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should be_empty
      log.publish_sent.has_key?(5u16).should be_true
      log.close
    end
  end

  it "loads up to a torn tail and truncates it" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      log.close
      size = File.size(path)
      File.open(path, "a") { |f| f.write Bytes[PacketIdLog::State::PublishSent.value, 2u8] }
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should eq Set{1u16}
      log.close # an open log's file is capacity-sized
      File.size(path).should eq size
    end
  end

  it "stops at a zeroed or unknown state byte mid-file" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      log.close
      File.open(path, "a") { |f| f.write Bytes[0u8, 0u8, 0u8, 1u8, 2u8, 0u8] }
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should eq Set{1u16}
      log.publish_received(3u16) # still appendable after the truncate
      log.close
      PacketIdLog.new(path, nil, nil).awaiting_pubrel.should eq Set{1u16, 3u16}
    end
  end

  it "recreates the file when the header is missing or invalid" do
    with_log_path do |path|
      File.write(path, "xy")
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should be_empty
      log.publish_received(1u16)
      log.close
      PacketIdLog.new(path, nil, nil).awaiting_pubrel.should eq Set{1u16}
    end
  end

  it "writes the documented little-endian layout" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(9u16)
      log.publish_sent(5u16, LavinMQ::SegmentPosition.new(3u32, 40u32, 0u32))
      log.close
      File.read(path).to_slice.should eq Bytes[1, 0, 0, 0, 1, 9, 0, 3, 5, 0, 3, 0, 0, 0, 40, 0, 0, 0]
    end
  end

  it "loads up to a torn tail inside a PublishSent payload and truncates it" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      log.close
      size = File.size(path)
      File.open(path, "a") { |f| f.write Bytes[PacketIdLog::State::PublishSent.value, 2u8, 0u8, 3u8, 0u8] }
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should eq Set{1u16}
      log.publish_sent.should be_empty
      log.close
      File.size(path).should eq size
    end
  end

  it "compacts to the live set when the file is full" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(7u16)
      log.publish_sent(8u16, LavinMQ::SegmentPosition.new(6u32, 123u32, 0u32))
      # Twice what fits, so it compacts more than once
      (PacketIdLog::MIN_CAPACITY // (2 * PacketIdLog::RECORD_SIZE) * 2).times do |i|
        id = (100 + i % 50).to_u16
        log.publish_received(id)
        log.pubrel_received(id)
      end
      File.size(path).should eq PacketIdLog::MIN_CAPACITY
      log.close
      File.size(path).should be < 4 + 3 * 100
      log = PacketIdLog.new(path, nil, nil)
      log.awaiting_pubrel.should eq Set{7u16}
      sp = log.publish_sent[8u16]
      {sp.segment, sp.position}.should eq({6u32, 123u32})
      log.close
    end
  end

  # Invariant guard: the old file is full, so the next append compacts again.
  # A compaction trigger that leaves room in it would append to the unlinked file.
  it "appends to the file at its path after a compaction fails past the rename" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(7u16)
      mfile = log.@mfile.not_nil!
      while mfile.size + PacketIdLog::RECORD_SIZE <= mfile.capacity
        log.pubrel_received(100u16)
      end
      LavinMQ::FileSystem.fail_fsync_dir = true
      begin
        expect_raises(PacketIdLog::Error) { log.publish_received(8u16) }
      ensure
        LavinMQ::FileSystem.fail_fsync_dir = false
      end
      log.publish_received(9u16)
      log.close
      PacketIdLog.new(path, nil, nil).awaiting_pubrel.should eq Set{7u16, 9u16}
    end
  end

  it "loads a crashed log's capacity-sized file and appends after its last record" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      crashed = File.join(File.dirname(path), "crashed.log")
      # Not closed, so not truncated: the shape a crash leaves
      File.copy(path, crashed)
      log.close
      File.size(crashed).should eq PacketIdLog::MIN_CAPACITY
      log = PacketIdLog.new(crashed, nil, nil)
      log.awaiting_pubrel.should eq Set{1u16}
      log.publish_received(2u16)
      log.close
      File.read(crashed).to_slice.should eq Bytes[1, 0, 0, 0, 1, 1, 0, 1, 2, 0]
    end
  end

  it "creates its file only with the first record" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      File.exists?(path).should be_false
      log.publish_received(1u16)
      log.close
      File.read(path).to_slice.should eq Bytes[1, 0, 0, 0, 1, 1, 0]
    end
  end

  it "does not create its file once closed" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.close
      expect_raises(PacketIdLog::Error) { log.publish_received(1u16) }
      File.exists?(path).should be_false
    end
  end

  it "deletes its file" do
    with_log_path do |path|
      log = PacketIdLog.new(path, nil, nil)
      log.publish_received(1u16)
      log.delete
      File.exists?(path).should be_false
    end
  end
end
