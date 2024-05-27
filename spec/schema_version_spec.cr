require "./spec_helper"
require "../src/lavinmq/schema"

describe LavinMQ::SchemaVersion do
  describe "Schema version" do
    it "Empty file should raise EmptyFile" do
      data_dir = "/tmp/lavinmq-spec"
      path = File.join(data_dir, "test_schema_version")
      file = MFile.new(path, 12)
      expect_raises(LavinMQ::EmptyFile) do
        LavinMQ::SchemaVersion.verify(file, :message)
      end
    end

    it "Should verify schema version" do
      data_dir = "/tmp/lavinmq-spec"
      path = File.join(data_dir, "test_schema_version")
      file = MFile.new(path, 12)
      file.write_bytes LavinMQ::Schema::VERSION
      LavinMQ::SchemaVersion.verify(file, :message).should eq LavinMQ::SchemaVersion::VERSIONS[:message]
    end

    it "Deletes empty file and creates a new when it is the first file" do
      data_dir = "/tmp/lavinmq-spec"
      path = File.join(data_dir, "msgs.0000000001")
      file = MFile.new(path, LavinMQ::Config.instance.segment_size)
      file.resize(LavinMQ::Config.instance.segment_size)
      # init new message store
      msg_store = LavinMQ::Queue::MessageStore.new(data_dir, nil)
      msg_store.@segments.first[1].size.should eq 4
    end

    it "Deletes empty file when it is second file" do
      v = Server.vhosts["/"].not_nil!
      v.declare_queue("q", true, false)
      data_dir = Server.vhosts["/"].queues["q"].@msg_store.@data_dir
      path = File.join(data_dir, "msgs.0000000002")
      file = MFile.new(path, LavinMQ::Config.instance.segment_size)
      file.resize(LavinMQ::Config.instance.segment_size)
      # init new message store
      msg_store = LavinMQ::Queue::MessageStore.new(data_dir, nil)
      msg_store.@segments.size.should eq 1
    end
  end
end
