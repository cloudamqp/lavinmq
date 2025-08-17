require "./spec_helper"
require "../src/lavinmq/schema"

describe LavinMQ::SchemaVersion do
  describe "Schema version" do
    it "Empty file should raise IO::EOFError" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "test_schema_version")
        MFile.open(path, 12) do |file|
          expect_raises(IO::EOFError) do
            LavinMQ::SchemaVersion.verify(file, :message)
          end
        end
      end
    end

    it "Should verify schema version" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "test_schema_version")
        MFile.open(path, 12) do |file|
          file.write_bytes LavinMQ::Schema::VERSION
          LavinMQ::SchemaVersion.verify(file, :message).should eq LavinMQ::SchemaVersion::VERSIONS[:message]
        end
      end
    end

    it "Deletes empty file and creates a new when it is the first file" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "msgs.0000000001")
        MFile.open(path, LavinMQ::Config.instance.segment_size) do |file|
          file.resize(LavinMQ::Config.instance.segment_size)
        end
        # init new message store
        msg_store = LavinMQ::MessageStore.new(data_dir, nil)
        msg_store.@segments.first_value.size.should eq 4
      end
    end

    it "Deletes empty file when it is second file" do
      with_amqp_server do |s|
        v = s.vhosts["/"]
        v.declare_queue("q", true, false)
        data_dir = s.vhosts["/"].queue("q").not_nil!.as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
        path = File.join(data_dir, "msgs.0000000002")
        MFile.open(path, LavinMQ::Config.instance.segment_size) do |file|
          file.resize(LavinMQ::Config.instance.segment_size)
        end
        # init new message store
        msg_store = LavinMQ::MessageStore.new(data_dir, nil)
        msg_store.@segments.size.should eq 1
      end
    end
  end
end
