require "./spec_helper"
require "../src/lavinmq/schema"

private def vhost_dir(data_dir) : String
  File.write(File.join(data_dir, "vhosts.json"), %([{"name":"/","dir":"#{Digest::SHA1.hexdigest("/")}"}]))
  dir = File.join(data_dir, Digest::SHA1.hexdigest("/"))
  Dir.mkdir_p dir
  dir
end

# Messages in vhost level segments, queue indexes in enq/ack files
private def write_pre_v1_layout(data_dir)
  dir = vhost_dir(data_dir)
  File.open(File.join(dir, "msgs.0000000001"), "w", &.write_bytes(1))
  queue_dir = File.join(dir, Digest::SHA1.hexdigest("q1"))
  Dir.mkdir_p queue_dir
  File.open(File.join(queue_dir, "enq"), "w", &.write_bytes(3))
  File.open(File.join(queue_dir, "ack"), "w", &.write_bytes(3))
end

# Messages in queue level segments prefixed with the schema version
private def write_v4_layout(data_dir)
  queue_dir = File.join(vhost_dir(data_dir), Digest::SHA1.hexdigest("q2"))
  Dir.mkdir_p queue_dir
  File.open(File.join(queue_dir, "msgs.0000000001"), "w", &.write_bytes(LavinMQ::Schema::VERSION))
end

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

    it "Doesn't read a file with a zero version but data after it as empty" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "test_schema_version")
        MFile.open(path, 12) do |file|
          file.write_bytes 0
          file.write_bytes 1i64
          expect_raises(LavinMQ::OutdatedSchemaVersion) do
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

    it "Deletes empty file and creates a new when it is the last file" do
      with_amqp_server do |s|
        v = s.vhosts["/"]
        v.declare_queue("q", true, false)
        data_dir = s.vhosts["/"].queue("q").as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
        path = File.join(data_dir, "msgs.0000000002")
        MFile.open(path, LavinMQ::Config.instance.segment_size) do |file|
          file.resize(LavinMQ::Config.instance.segment_size)
        end
        # init new message store
        msg_store = LavinMQ::MessageStore.new(data_dir, nil)
        msg_store.@segments.size.should eq 2
        msg_store.@segments.last_value.size.should eq 4
      end
    end
  end

  describe "data directory" do
    it "writes schema_version to a new data directory" do
      with_datadir do |data_dir|
        LavinMQ::Schema.verify(data_dir, nil)
        File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
      end
    end

    it "recreates a missing schema_version in a 1.0+ data directory" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("schema_q", durable: true)
          3.times { q.publish_confirm "m" }
        end
        s.stop
        path = File.join(s.data_dir, "schema_version")
        File.delete path
        restart_server(s)
        File.read(path).should eq LavinMQ::Schema::VERSION.to_s
        s.vhosts["/"].queue("schema_q").message_count.should eq 3
      end
    end

    it "recreates a missing schema_version when no vhost has a queue" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), "[]")
        LavinMQ::Schema.verify(data_dir, nil)
        File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
      end
    end

    it "refuses a pre 1.0 data directory without schema_version" do
      with_datadir do |data_dir|
        write_pre_v1_layout(data_dir)
        expect_raises(LavinMQ::UnsupportedSchemaVersion, /LavinMQ < 1.0/) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
        File.exists?(File.join(data_dir, "schema_version")).should be_false
      end
    end

    it "refuses a data directory with both pre 1.0 and 1.0+ files" do
      with_datadir do |data_dir|
        write_pre_v1_layout(data_dir)
        write_v4_layout(data_dir)
        expect_raises(LavinMQ::UnsupportedSchemaVersion, /write 4 to/) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
        File.exists?(File.join(data_dir, "schema_version")).should be_false
      end
    end

    {"empty" => "", "garbage" => "foo"}.each do |desc, content|
      it "recreates an #{desc} schema_version in a 1.0+ data directory" do
        with_datadir do |data_dir|
          write_v4_layout(data_dir)
          File.write(File.join(data_dir, "schema_version"), content)
          LavinMQ::Schema.verify(data_dir, nil)
          File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
        end
      end

      it "refuses an #{desc} schema_version in a pre 1.0 data directory" do
        with_datadir do |data_dir|
          write_pre_v1_layout(data_dir)
          File.write(File.join(data_dir, "schema_version"), content)
          expect_raises(LavinMQ::UnsupportedSchemaVersion, /LavinMQ < 1.0/) do
            LavinMQ::Schema.verify(data_dir, nil)
          end
        end
      end
    end

    it "refuses an unknown schema version" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "schema_version"), "5")
        expect_raises(LavinMQ::UnsupportedSchemaVersion) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
      end
    end
  end
end
