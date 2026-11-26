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

    it "refuses a pre 1.0 data directory without schema_version" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), %([{"name":"/","dir":"vhost"}]))
        queue_dir = File.join(data_dir, "vhost", "queue")
        Dir.mkdir_p(queue_dir)
        File.write(File.join(queue_dir, "enq"), "")
        expect_raises(LavinMQ::UnsupportedSchemaVersion, /LavinMQ < 1.0/) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
        File.exists?(File.join(data_dir, "schema_version")).should be_false
      end
    end

    it "refuses a data directory that a LavinMQ 2.11 migration left half done" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), %([{"name":"/","dir":"vhost"}]))
        migrated = File.join(data_dir, "vhost", "migrated")
        not_migrated = File.join(data_dir, "vhost", "not-migrated")
        Dir.mkdir_p(migrated)
        Dir.mkdir_p(not_migrated)
        File.write(File.join(migrated, "msgs.0000000001"), "")
        File.write(File.join(not_migrated, "enq"), "")
        expect_raises(LavinMQ::UnsupportedSchemaVersion, /interrupted.*backups/) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
        File.exists?(File.join(data_dir, "schema_version")).should be_false
      end
    end

    it "restores a lost schema_version in a data directory with the current layout" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), %([{"name":"/","dir":"vhost"}]))
        queue_dir = File.join(data_dir, "vhost", "queue")
        Dir.mkdir_p(queue_dir)
        File.write(File.join(queue_dir, "msgs.0000000001"), "")
        LavinMQ::Schema.verify(data_dir, nil)
        File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
      end
    end

    it "only looks in the vhost directories that vhosts.json lists" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), %([{"name":"/","dir":"vhost"}]))
        Dir.mkdir_p(File.join(data_dir, "vhost", "queue"))
        lost_found = File.join(data_dir, "lost+found")
        Dir.mkdir(lost_found, 0o000)
        begin
          LavinMQ::Schema.verify(data_dir, nil)
        ensure
          File.chmod(lost_found, 0o700)
        end
        File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
      end
    end

    it "restores an empty schema_version, as a power loss can leave it" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "vhosts.json"), "[]")
        File.write(File.join(data_dir, "schema_version"), "")
        LavinMQ::Schema.verify(data_dir, nil)
        File.read(File.join(data_dir, "schema_version")).should eq LavinMQ::Schema::VERSION.to_s
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

    it "refuses a schema_version that isn't a number" do
      with_datadir do |data_dir|
        File.write(File.join(data_dir, "schema_version"), "four")
        expect_raises(LavinMQ::UnsupportedSchemaVersion, /four/) do
          LavinMQ::Schema.verify(data_dir, nil)
        end
      end
    end
  end
end
