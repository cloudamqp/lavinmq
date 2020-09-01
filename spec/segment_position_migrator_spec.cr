require "./spec_helper"
require "../src/avalanchemq/segment_position_migrator"
require "../src/avalanchemq/segment_position"

describe AvalancheMQ::SegmentPositionMigrator do
  segment_version_file_name = "segment_position_version.txt"
  log = Logger.new(STDERR)
  path = "/tmp/spec"
  segment_file = File.join(path, "test_segments")
  subject = AvalancheMQ::SegmentPositionMigrator
  it "should exist" do
    sp_migrator = subject.new(path, log)
    sp_migrator.should_not be nil
  end

  describe "with non-existing folder" do
    segment_file = File.join(path, "test_segments")
    it "should not migrate anything" do
      path = "/tmp/sp_spec"
      sp_migrator = subject.new(path, log).run(segment_file)
      sp_migrator.should be nil
    ensure
      FileUtils.rm_rf(path)
    end

    it "should write current segment position version to disk" do
      path = "/tmp/sp_spec"
      sp_migrator = subject.new(path, log)
      sp_migrator.run(segment_file)
      sp_migrator.read_version_from_disk.should eq AvalancheMQ::SegmentPosition::VERSION
    ensure
      FileUtils.rm_rf(path)
    end
  end

  describe "with existing folder" do
    path = "/tmp/spec"
    segment_file = File.join(path, "test_segments")
    version_file = File.join(path, segment_version_file_name)
    File.write(version_file, 0)
    it "should update segment position version" do
      sp_migrator = subject.new(path, log)
      sp_migrator.run(segment_file)
      sp_migrator.read_version_from_disk.should eq AvalancheMQ::SegmentPosition::VERSION
    ensure
      FileUtils.rm_rf(version_file)
    end

    describe "migrate" do
      format = IO::ByteFormat::SystemEndian
      formats = {
        2 => [
          0_u32,
          0_u32,
          0_u64,
        ],
        0 =>
        [
          0_u32,
          0_u32,
        ]
      }
      version_to_migrate_to = 2
      current_version = 0_u32
      it "should migrate from ver #{current_version} to ver #{version_to_migrate_to}" do
        File.open(segment_file, "w") do |io|
          io.write_bytes(0_u32, format)
          io.write_bytes(1_u32, format)
          io.write_bytes(2_u32, format)
          io.write_bytes(3_u32, format)
          io.fsync
        end
        sp_migrator = subject.new(path, log, format, current_version)
        sp_migrator.convert_sp(version_to_migrate_to, formats, segment_file)
        File.open(segment_file) do |io|
          UInt32.from_io(io, format).should eq 0_u32
          UInt32.from_io(io, format).should eq 1_u32
          UInt64.from_io(io, format).should eq 0_u64
          UInt32.from_io(io, format).should eq 2_u32
          UInt32.from_io(io, format).should eq 3_u32
          UInt64.from_io(io, format).should eq 0_u64
        end
      end
    ensure
      FileUtils.rm_rf(segment_file)
    end
  end
end
