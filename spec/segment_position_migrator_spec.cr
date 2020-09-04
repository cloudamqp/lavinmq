require "./spec_helper"
require "../src/avalanchemq/segment_position_migrator"
require "../src/avalanchemq/segment_position"

describe AvalancheMQ::SegmentPositionMigrator do
  sp_version_file = AvalancheMQ::SegmentPositionMigrator::VERSION_FILE
  log = Logger.new(STDERR)
  path = "/tmp/spec"
  segment_file = File.join(path, "test_segments")
  subject = AvalancheMQ::SegmentPositionMigrator
  it "should exist" do
    sp_migrator = subject.new(path, log)
    sp_migrator.should_not be nil
  end

  describe "without existing data folder" do
    segment_file = File.join(path, "test_segments")
    it "should not migrate anything" do
      path = "/tmp/sp_spec"
      sp_migrator = subject.new(path, log).run(segment_file)
      sp_migrator.should be nil
    ensure
      FileUtils.rm_rf("/tmp/sp_spec")
    end

    it "should write current segment position version to disk" do
      path = "/tmp/sp_spec"
      sp_migrator = subject.new(path, log)
      sp_migrator.run(segment_file)
      sp_migrator.current_version.should eq AvalancheMQ::SegmentPosition::VERSION
    ensure
      FileUtils.rm_rf("/tmp/sp_spec")
    end
  end

  describe "with existing data folder" do
    path = "/tmp/spec"
    version_file = File.join(path, sp_version_file)
    it "should update segment position version" do
      sp_migrator = subject.new(path, log)
      sp_migrator.current_version.should eq AvalancheMQ::SegmentPosition::VERSION
    ensure
      FileUtils.rm(version_file)
    end

    describe "segment file migration" do
      format = IO::ByteFormat::SystemEndian

      it "should upgrade from version 0 to 1" do
        write_version(path, 0_u32)
        data = [0_u32, 1_u32, 2_u32, 3_u32]
        write_segment_file(segment_file, data, format)
        sp_migrator = subject.new(path, log, format)
        sp_migrator.run(segment_file)
        expected = [0_u32, 1_u32, 0_u64, 2_u32, 3_u32, 0_u64]
        assert_segment_file(segment_file, expected, format)
      ensure
        FileUtils.rm(segment_file)
      end

      it "should downgrade from version 2 to 1" do
        write_version(path, 2_u32)
        formats = {
          2 => [0_u32, 0_u32, 0_u64],
          1 => [0_u32, 0_u32],
        }
        data = [0_u32, 1_u32, 2_u64, 3_u32, 4_u32, 5_u64]
        write_segment_file(segment_file, data, format)
        sp_migrator = subject.new(path, log, format, formats)
        sp_migrator.run(segment_file)
        expected = [0_u32, 1_u32, 3_u32, 4_u32]
        assert_segment_file(segment_file, expected, format)
      ensure
        FileUtils.rm(segment_file)
      end
    end
  end
end

def write_version(data_dir, version)
  path = File.join(data_dir, AvalancheMQ::SegmentPositionMigrator::VERSION_FILE)
  File.write(path, version)
end

def write_segment_file(segment_file, data, format)
  File.open(segment_file, "w") do |io|
    data.each do |d|
      io.write_bytes(d, format)
    end
    io.fsync
  end
end

def assert_segment_file(segment_file, expected, format)
  File.open(segment_file) do |io|
    expected.each do |d|
      d.class.from_io(io, format).should eq d
    end
  end
end
