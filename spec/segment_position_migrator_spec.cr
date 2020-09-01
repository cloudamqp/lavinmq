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
      FileUtils.rm_rf("/tmp/sp_spec")
    end

    it "should write current segment position version to disk" do
      path = "/tmp/sp_spec"
      sp_migrator = subject.new(path, log)
      sp_migrator.run(segment_file)
      sp_migrator.read_version_from_disk.should eq AvalancheMQ::SegmentPosition::VERSION
    ensure
      FileUtils.rm_rf("/tmp/sp_spec")
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
      FileUtils.rm(version_file)
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

      it "should migrate from ver 0 to ver 2" do
        data = [0_u32, 1_u32, 2_u32, 3_u32]
        write_segment_file(segment_file, data, format)
        sp_migrator = subject.new(path, log, format, 0_u32)
        sp_migrator.convert_sp(2_u32, formats, segment_file)
        expected = [0_u32, 1_u32, 0_u64, 2_u32, 3_u32, 0_u64]
        assert_segment_file(segment_file, expected, format)
      ensure
        FileUtils.rm(segment_file)
      end

      it "should downgrade from ver 2 to ver 0" do
        data = [0_u32, 1_u32, 2_u64, 3_u32, 4_u32, 5_u64]
        write_segment_file(segment_file, data, format)
        sp_migrator = subject.new(path, log, format, 2_u32)
        sp_migrator.convert_sp(0_u32, formats, segment_file)
        expected = [0_u32, 1_u32, 3_u32, 4_u32]
        assert_segment_file(segment_file, expected, format)
      ensure
        FileUtils.rm(segment_file)
      end
    end
  end
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
