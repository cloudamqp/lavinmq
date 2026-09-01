require "./spec_helper"
require "../src/stdlib/filesystem"
require "../src/lavinmq/filesystem"

describe FilesystemInfo do
  # Real statfs64 against the OS; only diverges from the bug on virtiofs/NFS
  # mounts. The deterministic specs below guard the regression everywhere.
  it "matches df for the current filesystem" do
    path = Dir.current
    df = `df -P -k #{path}`.lines.last.split
    df_total = df[1].to_u64 * 1024
    df_available = df[3].to_u64 * 1024

    info = Filesystem.info(path)
    info.total.should eq df_total
    # free space can change between the two measurements
    info.available.should be_close(df_available, df_total // 100)
  end

  {% if flag?(:linux) %}
    # Block counts are in f_frsize units; bsize is the (larger) transfer size.
    it "scales block counts by f_frsize, not f_bsize" do
      statfs = uninitialized LibC::Statfs
      statfs.blocks = 1000_u64
      statfs.bavail = 400_u64
      statfs.frsize = 4096_i64
      statfs.bsize = 1048576_i64

      info = FilesystemInfo.new(statfs)
      info.total.should eq 1000_u64 * 4096
      info.available.should eq 400_u64 * 4096
    end

    it "falls back to f_bsize when f_frsize is zero" do
      statfs = uninitialized LibC::Statfs
      statfs.blocks = 1000_u64
      statfs.bavail = 400_u64
      statfs.frsize = 0_i64
      statfs.bsize = 4096_i64

      info = FilesystemInfo.new(statfs)
      info.total.should eq 1000_u64 * 4096
      info.available.should eq 400_u64 * 4096
    end
  {% end %}
end

describe LavinMQ::FileSystem do
  describe ".durable_rename" do
    it "atomically replaces a file in the same directory" do
      with_datadir do |data_dir|
        source = File.join(data_dir, "state.tmp")
        destination = File.join(data_dir, "state.json")
        File.open(source, "w") { |file| file.print "new state"; file.fsync }
        File.write(destination, "old state")

        LavinMQ::FileSystem.durable_rename(source, destination)

        File.read(destination).should eq "new state"
        File.exists?(source).should be_false
      end
    end

    it "renames between directories" do
      with_datadir do |data_dir|
        source_dir = File.join(data_dir, "source")
        destination_dir = File.join(data_dir, "destination")
        Dir.mkdir(source_dir)
        Dir.mkdir(destination_dir)
        source = File.join(source_dir, "state.tmp")
        destination = File.join(destination_dir, "state.json")
        File.open(source, "w") { |file| file.print "state"; file.fsync }

        LavinMQ::FileSystem.durable_rename(source, destination)

        File.read(destination).should eq "state"
        File.exists?(source).should be_false
      end
    end

    it "updates an open file's path" do
      with_datadir do |data_dir|
        source = File.join(data_dir, "state.tmp")
        destination = File.join(data_dir, "state.json")

        File.open(source, "w+") do |file|
          file.print "state"
          file.fsync
          LavinMQ::FileSystem.durable_rename(file, destination)

          file.path.should eq destination
        end
        File.read(destination).should eq "state"
      end
    end
  end
end
