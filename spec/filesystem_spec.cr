require "spec"
require "../src/stdlib/filesystem"

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
