require "./spec_helper"

private class RecordingPersister < LavinMQ::Persister
  getter msynced = Array(MFile).new
  getter syncfs_count = 0

  def sync_files_public(files : Array(MFile)) : Nil
    sync_files(files)
  end

  def data_dir_fd_public : Int32
    @data_dir_fd
  end

  protected def sync_file(file : MFile) : Nil
    @msynced << file
  end

  protected def syncfs : Nil
    @syncfs_count += 1
  end
end

describe LavinMQ::Persister do
  {% if flag?(:linux) %}
    it "keeps the data directory descriptor open for its lifetime" do
      data_dir = Dir.tempdir
      persister = RecordingPersister.new(data_dir: data_dir)
      fd = persister.data_dir_fd_public

      fd.should be >= 0
      File.realpath("/proc/self/fd/#{fd}").should eq File.realpath(data_dir)
      Fiber.yield
      persister.data_dir_fd_public.should eq fd
    ensure
      persister.try &.close
      FileUtils.rm_rf(data_dir) if data_dir
    end
  {% end %}

  it "msyncs batches below the syncfs threshold" do
    LavinMQ::Config.instance.syncfs_threshold = 3
    persister = RecordingPersister.new
    files = Array.new(2) { MFile.new(File.tempname("persister"), 4096) }

    persister.sync_files_public(files)

    persister.msynced.should eq files
    persister.syncfs_count.should eq 0
  ensure
    persister.try &.close
    files.try &.each do |file|
      file.close
      File.delete?(file.path)
    end
    LavinMQ::Config.instance.syncfs_threshold = 10
  end

  it "uses syncfs when the batch reaches the threshold" do
    LavinMQ::Config.instance.syncfs_threshold = 3
    persister = RecordingPersister.new
    files = Array.new(3) { MFile.new(File.tempname("persister"), 4096) }

    persister.sync_files_public(files)

    persister.msynced.should be_empty
    persister.syncfs_count.should eq 1
  ensure
    persister.try &.close
    files.try &.each do |file|
      file.close
      File.delete?(file.path)
    end
    LavinMQ::Config.instance.syncfs_threshold = 10
  end

  it "does not count closed or deleted files toward the threshold" do
    LavinMQ::Config.instance.syncfs_threshold = 2
    persister = RecordingPersister.new
    live = MFile.new(File.tempname("persister"), 4096)
    deleted = MFile.new(File.tempname("persister"), 4096)
    deleted.delete

    persister.sync_files_public([live, deleted])

    persister.msynced.should eq [live]
    persister.syncfs_count.should eq 0
  ensure
    persister.try &.close
    live.try &.close
    File.delete?(live.path) if live
    deleted.try &.close
    File.delete?(deleted.path) if deleted
    LavinMQ::Config.instance.syncfs_threshold = 10
  end
end
