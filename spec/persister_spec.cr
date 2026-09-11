require "./spec_helper"

private class RecordingPersister < LavinMQ::Persister
  getter msynced = Array(MFile).new
  getter syncfs_count = 0
  property sync_started : Channel(Nil)?
  property resume_sync : Channel(Nil)?

  def pending_sync_waiters : Int32
    @sync_waiters.lock(&.size)
  end

  def sync_files_public(files : Array(MFile)) : Nil
    sync_files(files)
  end

  def data_dir_fd_public : Int32
    @data_dir_fd
  end

  protected def sync_file(file : MFile) : Nil
    @sync_started.try &.send(nil)
    @resume_sync.try &.receive
    @msynced << file
  end

  protected def syncfs : Nil
    @syncfs_count += 1
  end
end

describe LavinMQ::Persister do
  it "releases sync waiters only after their own batch completes" do
    with_datadir do |data_dir|
      persister = RecordingPersister.new(data_dir: data_dir)
      started = Channel(Nil).new(2)
      resume = Channel(Nil).new(2)
      persister.sync_started = started
      persister.resume_sync = resume
      file = MFile.new(File.join(data_dir, "segment"), 4096)
      first_done = Channel(Nil).new(1)
      second_done = Channel(Nil).new(1)

      persister.mark_dirty(file)
      spawn { persister.sync; first_done.send(nil) }
      started.receive
      persister.mark_dirty(file)
      spawn { persister.sync; second_done.send(nil) }
      wait_for { persister.pending_sync_waiters == 1 }
      resume.send(nil)
      started.receive
      first_done.receive
      select
      when second_done.receive
        fail "second sync returned before its batch completed"
      when timeout(20.milliseconds)
      end
      resume.send(nil)
      second_done.receive
    ensure
      # Release either paused batch if an assertion failed.
      2.times { resume.try &.try_send(nil) }
      persister.try &.close
      file.try &.close
    end
  end

  it "does not sync an empty transaction" do
    with_datadir do |data_dir|
      persister = RecordingPersister.new(data_dir: data_dir)
      persister.sync
      persister.syncfs_count.should eq(0)
      persister.msynced.should be_empty
    ensure
      persister.try &.close
    end
  end

  {% if flag?(:linux) %}
    it "keeps the data directory descriptor open for its lifetime" do
      with_datadir do |data_dir|
        data_dir.should_not eq(Dir.tempdir)
        persister = RecordingPersister.new(data_dir: data_dir)
        fd = persister.data_dir_fd_public

        fd.should be >= 0
        File.realpath("/proc/self/fd/#{fd}").should eq File.realpath(data_dir)
        Fiber.yield
        persister.data_dir_fd_public.should eq fd
      ensure
        persister.try &.close
      end
    end

    it "syncs inline with syncfs after close without losing the descriptor" do
      LavinMQ::Config.instance.syncfs_threshold = 1
      with_datadir do |data_dir|
        persister = RecordingPersister.new(data_dir: data_dir)
        file = MFile.new(File.join(data_dir, "segment"), 4096)
        persister.close
        sleep 10.milliseconds # let the confirm loop thread exit
        persister.mark_dirty(file)
        persister.sync
        persister.syncfs_count.should eq 1
        persister.data_dir_fd_public.should be >= 0
      ensure
        file.try &.close
      end
    ensure
      LavinMQ::Config.instance.syncfs_threshold = 10
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
