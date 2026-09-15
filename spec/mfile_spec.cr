require "spec"
require "../src/lavinmq/mfile"

class MFile
  getter synchronous_flushes = 0

  private def sync_mapping(flag) : Nil
    @synchronous_flushes += 1 if flag == LibC::MS_SYNC
    previous_def
  end

  # Pause just after the open check to exercise the former check/unmap race.
  property sync_checked : Channel(Nil)?
  property resume_sync : Channel(Nil)?

  private def check_open
    previous_def
    if checked = @sync_checked
      @sync_checked = nil
      checked.send(nil)
      @resume_sync.not_nil!.receive
    end
  end
end

describe MFile do
  it "syncs drained dirty pages before closing their mapping" do
    file = File.tempfile "mfile_spec"
    mfile = MFile.new(file.path, capacity: 4096)
    mfile.write("pending confirm".to_slice)
    mfile.mark_needs_msync!
    mfile.clear_needs_msync!
    mfile.close
    mfile.synchronous_flushes.should eq(1)
    File.read(file.path).should eq("pending confirm")
  ensure
    mfile.try &.close
    file.try &.delete
  end

  it "does not add a close barrier when syncing is disabled" do
    file = File.tempfile "mfile_spec"
    mfile = MFile.new(file.path, capacity: 4096)
    mfile.write("no sync".to_slice)
    mfile.mark_needs_msync!(sync_on_close: false)
    mfile.close
    mfile.synchronous_flushes.should eq(0)
  ensure
    mfile.try &.close
    file.try &.delete
  end

  it "updates its path when renamed across directories" do
    file = File.tempfile "mfile_spec"
    dir = File.tempname("mfile_dir_spec")
    Dir.mkdir(dir)
    mfile = MFile.new(file.path, capacity: 4096)
    mfile.rename(File.join(dir, "renamed"))
    mfile.path.should eq(File.join(dir, "renamed"))
    File.exists?(mfile.path).should be_true
    mfile.delete
  ensure
    mfile.try &.close
    File.delete?(file.path) if file
    Dir.delete(dir) if dir
  end

  it "does not expose deletion until the caller's bookkeeping completes" do
    file = File.tempfile "mfile_spec"
    mfile = MFile.new(file.path, capacity: 4096)
    started = Channel(Nil).new
    resume = Channel(Nil).new
    done = Channel(Nil).new
    spawn do
      mfile.delete do
        started.send(nil)
        resume.receive
      end
      done.send(nil)
    end
    started.receive
    begin
      File.exists?(file.path).should be_false
      mfile.deleted?.should be_false
    ensure
      resume.send(nil)
      done.receive
    end
    mfile.deleted?.should be_true
  ensure
    mfile.try &.close
    File.delete?(file.path) if file
  end

  {% for operation in [:close, :truncate] %}
    it "prevents {{ operation.id }} from unmapping during fsync" do
      file = File.tempfile "mfile_spec"
      mfile = MFile.new(file.path, capacity: 8192)
      mfile.write(Bytes.new(8192, 1))
      checked = Channel(Nil).new
      resume = Channel(Nil).new
      synced = Channel(Exception?).new
      unmapped = Channel(Exception?).new
      mfile.sync_checked = checked
      mfile.resume_sync = resume

      spawn do
        mfile.fsync
        synced.send(nil)
      rescue ex
        synced.send(ex)
      end
      checked.receive
      spawn do
        {% if operation == :close %}
          mfile.close
        {% else %}
          mfile.truncate(4096)
        {% end %}
        unmapped.send(nil)
      rescue ex
        unmapped.send(ex)
      end
      Fiber.yield
      begin
        mfile.closed?.should be_false
        mfile.capacity.should eq(8192)
      ensure
        resume.send(nil)
        synced.receive.should be_nil
        unmapped.receive.should be_nil
      end
    ensure
      mfile.try &.close
      file.try &.delete
    end
  {% end %}

  it "can be double closed" do
    file = File.tempfile "mfile_spec"
    file.sync = true
    begin
      file.puts "foobar" # can't mmap empty file
      mfile = MFile.new file.path
      mfile.close
      mfile.close
    ensure
      file.delete
    end
  end

  it "can be read" do
    file = File.tempfile "mfile_spec"
    file.print "hello world"
    file.flush
    begin
      MFile.open(file.path) do |mfile|
        buf = Bytes.new(6)
        cnt = mfile.read(buf)
        String.new(buf[0, cnt]).should eq "hello "
        cnt = mfile.read(buf)
        String.new(buf[0, cnt]).should eq "world"
      end
    ensure
      file.delete
    end
  end

  it "fsyncs written data" do
    file = File.tempfile "mfile_spec"
    begin
      mfile = MFile.new file.path, capacity: 1024
      mfile.write "hello world".to_slice
      mfile.fsync
      File.read(file.path)[0, 11].should eq "hello world"
      mfile.close
    ensure
      file.delete
    end
  end

  it "raises on fsync after close" do
    file = File.tempfile "mfile_spec"
    file.sync = true
    begin
      file.puts "foobar" # can't mmap empty file
      mfile = MFile.new file.path
      mfile.close
      expect_raises(IO::Error, "Closed mfile") { mfile.fsync }
    ensure
      file.delete
    end
  end

  it "dedupes needs-msync marking until cleared" do
    file = File.tempfile "mfile_spec"
    begin
      mfile = MFile.new file.path, capacity: 1024
      mfile.mark_needs_msync!.should be_false # was clear
      mfile.mark_needs_msync!.should be_true  # already marked
      mfile.clear_needs_msync!
      mfile.mark_needs_msync!.should be_false
      mfile.close
    ensure
      file.delete
    end
  end

  it "tracks mmap count" do
    file = File.tempfile "mfile_spec"
    file.print "test"
    file.flush
    begin
      count_before = MFile.mmap_count
      mfile = MFile.new file.path
      MFile.mmap_count.should eq(count_before + 1)
      mfile.close
      MFile.mmap_count.should eq(count_before)
    ensure
      file.delete
    end
  end
end

class MFile
  class_getter batch_delete_synced_dirs = [] of String
  class_property batch_sync_observer : Proc(Nil)?

  private def self.sync_deleted_directory(path : String) : Nil
    @@batch_delete_synced_dirs << path
    @@batch_sync_observer.try &.call
    previous_def
  end
end

describe "MFile batch deletion" do
  it "releases earlier mappings between bounded deletion batches" do
    with_datadir do |dir|
      files = (1..65).map { |i| MFile.new(File.join(dir, "segment.#{i}"), 4096) }
      remaining = [] of Int32
      MFile.batch_sync_observer = -> do
        remaining << files.count { |file| File.exists?(file.path) }
        # A later mapping must remain available to the shared confirm loop.
        files.last.fsync if File.exists?(files.last.path)
      end
      MFile.delete_all(files, needs_sync: true)
      remaining.should eq([33, 1, 0])
      files.each(&.deleted?.should(be_true))
    ensure
      MFile.batch_sync_observer = nil
      files.try &.each &.close
    end
  end

  it "syncs each parent once for a batch of deleted files" do
    with_datadir do |dir|
      files = (1..8).map { |i| MFile.new(File.join(dir, "segment.#{i}"), 4096) }
      MFile.batch_delete_synced_dirs.clear
      MFile.delete_all(files, needs_sync: true)
      MFile.batch_delete_synced_dirs.should eq [dir]
      files.each do |file|
        file.deleted?.should be_true
        File.exists?(file.path).should be_false
      end
    ensure
      files.try &.each &.close
    end
  end

  it "does not sync batch deletions when syncing is not requested" do
    with_datadir do |dir|
      file = MFile.new(File.join(dir, "segment"), 4096)
      MFile.batch_delete_synced_dirs.clear
      MFile.delete_all([file])
      MFile.batch_delete_synced_dirs.should be_empty
      file.deleted?.should be_true
    ensure
      file.try &.close
    end
  end
end
