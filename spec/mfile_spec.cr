require "spec"
require "../src/lavinmq/mfile"

class MFile
  # Pause just after the open check to exercise the former check/unmap race.
  property sync_checked : Channel(Nil)?
  property resume_sync : Channel(Nil)?
  property directory_sync_started : Channel(Nil)?
  property resume_directory_sync : Channel(Nil)?

  private def fsync_parent_dir : Nil
    @directory_sync_started.try &.send(nil)
    @resume_directory_sync.try &.receive
    previous_def
  end

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
  it "updates its retained directory when renamed across directories" do
    file = File.tempfile "mfile_spec"
    dir = File.tempname("mfile_dir_spec")
    Dir.mkdir(dir)
    mfile = MFile.new(file.path, capacity: 4096)
    original_directory = mfile.@directory
    mfile.rename(File.join(dir, "renamed"))
    original_directory.@file.closed?.should be_true
    mfile.@directory.@file.path.should eq(dir)
    mfile.delete(durable: true)
  ensure
    mfile.try &.close
    File.delete?(file.path) if file
    Dir.delete(dir) if dir
  end

  it "does not expose a durable deletion until the directory is synced" do
    file = File.tempfile "mfile_spec"
    mfile = MFile.new(file.path, capacity: 4096)
    started = Channel(Nil).new
    resume = Channel(Nil).new
    done = Channel(Nil).new
    mfile.directory_sync_started = started
    mfile.resume_directory_sync = resume
    spawn do
      mfile.delete(durable: true)
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

  it "persists the parent directory on the first synchronous flush" do
    file = File.tempfile "mfile_spec"
    mfile = MFile.new(file.path, capacity: 4096)
    mfile.write "message".to_slice
    mfile.flush
    mfile.@directory_synced.should be_false
    mfile.fsync
    mfile.@directory_synced.should be_true
  ensure
    mfile.try &.close
    file.try &.delete
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
