require "spec"
require "../src/lavinmq/mfile"

describe MFile do
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
