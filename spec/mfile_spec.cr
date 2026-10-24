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

  describe "leases" do
    it "unmaps on close only when the last lease is released" do
      path = File.tempname("mfile_spec")
      begin
        mfile = MFile.new(path, 4096)
        mfile.write "hello".to_slice
        slice = mfile.to_slice
        mfile.lease
        mfile.lease
        mfile.close
        mfile.closed?.should be_false
        # Truncated right away, the deferred unmap won't truncate
        File.size(path).should eq 5
        String.new(slice).should eq "hello"
        mfile.release_lease
        mfile.closed?.should be_false
        mfile.release_lease
        mfile.closed?.should be_true
      ensure
        File.delete?(path)
      end
    end

    it "doesn't truncate when the last lease is released" do
      path = File.tempname("mfile_spec")
      begin
        mfile = MFile.new(path, 4096)
        mfile.write "hello".to_slice
        mfile.lease
        mfile.close
        # Opened again and written to before the lease is released
        File.open(path, "a", &.print(" world"))
        mfile.release_lease
        mfile.closed?.should be_true
        File.read(path).should eq "hello world"
      ensure
        File.delete?(path)
      end
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

  describe "madvise chunking" do
    # madvise(MADV_DONTNEED) over PMD_SIZE or more lets the kernel reclaim the
    # page table and flush at the wrong address on Linux 7.0.0-rc1 through
    # 7.1.8 (CVE-2026-74674). The kernel check is `>=`, so exactly PMD_SIZE
    # triggers it too: measured on arm64, 2 MiB chunks storm, 1 MiB chunks do
    # not.
    it "derives PMD_SIZE from the running page size" do
      page_size = LibC.sysconf(LibC::SC_PAGESIZE)
      MFile::PMD_SIZE.should eq(page_size.to_i64 * (page_size // 8))
    end

    it "keeps the DontNeed chunk strictly below PMD_SIZE" do
      MFile::DONTNEED_CHUNK_SIZE.should be < MFile::PMD_SIZE
    end

    it "leaves at most one page of headroom, so the chunk is as large as it can be" do
      page_size = LibC.sysconf(LibC::SC_PAGESIZE)
      (MFile::PMD_SIZE - MFile::DONTNEED_CHUNK_SIZE).should eq page_size
    end
  end

  it "keeps its contents readable after dontneed over several chunks" do
    file = File.tempfile "mfile_spec"
    capacity = MFile::DONTNEED_CHUNK_SIZE * 2 + 4096
    begin
      MFile.open(file.path, capacity) do |mfile|
        mfile.write "hello world".to_slice
        mfile.dontneed
        mfile.pos = 0
        buf = Bytes.new(11)
        mfile.read(buf)
        String.new(buf).should eq "hello world"
      end
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

  describe "sync bookkeeping" do
    it "reports whether it created the file, once" do
      path = File.tempname("mfile_spec")
      begin
        mfile = MFile.new(path, 4096)
        mfile.take_created!.should be_true
        mfile.take_created!.should be_false
        mfile.close
        reopened = MFile.new(path, 4096)
        reopened.take_created!.should be_false
        reopened.close
      ensure
        File.delete?(path)
      end
    end

    it "dedupes the needs msync mark until cleared" do
      path = File.tempname("mfile_spec")
      begin
        MFile.open(path, 4096) do |mfile|
          mfile.mark_needs_msync!.should be_false
          mfile.mark_needs_msync!.should be_true
          mfile.clear_needs_msync!
          mfile.mark_needs_msync!.should be_false
        end
      ensure
        File.delete?(path)
      end
    end

    it "fsyncs written data, also after being closed" do
      path = File.tempname("mfile_spec")
      begin
        mfile = MFile.new(path, 4096)
        mfile.write "hello".to_slice
        mfile.fsync
        mfile.close
        mfile.fsync
        File.read(path).should eq "hello"
      ensure
        File.delete?(path)
      end
    end

    it "skips the fsync of a deleted file" do
      path = File.tempname("mfile_spec")
      mfile = MFile.new(path, 4096)
      mfile.write "hello".to_slice
      mfile.delete
      mfile.close
      mfile.fsync
    end
  end
end
