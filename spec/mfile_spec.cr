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
end
