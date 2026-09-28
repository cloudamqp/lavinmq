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

  describe ".each_dontneed_chunk" do
    # Chunks must stay under PMD_SIZE, 2 MiB on a 4K-page kernel. At or above it
    # the kernel reclaims the page table and flushes at the wrong address on
    # Linux 7.0 through 7.1.8 (CVE-2026-74674).
    pmd_size = 2i64 * 1024 * 1024

    it "splits a large capacity into chunks below the reclaim threshold" do
      chunks = [] of Tuple(Int64, Int64)
      MFile.each_dontneed_chunk(8i64 * 1024 * 1024) { |offset, length| chunks << {offset, length} }

      chunks.size.should eq 8
      chunks.each { |(_, length)| length.should be < pmd_size }
      chunks.first.should eq({0i64, 1i64 * 1024 * 1024})
      chunks.sum { |(_, length)| length }.should eq 8i64 * 1024 * 1024
    end

    it "covers a capacity that is not a multiple of the chunk size" do
      chunks = [] of Tuple(Int64, Int64)
      capacity = 1i64 * 1024 * 1024 + 4096
      MFile.each_dontneed_chunk(capacity) { |offset, length| chunks << {offset, length} }

      chunks.should eq [{0i64, 1i64 * 1024 * 1024}, {1i64 * 1024 * 1024, 4096i64}]
    end

    it "yields a single chunk when the capacity is already small enough" do
      chunks = [] of Tuple(Int64, Int64)
      MFile.each_dontneed_chunk(512i64 * 1024) { |offset, length| chunks << {offset, length} }

      chunks.should eq [{0i64, 512i64 * 1024}]
    end

    it "yields nothing for an empty range" do
      called = false
      MFile.each_dontneed_chunk(0i64) { |_, _| called = true }
      called.should be_false
    end
  end

  it "keeps its contents readable after dontneed" do
    file = File.tempfile "mfile_spec"
    begin
      MFile.open(file.path, 4i64 * 1024 * 1024) do |mfile|
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
