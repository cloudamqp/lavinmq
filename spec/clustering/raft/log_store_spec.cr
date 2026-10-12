require "spec"
require "file_utils"
require "../../../src/lavinmq/clustering/raft/journal"

private alias Raft = LavinMQ::Clustering::Raft

private def entry(term : Int64, data : String? = nil) : Raft::Entry
  Raft::Entry.new(term, nil, nil, data.try(&.to_slice))
end

describe Raft::MemoryLogStore do
  it "is empty after the snapshot" do
    log = Raft::MemoryLogStore.new(5, 2)
    log.empty?.should be_true
    log.last_index.should eq 5
    log.term_at(5).should eq 2
    log.term_at(4).should eq 0
    log.term_at(6).should eq 0
    log.slice(1).should be_empty
    log.entries.should be_empty
  end

  it "indexes entries from after the snapshot" do
    log = Raft::MemoryLogStore.new(5, 2, [entry(2), entry(3, "a")])
    log.last_index.should eq 7
    log.entry(6).should eq entry(2)
    log.entry(7).data.should eq "a".to_slice
    log.term_at(7).should eq 3
    log.slice(6, max: 1).should eq [entry(2)]
    log.slice(1).should eq [entry(2), entry(3, "a")]
    log.slice(8).should be_empty
    terms = [] of Int64
    log.reverse_each { |e| terms << e.term }
    terms.should eq [3, 2]
  end

  it "truncates from an index on" do
    log = Raft::MemoryLogStore.new(0, 0, [entry(1), entry(1), entry(2)])
    log.truncate_from(4) # past the end
    log.last_index.should eq 3
    log.truncate_from(2)
    log.entries.should eq [entry(1)]
    log.truncate_from(1)
    log.empty?.should be_true
    log.last_index.should eq 0
  end

  it "doesn't truncate what's compacted" do
    log = Raft::MemoryLogStore.new(3, 1, [entry(1)])
    expect_raises(ArgumentError) { log.truncate_from(3) }
  end

  it "compacts up to an index" do
    log = Raft::MemoryLogStore.new(0, 0, [entry(1), entry(2), entry(2)])
    log.compact_to(2, 2)
    log.snapshot_index.should eq 2
    log.term_at(2).should eq 2
    log.entries.should eq [entry(2)]
    log.compact_to(1, 1) # already compacted
    log.snapshot_index.should eq 2
    log.compact_to(9, 4) # beyond the last entry
    log.empty?.should be_true
    log.last_index.should eq 9
    log.term_at(9).should eq 4
  end

  it "starts over after a snapshot" do
    log = Raft::MemoryLogStore.new(0, 0, [entry(1), entry(1), entry(1)])
    log.reset(2, 3)
    log.empty?.should be_true
    log.last_index.should eq 2
    log.term_at(2).should eq 3
    log.append entry(3)
    log.last_index.should eq 3
  end
end

private def with_journal_dir(&)
  dir = File.tempname("raft-journal-spec")
  Dir.mkdir_p dir
  yield dir
ensure
  FileUtils.rm_rf dir if dir
end

private def journal_files(dir : String) : Array(String)
  Dir.children(dir).select! { |c| Raft::JournalLogStore.generation_file?(c) }.sort!
end

describe Raft::JournalLogStore do
  it "is read back after a sync" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      log.generation.should eq 1
      log.append entry(1)
      log.append entry(1, "ops")
      log.pending?.should be_true
      log.sync
      log.pending?.should be_false
      log.close
      reopened = Raft::JournalLogStore.new(dir, 1, 0, 0)
      reopened.entries.should eq [entry(1), entry(1, "ops")]
      reopened.close
    end
  end

  it "doesn't write before a sync" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      log.append entry(1)
      journal_files(dir).should be_empty
      log.sync
      size = File.size(File.join(dir, ".raft_log.1"))
      log.append entry(2)
      File.size(File.join(dir, ".raft_log.1")).should eq size
      log.close
    end
  end

  it "replays truncations" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      3.times { log.append entry(1) }
      log.sync
      log.truncate_from(2)
      log.append entry(2, "new")
      log.sync
      log.close
      reopened = Raft::JournalLogStore.new(dir, 1, 0, 0)
      reopened.entries.should eq [entry(1), entry(2, "new")]
      reopened.close
    end
  end

  it "skips what the snapshot has" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      3.times { |i| log.append entry(1, i.to_s) }
      log.sync
      log.compact_to(2, 1)
      log.close
      reopened = Raft::JournalLogStore.new(dir, 1, 2, 1)
      reopened.last_index.should eq 3
      reopened.entries.should eq [entry(1, "2")]
      reopened.close
    end
  end

  it "takes the entries of an older state file as its first generation" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 5, 2, [entry(2), entry(3)])
      log.last_index.should eq 7
      log.sync
      log.close
      reopened = Raft::JournalLogStore.new(dir, 1, 5, 2)
      reopened.entries.should eq [entry(2), entry(3)]
      reopened.close
    end
  end

  it "cuts off a record cut short, and continues after it" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      log.append entry(1, "a")
      log.append entry(1, "b")
      log.sync
      log.close
      path = File.join(dir, ".raft_log.1")
      bytes = File.read(path).to_slice
      File.write(path, bytes[0, bytes.size - 3])
      reopened = Raft::JournalLogStore.new(dir, 1, 0, 0)
      reopened.entries.should eq [entry(1, "a")]
      reopened.append entry(2, "c")
      reopened.sync
      reopened.close
      Raft::JournalLogStore.new(dir, 1, 0, 0).tap do |again|
        again.entries.should eq [entry(1, "a"), entry(2, "c")]
        again.close
      end
    end
  end

  it "cuts off a record that fails its checksum, or claims to be too large" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      log.append entry(1, "a")
      log.sync
      first = File.size(File.join(dir, ".raft_log.1"))
      log.append entry(1, "b")
      log.sync
      log.close
      path = File.join(dir, ".raft_log.1")
      bytes = File.read(path).to_slice.dup
      bytes[bytes.size - 1] ^= 0xff
      File.write(path, bytes)
      Raft::JournalLogStore.new(dir, 1, 0, 0).tap do |reopened|
        reopened.entries.should eq [entry(1, "a")]
        reopened.close
      end
      File.size(path).should eq first

      oversized = Bytes.new(8)
      IO::ByteFormat::LittleEndian.encode(UInt32::MAX, oversized[0, 4])
      File.open(path, "a", &.write(oversized))
      Raft::JournalLogStore.new(dir, 1, 0, 0).tap do |reopened|
        reopened.entries.should eq [entry(1, "a")]
        reopened.close
      end
      File.size(path).should eq first
    end
  end

  it "starts a new generation when a snapshot is installed, dropping the old one only when told" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      2.times { log.append entry(1) }
      log.sync
      log.reset(10, 3)
      log.append entry(3, "x")
      log.generation.should eq 2
      log.sync
      journal_files(dir).should eq [".raft_log.1", ".raft_log.2"]
      # A crash now: the state file still names generation 1, which is whole
      with_journal_dir do |crashed|
        journal_files(dir).each { |f| FileUtils.cp(File.join(dir, f), crashed) }
        Raft::JournalLogStore.new(crashed, 1, 0, 0).tap do |old|
          old.entries.should eq [entry(1), entry(1)]
          old.close
        end
      end
      log.drop_old_generations
      journal_files(dir).should eq [".raft_log.2"]
      log.close
      Raft::JournalLogStore.new(dir, 2, 10, 3).tap do |reopened|
        reopened.entries.should eq [entry(3, "x")]
        reopened.close
      end
    end
  end

  it "deletes the generations the state file doesn't name" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      log.append entry(1)
      log.sync
      log.reset(5, 2)
      log.sync # a crash before the state file named generation 2
      log.close
      Raft::JournalLogStore.new(dir, 1, 0, 0).close
      journal_files(dir).should eq [".raft_log.1"]
    end
  end

  it "rotates to a new generation once most of it is compacted" do
    with_journal_dir do |dir|
      log = Raft::JournalLogStore.new(dir, 0, 0, 0)
      data = "x" * 64 * 1024
      20.times { log.append entry(1, data) }
      log.sync
      log.compact_to(19, 1)
      log.generation.should eq 2
      log.sync
      log.drop_old_generations
      journal_files(dir).should eq [".raft_log.2"]
      File.size(File.join(dir, ".raft_log.2")).should be < 2 * data.bytesize
      log.close
      Raft::JournalLogStore.new(dir, 2, 19, 1).tap do |reopened|
        reopened.entries.should eq [entry(1, data)]
        reopened.close
      end
    end
  end
end
