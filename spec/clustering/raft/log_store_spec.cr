require "spec"
require "../../../src/lavinmq/clustering/raft/log_store"

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
