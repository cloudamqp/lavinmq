require "../spec_helper"
require "../../src/lavinmq/clustering/journal"

# A sender stand-in: records wakeups and holds a journal position
private class FakeSubscriber
  include LavinMQ::Clustering::Journal::Subscriber

  getter wakeups = 0
  @position = Atomic(UInt64).new(0)

  def wake : Nil
    @wakeups += 1
  end

  def journal_position : UInt64
    @position.get
  end

  def journal_position=(lsn : UInt64)
    @position.set(lsn)
  end
end

describe LavinMQ::Clustering::Journal do
  it "assigns contiguous lsns starting at 1" do
    journal = LavinMQ::Clustering::Journal.new(8)
    journal.subscribe(FakeSubscriber.new) # entries are only readable while someone tails them
    journal.last_lsn.should eq 0
    journal.delete("a").should eq 1
    journal.append_bytes("b", "x".to_slice, 0i64).should eq 2
    journal.fsync("").should eq 3
    journal.last_lsn.should eq 3
    journal.head.should eq 4
    journal.read(1).not_nil!.op.delete?.should be_true
    journal.read(2).not_nil!.payload.should eq "x".to_slice
    journal.read(3).not_nil!.path.should eq ""
    journal.read(4).should be_nil # not appended yet
  end

  it "evicts the oldest entries when full, so a reader there is behind" do
    journal = LavinMQ::Clustering::Journal.new(4)
    sub = FakeSubscriber.new
    journal.subscribe(sub)
    6.times { |i| journal.delete("f#{i}") } # lsns 1..6, capacity 4
    journal.tail.should eq 3
    journal.read(2).should be_nil
    journal.behind?(2).should be_true
    journal.behind?(3).should be_false
    journal.read(3).not_nil!.path.should eq "f2"
    journal.read(6).not_nil!.path.should eq "f5"
  end

  it "wakes subscribers on append and starts them at the head" do
    journal = LavinMQ::Clustering::Journal.new(8)
    journal.delete("before")
    sub = FakeSubscriber.new
    journal.subscribe(sub)
    sub.journal_position.should eq journal.head
    journal.delete("after")
    sub.wakeups.should eq 1
    journal.unsubscribe(sub)
    journal.delete("later")
    sub.wakeups.should eq 1
  end

  describe "mfile leases" do
    it "keeps a closed segment mapped until every subscriber has passed its entries" do
      with_datadir do |data_dir|
        mfile = MFile.new(File.join(data_dir, "seg"), 64)
        mfile.print "hello"
        journal = LavinMQ::Clustering::Journal.new(8)
        sub = FakeSubscriber.new
        journal.subscribe(sub)
        lsn = journal.append_mfile("seg", mfile, 0i64, 5i64)

        mfile.close # the writer rolled over; the journal's lease defers the unmap
        mfile.closed?.should be_false
        entry = journal.read(lsn).not_nil!
        entry.payload.should eq "hello".to_slice
        mfile.release_lease # the reader's own lease

        sub.journal_position = lsn + 1
        journal.release_unneeded
        mfile.closed?.should be_true
      end
    end

    it "holds no lease without subscribers" do
      with_datadir do |data_dir|
        mfile = MFile.new(File.join(data_dir, "seg"), 64)
        mfile.print "hello"
        journal = LavinMQ::Clustering::Journal.new(8)
        lsn = journal.append_mfile("seg", mfile, 0i64, 5i64)
        # Never leased: a reader gets the entry without the mapping
        journal.read(lsn).not_nil!.mfile.should be_nil
        mfile.close
        mfile.closed?.should be_true
      end
    end

    it "releases leases on eviction and when the last subscriber leaves" do
      with_datadir do |data_dir|
        mfile = MFile.new(File.join(data_dir, "seg"), 64)
        mfile.print "hello"
        journal = LavinMQ::Clustering::Journal.new(2)
        sub = FakeSubscriber.new
        journal.subscribe(sub)
        journal.append_mfile("seg", mfile, 0i64, 5i64)
        mfile.close
        mfile.closed?.should be_false
        2.times { journal.delete("x") } # evicts the mfile entry
        mfile.closed?.should be_true

        mfile2 = MFile.new(File.join(data_dir, "seg2"), 64)
        mfile2.print "hello"
        journal.append_mfile("seg2", mfile2, 0i64, 5i64)
        mfile2.close
        mfile2.closed?.should be_false
        journal.unsubscribe(sub)
        mfile2.closed?.should be_true
      end
    end
  end

  describe "resuming" do
    it "serves entries all subscribers passed, without their mmap lease" do
      with_datadir do |data_dir|
        mfile = MFile.new(File.join(data_dir, "seg"), 64)
        mfile.print "hello"
        journal = LavinMQ::Clustering::Journal.new(8)
        sub = FakeSubscriber.new
        journal.subscribe(sub)
        lsn = journal.append_mfile("seg", mfile, 0i64, 5i64)
        sub.journal_position = lsn + 1
        journal.release_unneeded # the lease is gone, the mapping may be too
        mfile.close
        mfile.closed?.should be_true

        entry = journal.read(lsn).not_nil!
        entry.mfile.should be_nil # the reader fetches the bytes from the file
        entry.op.append?.should be_true
        entry.offset.should eq 0
        entry.len.should eq 5
        journal.behind?(lsn).should be_false
        journal.resumable?(lsn - 1).should be_true
        journal.resumable?(lsn).should be_true # caught up: nothing to send
        journal.resumable?(lsn + 1).should be_false
      end
    end

    it "is not resumable from an evicted entry" do
      journal = LavinMQ::Clustering::Journal.new(4)
      6.times { journal.delete("f") } # lsns 1..6, the first two evicted
      journal.resumable?(1).should be_false
      journal.resumable?(2).should be_true # entry 3 onwards is here
      journal.id.should_not eq 0
    end

    it "lets a late subscriber start below the released mark" do
      journal = LavinMQ::Clustering::Journal.new(8)
      3.times { journal.delete("f") }
      late = FakeSubscriber.new
      journal.subscribe(late, 2u64)
      late.journal_position.should eq 2
      journal.read(2).not_nil!.lsn.should eq 2
    end
  end

  it "keeps lsns unique and ordered under concurrent appends" do
    journal = LavinMQ::Clustering::Journal.new(1 << 12)
    done = Channel(Array(UInt64)).new
    4.times do |t|
      spawn do
        lsns = Array(UInt64).new(500)
        500.times { lsns << journal.delete("t#{t}") }
        done.send lsns
      end
    end
    all = Array(UInt64).new
    4.times { all.concat done.receive }
    all.sort!
    all.should eq (1u64..2000u64).to_a
  end
end
