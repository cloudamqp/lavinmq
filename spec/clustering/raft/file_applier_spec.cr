require "spec"
require "file_utils"
require "../../../src/lavinmq/clustering/raft/file_applier"
require "../../../src/lavinmq/clustering/raft/log_store"

private alias Raft = LavinMQ::Clustering::Raft
private alias Ops = LavinMQ::Clustering::Raft::Ops

private def with_dir(&)
  dir = File.tempname("raft-files-spec")
  Dir.mkdir_p dir
  yield dir
ensure
  FileUtils.rm_rf dir if dir
end

private def decode(data : Bytes) : Array(Ops::Op)
  ops = [] of Ops::Op
  Ops.each(data) { |op| ops << op }
  ops
end

# Batches as Raft entries, applied as a follower would and kept in a log, so
# they can be undone
private class Applied
  getter log = Raft::MemoryLogStore.new
  getter applier : Raft::FileApplier

  def initialize(@dir : String, @local = false)
    @applier = Raft::FileApplier.new(@dir, 0)
  end

  def apply(payload : String? = nil, & : Ops::Batch ->) : Int64
    batch = Ops::Batch.new
    yield batch
    data = batch.to_slice.dup
    @log.append Raft::Entry.new(1, nil, nil, data)
    @applier.apply(@log.last_index, data, payload.try(&.to_slice), local: @local)
    @applier.sync
    @log.last_index
  end

  def undo(from : Int64) : Nil
    @applier.undo(from, @log)
    @log.truncate_from(from)
  end
end

private def read(dir : String, path : String) : String?
  File.read(File.join(dir, path)) if File.exists?(File.join(dir, path))
end

describe Raft::Ops do
  it "round-trips every op" do
    batch = Ops::Batch.new
    batch.create(1, "vhost/msgs.1")
    batch.create(2, "", Ops::STAGING)
    batch.append(1, 0, 10)
    batch.append_inline(3, 4, Bytes[1, 2, 3, 4])
    batch.install(2, 7i64, "users.json")
    batch.delete(4, 8i64)
    batch.delete_dir(9i64, "vhost/queue")
    batch.truncate_to(1, 5, 10i64)
    batch.adopt(5, 100, "vhosts.json")
    ops = decode(batch.to_slice)
    ops.map(&.type).should eq [Ops::Type::Create, Ops::Type::Create, Ops::Type::Append, Ops::Type::AppendInline,
                               Ops::Type::Install, Ops::Type::Delete, Ops::Type::DeleteDir, Ops::Type::TruncateTo,
                               Ops::Type::Adopt]
    ops[0].path.should eq "vhost/msgs.1"
    ops[1].flags.should eq Ops::STAGING
    ops[2].pos.should eq 0
    ops[2].len.should eq 10
    ops[3].bytes.should eq Bytes[1, 2, 3, 4]
    ops[4].tid.should eq 7
    ops[4].path.should eq "users.json"
    ops[6].path.should eq "vhost/queue"
    ops[7].pos.should eq 5
    ops[8].pos.should eq 100
    batch.payload_bytes.should eq 10
    Ops.payload_bytes(batch.to_slice).should eq 10
  end

  it "makes one op of appends that follow each other, also across other files" do
    batch = Ops::Batch.new
    batch.append(1, 0, 10)
    batch.append(2, 0, 5)
    batch.append(1, 10, 20)
    batch.append(2, 5, 5)
    batch.append(1, 40, 1) # a gap
    batch.op_count.should eq 3
    ops = decode(batch.to_slice)
    ops.map { |o| {o.id, o.pos, o.len} }.should eq [{1u32, 0i64, 30i64}, {2u32, 0i64, 10i64}, {1u32, 40i64, 1i64}]
    batch.payload_bytes.should eq 41
  end

  it "starts over after clear" do
    batch = Ops::Batch.new
    batch.append(1, 0, 10)
    batch.clear
    batch.empty?.should be_true
    batch.append(1, 10, 5)
    decode(batch.to_slice).map(&.len).should eq [5]
  end

  it "rejects a cut short batch" do
    batch = Ops::Batch.new
    batch.create(1, "a")
    bytes = batch.to_slice
    expect_raises(Ops::CorruptError) { decode(bytes[0, bytes.size - 1]) }
  end
end

describe Raft::FileTable do
  it "round-trips and checks its checksum" do
    with_dir do |dir|
      table = Raft::FileTable.new
      table.add Raft::FileTable::FileEntry.new(3, "vhost/msgs.1", 42)
      table.add Raft::FileTable::FileEntry.new(5, Raft::FileTable.staging_path(5), 0, Ops::STAGING)
      table.trash_watermark = 9
      table.save(dir, 12)
      loaded = Raft::FileTable.load(dir, 12)
      loaded.should eq table
      loaded.trash_watermark.should eq 9
      loaded.next_id.should eq 6
      bytes = File.read(Raft::FileTable.path(dir, 12)).to_slice.dup
      bytes[10] ^= 0xff
      File.write(Raft::FileTable.path(dir, 12), bytes)
      expect_raises(Raft::FileTable::CorruptError) { Raft::FileTable.load(dir, 12) }
    end
  end

  it "is empty before anything is compacted, and missing otherwise" do
    with_dir do |dir|
      Raft::FileTable.load(dir, 0).empty?.should be_true
      expect_raises(Raft::FileTable::CorruptError) { Raft::FileTable.load(dir, 3) }
    end
  end
end

describe Raft::FileApplier do
  it "writes appends from the payload and inline" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("hello") do |b|
        b.create(1, "v/msgs.1")
        b.append(1, 0, 5)
        b.append_inline(1, 5, " world".to_slice)
      end
      read(dir, "v/msgs.1").should eq "hello world"
      a.applier.current[1].size.should eq 11
    end
  end

  it "undoes appends by cutting the file back" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("abc") { |b| b.create(1, "f"); b.append(1, 0, 3) }
      second = a.apply("def", &.append(1, 3, 3))
      read(dir, "f").should eq "abcdef"
      a.undo(second)
      read(dir, "f").should eq "abc"
      a.applier.current[1].size.should eq 3
    end
  end

  it "installs a replacement, putting the old file in the trash until it's undone" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("old") { |b| b.create(1, "users.json"); b.append(1, 0, 3) }
      replace = a.apply("new") do |b|
        b.create(2, "", Ops::STAGING)
        b.append(2, 0, 3)
        b.install(2, 100i64, "users.json")
      end
      read(dir, "users.json").should eq "new"
      a.applier.current.by_path?("users.json").not_nil!.id.should eq 2
      a.undo(replace)
      read(dir, "users.json").should eq "old"
      a.applier.current.by_path?("users.json").not_nil!.id.should eq 1
      Dir.exists?(File.join(dir, Raft::FileTable::STAGING_DIR, "2")).should be_false
    end
  end

  it "moves deleted files and directories to the trash, back on undo, and purges them on compaction" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("ab") do |b|
        b.create(1, "v/q/msgs.1")
        b.append(1, 0, 1)
        b.create(2, "v/acks")
        b.append(2, 0, 1)
      end
      del = a.apply { |b| b.delete(2, 7i64); b.delete_dir(8i64, "v/q") }
      File.exists?(File.join(dir, "v/acks")).should be_false
      Dir.exists?(File.join(dir, "v/q")).should be_false
      a.undo(del)
      read(dir, "v/acks").should eq "b"
      read(dir, "v/q/msgs.1").should eq "a"
      a.applier.current.size.should eq 2

      del = a.apply(&.delete(2, 9_i64))
      Dir.exists?(File.join(dir, ".raft_trash", "9")).should be_true
      a.applier.compact(del, a.log)
      Dir.exists?(File.join(dir, ".raft_trash", "9")).should be_false
      a.applier.base.trash_watermark.should eq 9
      a.applier.base_index.should eq del
      Raft::FileTable.load(dir, del).should eq a.applier.current
    end
  end

  it "undoes a truncation by appending the tail back" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("abcdef") { |b| b.create(1, "seg"); b.append(1, 0, 6) }
      cut = a.apply(&.truncate_to(1, 2, 5_i64))
      read(dir, "seg").should eq "ab"
      a.undo(cut)
      read(dir, "seg").should eq "abcdef"
    end
  end

  it "only updates the tables for the leader's own entries" do
    with_dir do |dir|
      a = Applied.new(dir, local: true)
      a.apply { |b| b.create(1, "f"); b.append(1, 0, 3) }
      File.exists?(File.join(dir, "f")).should be_false
      a.applier.current[1].size.should eq 3
    end
  end

  it "reconciles the data dir with the log on start" do
    with_dir do |dir|
      a = Applied.new(dir)
      a.apply("abc") { |b| b.create(1, "v/long"); b.append(1, 0, 3) }
      a.apply("abc") { |b| b.create(2, "v/short"); b.append(2, 0, 3) }
      done = a.apply(&.delete(1, 3_i64)) # trash referenced by the log
      a.applier.compact(done, a.log)     # purged, watermark 3
      a.apply("abc") { |b| b.create(3, "v/long"); b.append(3, 0, 3) }
      a.apply("b") { |b| b.create(4, "v/back"); b.append(4, 0, 1) }
      a.applier.close
      File.write(File.join(dir, "v/long"), "abcdef")       # appended after the log
      File.write(File.join(dir, "v/short"), "a")           # lost what the log has
      File.write(File.join(dir, "v/unknown"), "x")         # never in the log
      Dir.mkdir_p(File.join(dir, ".raft_trash", "2", "v")) # purge cut short
      File.write(File.join(dir, ".raft_trash", "2", "v", "gone"), "g")
      Dir.mkdir_p(File.join(dir, ".raft_trash", "4", "v")) # moved by a fenced broker
      File.rename(File.join(dir, "v", "back"), File.join(dir, ".raft_trash", "4", "v", "back"))

      restarted = Raft::FileApplier.new(dir, done)
      (done + 1..a.log.last_index).each { |i| restarted.replay(i, a.log.entry(i).data) }
      restarted.reconcile
      read(dir, "v/long").should eq "abc"
      restarted.needs_snapshot?.should be_true
      read(dir, "v/unknown").should be_nil
      read(dir, ".raft_quarantine/v/unknown").should eq "x"
      Dir.exists?(File.join(dir, ".raft_trash", "2")).should be_false
      read(dir, "v/back").should eq "b"
    end
  end

  it "deletes the table files the raft state no longer names" do
    with_dir do |dir|
      a = Applied.new(dir)
      first = a.apply(&.create(1, "f"))
      a.applier.compact(first, a.log)
      second = a.apply(&.create(2, "g"))
      a.applier.compact(second, a.log)
      a.applier.drop_old_tables
      Dir.children(dir).select { |n| Raft::FileTable.file?(n) }.should eq [".raft_files.#{second}"]
    end
  end
end
