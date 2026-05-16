require "./spec_helper"
require "file_utils"
require "log/spec"
require "time"
require "../src/lavinmq/message_store"

class SpyReplicator
  include LavinMQ::Clustering::Replicator

  getter registered_files = Hash(String, Symbol).new
  getter deleted_files = Set(String).new
  getter replaced_files = Array(String).new

  def register_file(path : String)
    @registered_files[path] = :path
  end

  def register_file(file : File)
    @registered_files[file.path] = :file
  end

  def register_file(mfile : MFile)
    @registered_files[mfile.path] = :mfile
  end

  def replace_file(path : String)
    @replaced_files << path
  end

  def append(path : String, obj)
  end

  def delete_file(path : String, wg : WaitGroup)
    @deleted_files << path
  end

  def followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def syncing_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def all_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def close
  end

  def listen(server : TCPServer)
  end

  def clear
  end

  def password : String
    ""
  end

  def wait_for_sync(& : -> Nil) : Nil
    yield
  end
end

def mktmpdir(&)
  path = File.tempname
  Dir.mkdir_p(path)
  begin
    yield path
  ensure
    FileUtils.rm_r(path)
  end
end

def with_store(*, replicator = nil, durable = true, &)
  mktmpdir do |dir|
    store = LavinMQ::MessageStore.new(dir, replicator, durable: durable)
    begin
      yield store, dir
    ensure
      store.close
    end
  end
end

# Sets up a fully-acked segment 1 and appends 2 orphaned ack positions past the
# real data end — simulates an unclean shutdown where ack writes survived but
# the matching msg writes didn't.
def setup_orphaned_ack_scenario(dir)
  body = "a" * 1000
  store = LavinMQ::MessageStore.new(dir, nil, durable: true)
  5.times { store.push(LavinMQ::Message.new("ex", "rk", body)) }
  while env = store.shift?
    store.delete(env.segment_position)
  end
  store.close
  wait_for { store.closed }

  msg_path = File.join(dir, "msgs.0000000001")
  real_data_end = File.size(msg_path).to_u32
  File.open(File.join(dir, "acks.0000000001"), "a") do |f|
    f.write_bytes(real_data_end, IO::ByteFormat::SystemEndian)
    f.write_bytes(real_data_end + 1024u32, IO::ByteFormat::SystemEndian)
  end
end

describe LavinMQ::MessageStore do
  it "deletes orphaned ack files" do
    mktmpdir do |dir|
      # Create a dummy msgs file
      File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
      # Create a corresponding acks file
      File.write(File.join(dir, "acks.0000000001"), "")
      # Create an orphaned acks file
      File.write(File.join(dir, "acks.0000000002"), "")

      store = LavinMQ::MessageStore.new(dir, nil)
      store.close

      File.exists?(File.join(dir, "acks.0000000001")).should be_true
      File.exists?(File.join(dir, "acks.0000000002")).should be_false
    end
  end

  # Verifies #1296
  it "deletes unused segments on startup when multiple 'empty' segments exist" do
    with_store do |store, dir|
      msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
      msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))

      5.times { store.push(msg) }
      store.@segments.size.should eq 5
      store.close
      wait_for { store.closed }

      # Write ack files
      store.@segments.keys.each do |seg|
        break if seg == store.@segments.keys.last # don't ack the last segment
        path = File.join(dir, "acks." + seg.to_s.rjust(10, '0'))
        File.open(path, "w") do |f|
          f.write_bytes(4u32) # only one message per segment
        end
      end

      store = LavinMQ::MessageStore.new(dir, nil, durable: true)
      segment_files = Dir.glob(File.join(dir, "msgs.*")).count &.match(/msgs.\d{10}$/)
      store.@segments.size.should eq 1
      segment_files.should eq 1
    end
  end

  it "advances @rfile when delete_unused_segments removes the rfile's segment" do
    mktmpdir do |dir|
      msg_size = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
      msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
        AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))

      # Leave one segment on disk with all messages acked.
      store = LavinMQ::MessageStore.new(dir, nil, durable: true)
      store.push(msg)
      env = store.shift?.should_not be_nil
      store.delete(env.segment_position)
      store.close

      # Reopen. The leftover segment is kept at startup because it is
      # current_seg, so @rfile = @wfile points at it.
      store = LavinMQ::MessageStore.new(dir, nil, durable: true)

      # Pushing a message that needs to roll the segment runs
      # delete_unused_segments. The fix must advance @rfile before
      # delete_file closes the orphaned mfile, otherwise the next shift?
      # would raise IO::Error("Closed mfile").
      store.push(msg)
      env = store.shift?.should_not be_nil
      store.delete(env.segment_position)
      store.close
    end
  end

  #
  # Run all specs for both durable and non-durable stores
  #
  {false, true}.each do |durable|
    describe "when #{durable ? "" : "non-"}durable" do
      describe "#first?" do
        it "should return nil from empty segment" do
          mktmpdir do |dir|
            File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
            store = LavinMQ::MessageStore.new(dir, nil, durable)
            store.@segments.first_value.truncate(1000)
            store.first?.should be_nil
            store.close
          end
        end
      end

      describe "#shift?" do
        it "should return nil from empty segment" do
          mktmpdir do |dir|
            File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
            store = LavinMQ::MessageStore.new(dir, nil, durable)
            store.@segments.first_value.truncate(1000)
            store.shift?.should be_nil
            store.close
          end
        end
      end

      it "can ack messages after restart" do
        mktmpdir do |dir|
          File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
          File.write(File.join(dir, "acks.0000000001"), "")
          store = LavinMQ::MessageStore.new(dir, nil, durable)
          body_io = IO::Memory.new("hello")
          message = LavinMQ::Message.new(RoughTime.unix_ms, "test_exchange", "test_key", AMQ::Protocol::Properties.new, 5u64, body_io)
          store.push(message)
          env = store.shift?.should_not be_nil
          String.new(env.message.as(LavinMQ::BytesMessage).body).should eq "hello"
          store.delete(env.segment_position)
          store.close
        end
      end

      describe "#push" do
        it "should not raise when rolling over on new wfile" do
          with_store(durable: durable) do |store|
            msg_size = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
            msg = LavinMQ::Message.new(
              RoughTime.unix_ms, "e", "k",
              AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size)
            )
            begin
              10.times do
                store.push msg
              end
            rescue ex
              fail ex.inspect_with_backtrace, line: (__LINE__ - 3) # -3 to get the push line
            end
          end
        end
      end

      describe "#empty?" do
        it "should be true for a fresh store" do
          with_store(durable: durable) do |store|
            store.empty?.should be_true
          end
        end

        it "should be false after push" do
          with_store(durable: durable) do |store|
            store.push LavinMQ::Message.new("ex", "rk", "body")
            store.empty?.should be_false
          end
        end

        it "should be true after all messages has been shifted" do
          with_store(durable: durable) do |store|
            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            while store.shift?
            end

            store.empty?.should be_true
          end
        end
      end

      describe "#empty" do
        it "should be true for a new store" do
          with_store(durable: durable) do |store|
            store.empty.should be_true
          end
        end

        it "should be true after all messages has been shifted" do
          with_store(durable: durable) do |store|
            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            while store.shift?
            end

            store.empty.should be_true
          end
        end

        it "should be false on after a push to a new store" do
          with_store(durable: durable) do |store|
            store.push LavinMQ::Message.new("ex", "rk", "body")

            store.empty.should be_false
          end
        end

        it "should be false after push to an empty store" do
          with_store(durable: durable) do |store|
            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            while store.shift?
            end

            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            store.empty.should be_false
          end
        end

        it "should be false after requeue" do
          with_store(durable: durable) do |store|
            store.push LavinMQ::Message.new("ex", "rk", "body")
            env = store.shift?.should_not be_nil
            store.empty.should be_true
            store.requeue env.segment_position
            store.empty.should be_false
          end
        end

        it "should be true after last message has been purged" do
          with_store(durable: durable) do |store|
            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            store.empty.should be_false
            store.purge(10)

            store.empty.should be_true
          end
        end

        it "should be true after purge_all" do
          with_store(durable: durable) do |store|
            5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

            store.empty.should be_false
            store.purge_all

            store.empty.should be_true
          end
        end
      end
    end
  end

  it "closes gracefully when segment has corrupt schema version with replicator" do
    with_etcd do
      mktmpdir do |dir|
        # Create a valid store with a message, then close it
        store = LavinMQ::MessageStore.new(dir, nil)
        store.push(LavinMQ::Message.new("ex", "rk", "body"))
        store.close

        # Corrupt the schema version at the start of the segment file
        seg_file = Dir.children(dir).find!(&.starts_with?("msgs."))
        File.open(File.join(dir, seg_file), "r+") { |f| f.write("abcd".to_slice) }

        # With a replicator (no followers), close spawns a fiber that races
        # with the constructor — this should close gracefully, not crash
        replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
        begin
          store = LavinMQ::MessageStore.new(dir, replicator)
          store.closed.should be_true
        ensure
          replicator.close
        end
      end
    end
  end

  it "closes gracefully when a middle segment is corrupt with replicator" do
    with_etcd do
      mktmpdir do |dir|
        # Create a store with multiple segments (one message per segment)
        store = LavinMQ::MessageStore.new(dir, nil)
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        store.@segments.size.should eq 3
        store.close

        # Corrupt the schema version of the second segment
        seg_files = Dir.children(dir).select(&.starts_with?("msgs.")).sort!
        File.open(File.join(dir, seg_files[1]), "r+") { |f| f.write("abcd".to_slice) }

        # With a replicator (no followers), this should close gracefully
        # even though valid segments before the corrupt one were already loaded
        replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
        begin
          store = LavinMQ::MessageStore.new(dir, replicator)
          store.closed.should be_true
        ensure
          replicator.close
        end
      end
    end
  end

  describe "replication" do
    it "registers the initial segment file" do
      mktmpdir do |dir|
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        replicator.registered_files.keys.map { |p| File.basename(p) }.should contain("msgs.0000000001")
      end
    end

    it "registers existing segment files on startup" do
      mktmpdir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        replicator.registered_files.keys.map { |p| File.basename(p) }.should contain("msgs.0000000001")
      end
    end

    it "registers ack files on startup" do
      mktmpdir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
        File.write(File.join(dir, "acks.0000000001"), "")
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        replicator.registered_files.keys.map { |p| File.basename(p) }.should contain("acks.0000000001")
      end
    end

    it "does not register orphaned ack files" do
      mktmpdir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
        File.write(File.join(dir, "acks.0000000002"), "")
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        replicator.registered_files.keys.map { |p| File.basename(p) }.should_not contain("acks.0000000002")
      end
    end

    it "deletes orphaned ack files from the replicator on startup" do
      mktmpdir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
        orphan_path = File.join(dir, "acks.0000000002")
        File.write(orphan_path, "")
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        replicator.deleted_files.should contain(orphan_path)
      end
    end

    it "deletes fully-acked segments from the replicator on startup" do
      mktmpdir do |dir|
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))

        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        2.times { store.push(msg) }
        store.close
        wait_for { store.closed }

        File.open(File.join(dir, "acks.0000000001"), "w") do |f|
          f.write_bytes(4u32)
        end

        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator, durable: true)
        store.close

        replicator.deleted_files.map { |p| File.basename(p) }.should contain("msgs.0000000001")
      end
    end

    it "re-registers files without an MFile reference when closed" do
      mktmpdir do |dir|
        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        sleep 1.millisecond
        file_registrations = replicator.registered_files.select { |_, t| t == :path }
        file_registrations.keys.map { |p| File.basename(p) }.should contain("msgs.0000000001")
      end
    end

    it "registers ack files from all segments on normal startup" do
      mktmpdir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        store.close
        wait_for { store.closed }

        seg_files = Dir.children(dir).select(&.starts_with?("msgs.")).sort!
        ack_files = seg_files.map(&.sub("msgs.", "acks."))
        ack_files.each { |f| File.open(File.join(dir, f), "w", &.write_bytes(4_u32)) }

        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        store.close
        registered = replicator.registered_files.keys.map { |p| File.basename(p) }
        ack_files.each { |f| registered.should contain(f) }
      end
    end

    it "deletes orphaned ack file and registers valid ack files when a segment is corrupt" do
      mktmpdir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        store.close
        wait_for { store.closed }

        seg_files = Dir.children(dir).select(&.starts_with?("msgs.")).sort!
        ack_files = seg_files.map(&.sub("msgs.", "acks."))
        ack_files.each { |f| File.open(File.join(dir, f), "w", &.write_bytes(4_u32)) }
        orphan_ack = File.join(dir, "acks.0000000099")
        File.write(orphan_ack, "")
        File.open(File.join(dir, seg_files[0]), "r+") { |f| f.write("abcd".to_slice) }

        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator)
        sleep 1.millisecond
        store.closed.should be_true
        registered = replicator.registered_files.keys.map { |p| File.basename(p) }
        ack_files.each { |f| registered.should contain(f) }
        replicator.deleted_files.should contain(orphan_ack)
        File.exists?(orphan_ack).should be_false
      end
    end

    [
      {"first segment", 3, 0},
      {"middle segment", 4, 1},
      {"last segment", 3, -1},
    ].each do |desc, n_segments, corrupt_idx|
      it "registers all files when the #{desc} is corrupt" do
        mktmpdir do |dir|
          store = LavinMQ::MessageStore.new(dir, nil, durable: true)
          msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
          msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
          n_segments.times { store.push(msg) }
          store.close
          wait_for { store.closed }

          seg_files = Dir.children(dir).select(&.starts_with?("msgs.")).sort!
          ack_files = seg_files.map(&.sub("msgs.", "acks."))
          ack_files.each { |f| File.open(File.join(dir, f), "w", &.write_bytes(4_u32)) }
          File.open(File.join(dir, seg_files[corrupt_idx]), "r+") { |f| f.write("abcd".to_slice) }

          replicator = SpyReplicator.new
          store = LavinMQ::MessageStore.new(dir, replicator)
          sleep 1.millisecond
          store.closed.should be_true
          registered = replicator.registered_files.keys.map { |p| File.basename(p) }
          (seg_files + ack_files).each { |f| registered.should contain(f) }
        end
      end
    end

    it "replicates the rewritten ack file after pruning orphans" do
      mktmpdir do |dir|
        setup_orphaned_ack_scenario(dir)

        replicator = SpyReplicator.new
        store = LavinMQ::MessageStore.new(dir, replicator, durable: true)
        store.close

        replicator.replaced_files.map { |p| File.basename(p) }.should contain("acks.0000000001")
        replicator.registered_files.keys.map { |p| File.basename(p) }.should contain("acks.0000000001")
      end
    end
  end

  # #1862 — on unclean shutdown, acks.* can end up with positions past the
  # corresponding msgs.* data end (orphaned acks), causing shift? to skip
  # newly-pushed messages and raise "EOF but @size=1".
  describe "after crash with fully acked segment" do
    # Simulates unclean shutdown where mmap msg writes were lost but ack writes survived:
    # the ack file references positions that don't exist in the msg file anymore.
    it "prunes orphaned acks across multiple segments when all positions are orphaned" do
      mktmpdir do |dir|
        # Two segments with only a schema header and an all-orphaned ack file
        # each — exercises the @deleted.delete(seg) branch for multiple segments.
        [1u32, 2u32].each do |seg|
          File.write(File.join(dir, "msgs.#{seg.to_s.rjust(10, '0')}"), "\x04\x00\x00\x00")
          File.open(File.join(dir, "acks.#{seg.to_s.rjust(10, '0')}"), "w") do |f|
            f.write_bytes(100u32, IO::ByteFormat::SystemEndian)
            f.write_bytes(200u32, IO::ByteFormat::SystemEndian)
          end
        end

        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        store.@deleted.empty?.should be_true
        store.close
      end
    end

    it "does not raise EOF when ack file has orphaned positions past data" do
      mktmpdir do |dir|
        setup_orphaned_ack_scenario(dir)

        # Reopen — prune_orphaned_acks should drop the 2 orphaned positions.
        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        store.@size.should eq 0
        store.@deleted[1]?.try(&.size).should eq 5
        store.@acks[1].size.should eq 5 * sizeof(UInt32)

        body = "a" * 1000
        store.push(LavinMQ::Message.new("ex", "rk", body))
        store.@size.should eq 1

        env = store.shift?
        env.should_not be_nil
        String.new(env.not_nil!.message.as(LavinMQ::BytesMessage).body).should eq body
        store.@size.should eq 0
        store.close
      end
    end

    it "logs when pruning orphaned ack positions" do
      mktmpdir do |dir|
        setup_orphaned_ack_scenario(dir)
        Log.capture("lmq.*", :warn) do |log|
          store = LavinMQ::MessageStore.new(dir, nil, durable: true)
          store.close
          log.check(:warn, /Msgs\/acks files for segment 1 are out of sync.*Removing 2 orphaned ack position\(s\)/)
        end
      end
    end

    it "handles orphaned ack positions without crashing when opened as non-durable" do
      mktmpdir do |dir|
        setup_orphaned_ack_scenario(dir)

        # Non-durable reopens unlink the msg/ack files as they load, so the
        # ack file is detected as orphaned and @deleted never gets populated.
        # The important thing is that opening doesn't crash.
        store = LavinMQ::MessageStore.new(dir, nil, durable: false)
        (store.@deleted[1]?.nil? || store.@deleted[1].empty?).should be_true
        store.close
      end
    end

    it "deletes leftover tmp.acks.* files in the data dir" do
      mktmpdir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), "\x04\x00\x00\x00")
        File.write(File.join(dir, "acks.0000000001"), "")
        orphan_tmp = File.join(dir, "tmp.acks.0000000001")
        File.write(orphan_tmp, "garbage-bytes")

        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        store.@acks.has_key?(1u32).should be_true
        File.exists?(orphan_tmp).should be_false
        store.close
      end
    end

    it "does not raise EOF when rfile.pos is at end when open_new_segment fires" do
      mktmpdir do |dir|
        body = "a" * 1000
        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        5.times { store.push(LavinMQ::Message.new("ex", "rk", body)) }
        while env = store.shift?
          store.delete(env.segment_position)
        end
        store.close
        wait_for { store.closed }

        store = LavinMQ::MessageStore.new(dir, nil, durable: true)
        store.@size.should eq 0

        # Push-shift-ack one small msg; rfile.pos is now at end of segment 1.
        store.push(LavinMQ::Message.new("ex", "rk", body))
        env = store.shift?.not_nil!
        store.delete(env.segment_position)
        store.empty?.should be_true

        # Invariant: rfile == wfile and rfile.pos == rfile.size (fully consumed).
        store.@rfile.pos.should eq store.@rfile.size

        # Now push a large message that forces open_new_segment.
        # open_new_segment adds segment 2 and delete_unused_segments deletes segment 1.
        # @rfile still points to orphan segment 1 with pos == size.
        half_seg = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
        store.push(LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, half_seg, IO::Memory.new("b" * half_seg)))
        store.@size.should eq 1

        env = store.shift?
        env.should_not be_nil
        store.close
      end
    end
  end

  # Regression: purge_all phase 1 used to subtract @segment_msg_count from @size
  # without accounting for already-acked messages in the segment, so any
  # acked-but-not-fully-acked segment under purge_all could underflow @size or
  # leave it inconsistent — observed under MT stress as "EOF but @size=N".
  describe "#purge_all" do
    it "leaves @size == 0 when deleted segments contain partial acks" do
      with_store do |store|
        # Fill several segments so phase 1 has real segments to delete.
        half_seg = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
        big = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, half_seg, IO::Memory.new("a" * half_seg))
        6.times { store.push(big) } # 3 segments, 2 msgs each
        store.@segments.size.should be >= 3

        # Shift+ack just the first message — leaves segment 1 with one ready
        # message and one ack, exercising the partial-ack code path in phase 1.
        env = store.shift?.not_nil!
        store.delete(env.segment_position)

        store.purge_all
        store.@size.should eq 0
        store.@bytesize.should eq 0
        store.empty?.should be_true
        store.empty.value.should be_true
      end
    end

    it "shift? from @requeued keeps @size consistent when the segment is gone" do
      with_store do |store|
        store.push(LavinMQ::Message.new("ex", "rk", "body"))
        env = store.shift?.not_nil!
        store.requeue(env.segment_position)
        store.@size.should eq 1

        # Simulate a race: the requeued sp's segment was deleted by another
        # purge before we got back to shift?. Without the fix, shift? bubbles
        # an exception while the requeued entry's accounting is left behind.
        store.@segments.delete(env.segment_position.segment)

        expect_raises(LavinMQ::MessageStore::Error) do
          store.shift?
        end
        store.@size.should eq 0
        store.@bytesize.should eq 0
      end
    end

    it "leaves @size == 0 even when shift? raises during the loop" do
      with_store do |store|
        store.push(LavinMQ::Message.new("ex", "rk", "body"))
        env = store.shift?.not_nil!
        store.requeue(env.segment_position)

        # Same race as above, but exercised through purge_all so we also
        # confirm the rescue/reset safety net runs to completion.
        store.@segments.delete(env.segment_position.segment)

        store.purge_all
        store.@size.should eq 0
        store.@bytesize.should eq 0
        store.empty.value.should be_true
      end
    end
  end
end
