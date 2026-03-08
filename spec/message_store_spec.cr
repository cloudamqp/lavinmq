require "./spec_helper"
require "file_utils"
require "time"
require "../src/lavinmq/message_store"

# Replicator mock to "record" files being registered
class SpyReplicator
  include LavinMQ::Clustering::Replicator

  getter registered_files = Hash(String, Symbol).new
  getter deleted_files = Set(String).new

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
          String.new(env.message.body).should eq "hello"
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
  end
end
