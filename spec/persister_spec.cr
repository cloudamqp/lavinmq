require "./spec_helper"

private class StallReplicator < NoOpReplicator
  @in_sync = Atomic(Bool).new(false)

  def initialize(in_sync = false)
    @in_sync.set(in_sync)
  end

  def in_sync_followers? : Bool
    @in_sync.get(:acquire)
  end

  def in_sync_followers=(value : Bool)
    @in_sync.set(value, :release)
  end
end

private class StallTarget
  include LavinMQ::Persister::ConfirmTarget

  def enqueue_confirm_ack(msgid : UInt64) : Nil
  end
end

# Every sync blocks until released, and each watchdog verdict is reported:
# the exit code, or nil when it only logged
class LavinMQ::SpecStallingPersister < LavinMQ::Persister
  getter verdicts = Channel(Int32?).new(100)
  getter release = Channel(Nil).new

  protected def sync_timeout : Time::Span
    10.milliseconds
  end

  protected def sync_stalled(elapsed : Time::Span) : Nil
    super
    @verdicts.send nil
  rescue ex : SpecExit
    @verdicts.send ex.code
  end

  private def syncfs_data_dir : Nil
    @release.receive
  end

  private def fsync_paths(files, paths, dirs) : Nil
    @release.receive
  end
end

# The watchdog's verdict raises, like a log write failing on a closed pipe
class LavinMQ::SpecRaisingWatchdogPersister < LavinMQ::SpecStallingPersister
  protected def sync_stalled(elapsed : Time::Span) : Nil
    @verdicts.send nil
    raise IO::Error.new("log output closed (spec)")
  end
end

private def queue_dir(s : LavinMQ::Server, queue_name : String) : String
  File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(queue_name))
end

private def last_sync(s : LavinMQ::Server) : LavinMQ::Persister::SyncRecord
  s.persister.last_sync.should_not be_nil
  s.persister.last_sync.not_nil!
end

describe LavinMQ::Persister do
  it "syncs the segments of confirmed publishes, not of other publishes" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        plain = ch.queue("plain")
        confirmed = ch.queue("confirmed")
        plain.publish "not confirmed"
        confirmed.publish_confirm "confirmed"
        sync = last_sync(s)
        sync.syncfs.should be_false
        sync.paths.should contain File.join(queue_dir(s, "confirmed"), "msgs.0000000001")
        sync.paths.none?(&.starts_with?(queue_dir(s, "plain"))).should be_true
      end
    end
  end

  it "fsyncs the directory of a newly created segment once" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("new_segment")
        q.publish_confirm "first"
        last_sync(s).paths.should contain queue_dir(s, "new_segment")
        q.publish_confirm "second"
        last_sync(s).paths.should_not contain queue_dir(s, "new_segment")
        last_sync(s).paths.should contain File.join(queue_dir(s, "new_segment"), "msgs.0000000001")
      end
    end
  end

  it "doesn't sync the ack files consumers append to" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("acked")
        10.times { |i| q.publish "m#{i}" }
        10.times { q.get(no_ack: false).not_nil!.ack }
        should_eventually(be_true) { s.vhosts["/"].queue("acked").message_count.zero? }
        q.publish_confirm "confirmed"
        last_sync(s).paths.none?(&.includes?("acks.")).should be_true
      end
    end
  end

  it "syncs the whole filesystem on tx commit, so the tx acks are durable too" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("tx")
        q.publish "m"
        ch.tx_select
        q.get(no_ack: false).not_nil!.ack
        q.publish "tx publish"
        ch.tx_commit
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs the whole filesystem when a confirm depends on more files than the threshold" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.syncfs_threshold = 4
      with_channel(s) do |ch|
        x = ch.exchange("many", "fanout")
        (LavinMQ::Config.instance.syncfs_threshold + 1).times do |i|
          ch.queue("many#{i}").bind(x.name, "")
        end
        ch.confirm_select
        x.publish_confirm "to all", ""
        last_sync(s).syncfs.should be_true
      end
    end
  end

  describe "sync watchdog" do
    it "exits when a syncfs stalls and an in-sync follower can take over" do
      with_datadir do |data_dir|
        persister = LavinMQ::SpecStallingPersister.new(data_dir, StallReplicator.new(in_sync: true))
        spawn { persister.sync }
        persister.verdicts.receive.should eq 1
      ensure
        persister.try &.release.send nil
        persister.try &.close
      end
    end

    it "exits when a per-file fsync stalls and an in-sync follower can take over" do
      with_datadir do |data_dir|
        persister = LavinMQ::SpecStallingPersister.new(data_dir, StallReplicator.new(in_sync: true))
        persister.mark_dirty(File.join(data_dir, "file"))
        persister.enqueue_ack(StallTarget.new, 1u64)
        persister.verdicts.receive.should eq 1
      ensure
        persister.try &.release.send nil
        persister.try &.close
      end
    end

    it "only logs a stall when standalone" do
      with_datadir do |data_dir|
        persister = LavinMQ::SpecStallingPersister.new(data_dir)
        done = Channel(Nil).new
        spawn { persister.sync; done.send nil }
        persister.verdicts.receive.should be_nil
        persister.verdicts.receive.should be_nil
        persister.release.send nil
        done.receive
      ensure
        persister.try &.close
      end
    end

    it "only logs a stall when no follower is in-sync" do
      with_datadir do |data_dir|
        persister = LavinMQ::SpecStallingPersister.new(data_dir, StallReplicator.new)
        done = Channel(Nil).new
        spawn { persister.sync; done.send nil }
        persister.verdicts.receive.should be_nil
        persister.verdicts.receive.should be_nil
        persister.release.send nil
        done.receive
      ensure
        persister.try &.close
      end
    end

    it "exits once a follower becomes in-sync during the stall" do
      with_datadir do |data_dir|
        replicator = StallReplicator.new
        persister = LavinMQ::SpecStallingPersister.new(data_dir, replicator)
        spawn { persister.sync }
        persister.verdicts.receive.should be_nil
        replicator.in_sync_followers = true
        loop { break if persister.verdicts.receive == 1 }
      ensure
        persister.try &.release.send nil
        persister.try &.close
      end
    end

    it "keeps syncing and watching when a stall verdict raises" do
      with_datadir do |data_dir|
        persister = LavinMQ::SpecRaisingWatchdogPersister.new(data_dir)
        3.times do
          done = Channel(Nil).new
          spawn { persister.sync; done.send nil }
          persister.verdicts.receive.should be_nil
          persister.release.send nil
          select
          when done.receive
          when timeout(5.seconds)
            fail "sync wedged after the watchdog raised"
          end
        end
      ensure
        persister.try &.close
      end
    end
  end
end
