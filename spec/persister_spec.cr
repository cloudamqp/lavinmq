require "./spec_helper"

# Minimal no-op stand-in for a real replicator: only its presence matters
# here (Persister treats a non-nil @replicator as "clustering is enabled").
private class NullReplicator
  include LavinMQ::Clustering::Replicator

  def register_file(path : String)
  end

  def register_file(file : File)
  end

  def register_file(mfile : MFile)
  end

  def replace_file(path : String)
  end

  def replace_file(mfile : MFile)
  end

  def append(path : String, pos : Int, length : Int)
  end

  def append_value(path : String, value : UInt32 | Int32, offset : Int64)
  end

  def append_bytes(path : String, bytes : Bytes, offset : Int64)
  end

  def delete_file(path : String)
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

  def isr_dirty? : Bool
    false
  end

  def flush_isr : Nil
  end

  def wait_for_followers : Nil
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
end

private class BlockingPersister < LavinMQ::Persister
  getter sync_started = Channel(Nil).new
  getter release_sync = Channel(Nil).new

  protected def sync_data_dir : Nil
    @sync_started.send nil
    @release_sync.receive
  end

  # `sync` is private (only called internally by the sync loop); expose it so
  # specs can drive it directly instead of going through enqueue_ack/enqueue_tx_commit.
  def sync_public : Nil
    sync
  end
end

private class TimeoutPersister < LavinMQ::Persister
  def wait_for_syncfs_public : Nil
    wait_for_syncfs
  end

  protected def syncfs_timeout : Time::Span
    1.millisecond
  end
end

# Combines BlockingPersister's controllable sync_data_dir with a very short
# timeout, so the real sync() -> @syncfs_ok -> background watchdog path can
# be exercised end-to-end (a single producer/consumer pair on @syncfs_ok,
# unlike calling wait_for_syncfs directly alongside the persister's own live
# background watchdog fiber, which would race for the same signal).
private class BlockingTimeoutPersister < BlockingPersister
  protected def syncfs_timeout : Time::Span
    1.millisecond
  end
end

describe LavinMQ::Persister do
  it "exits when syncfs exceeds the timeout while clustering is enabled" do
    persister = TimeoutPersister.new(LavinMQ::Config.instance.data_dir, NullReplicator.new)

    ex = expect_raises(SpecExit) { persister.wait_for_syncfs_public }
    ex.code.should eq 1
  ensure
    persister.try &.close
  end

  it "logs but does not exit when syncfs exceeds the timeout and clustering is disabled" do
    persister = BlockingTimeoutPersister.new(LavinMQ::Config.instance.data_dir) # no replicator: standalone
    sync_result = Channel(Exception?).new(1)

    spawn do
      persister.sync_public
      sync_result.send nil
    rescue ex
      sync_result.send ex
    end
    persister.sync_started.receive

    # Give the background watchdog time to notice the 1ms timeout, log, and
    # settle into waiting for the real completion instead of exiting.
    sleep 20.milliseconds

    persister.release_sync.send nil
    sync_result.receive.should be_nil
  ensure
    persister.try &.close
  end
end
