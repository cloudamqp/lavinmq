require "./spec_helper"

private class BlockingPersister < LavinMQ::Persister
  getter sync_started = Channel(Nil).new
  getter release_sync = Channel(Nil).new

  protected def sync_data_dir : Nil
    @sync_started.send nil
    @release_sync.receive
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

describe LavinMQ::Persister do
  it "exits when syncfs exceeds the timeout" do
    persister = TimeoutPersister.new(LavinMQ::Config.instance.data_dir)

    ex = expect_raises(SpecExit) { persister.wait_for_syncfs_public }
    ex.code.should eq 1
  ensure
    persister.try &.close
  end

  it "serializes concurrent sync calls" do
    persister = BlockingPersister.new(LavinMQ::Config.instance.data_dir)
    completed = Channel(Nil).new(2)

    spawn do
      persister.sync
      completed.send nil
    end
    persister.sync_started.receive

    spawn do
      persister.sync
      completed.send nil
    end

    select
    when persister.sync_started.receive
      fail "a second sync entered while the first was blocked"
    when timeout(100.milliseconds)
      # The second sync is waiting for the first to release the mutex.
    end

    persister.release_sync.send nil
    persister.sync_started.receive
    persister.release_sync.send nil
    2.times { completed.receive }
  ensure
    persister.try &.close
  end
end
