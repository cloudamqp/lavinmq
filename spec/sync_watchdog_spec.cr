require "./spec_helper"
require "../src/lavinmq/sync_watchdog"

private class ShortTimeoutWatchdog < LavinMQ::SyncWatchdog
  def wait_public : Nil
    wait
  end

  protected def timeout : Time::Span
    1.millisecond
  end
end

describe LavinMQ::SyncWatchdog do
  it "exits when a sync outlives the timeout and exit is enabled" do
    watchdog = ShortTimeoutWatchdog.new("spec", exit_on_timeout: true)
    ex = expect_raises(SpecExit) { watchdog.wait_public }
    ex.code.should eq 1
  ensure
    watchdog.try &.close
  end

  it "only logs and keeps guarding when exit is disabled" do
    watchdog = ShortTimeoutWatchdog.new("spec", exit_on_timeout: false)
    release = Channel(Nil).new
    done = Channel(Nil).new
    spawn do
      watchdog.guard { release.receive }
      done.send nil
    end
    sleep 10.milliseconds # let the watchdog time out
    release.send nil
    done.receive
    # The completion signal was consumed, so a fast sync doesn't trip it.
    watchdog.guard { }
    sleep 10.milliseconds
  ensure
    watchdog.try &.close
  end

  it "runs a guarded block after close" do
    watchdog = LavinMQ::SyncWatchdog.new("spec", exit_on_timeout: true)
    watchdog.close
    ran = false
    watchdog.guard { ran = true }
    ran.should be_true
  end
end
