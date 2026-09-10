require "../spec_helper"
require "../../src/lavinmq/clustering/disk_watchdog"

describe LavinMQ::Clustering::DiskWatchdog do
  it "does not fence when probes complete in time" do
    fenced = Channel(String).new(1)
    wd = LavinMQ::Clustering::DiskWatchdog.new(
      interval: 1.milliseconds, timeout: 200.milliseconds,
      probe: -> { nil } # returns immediately
) { |reason| fenced.send(reason) }
    spawn wd.run
    select
    when fenced.receive
      fail "should not have fenced on a healthy disk"
    when timeout(100.milliseconds)
      # good: several probes ran without fencing
    end
  end

  it "fences when a probe stalls past the timeout" do
    fenced = Channel(String).new(1)
    wd = LavinMQ::Clustering::DiskWatchdog.new(
      interval: 1.milliseconds, timeout: 50.milliseconds,
      probe: -> { sleep 10.seconds } # simulates a wedged fsync
) { |reason| fenced.send(reason) }
    spawn wd.run
    select
    when reason = fenced.receive
      reason.should contain "stalled"
    when timeout(2.seconds)
      fail "watchdog did not fence on a stalled probe"
    end
  end

  it "fences when a probe returns an error (EIO / read-only remount)" do
    fenced = Channel(String).new(1)
    wd = LavinMQ::Clustering::DiskWatchdog.new(
      interval: 1.milliseconds, timeout: 2.seconds,
      probe: -> { raise IO::Error.new("Read-only file system") }
    ) { |reason| fenced.send(reason) }
    spawn wd.run
    select
    when reason = fenced.receive
      reason.should contain "probe failed"
    when timeout(2.seconds)
      fail "watchdog did not fence on a failing probe"
    end
  end

  it "fences at most once" do
    count = 0
    done = Channel(Nil).new(1)
    wd = LavinMQ::Clustering::DiskWatchdog.new(
      interval: 1.milliseconds, timeout: 2.seconds,
      probe: -> { raise IO::Error.new("boom") }
    ) do |_reason|
      count += 1
      done.send(nil) rescue nil
    end
    spawn wd.run
    done.receive
    sleep 50.milliseconds # give the loop a chance to (wrongly) fence again
    count.should eq 1
  end

  it "does not fence after stop (graceful shutdown)" do
    fenced = Channel(String).new(1)
    wd = LavinMQ::Clustering::DiskWatchdog.new(
      interval: 1.milliseconds, timeout: 50.milliseconds,
      probe: -> { sleep 10.seconds } # would fence if the loop ran
) { |reason| fenced.send(reason) }
    wd.stop # requested before run: the loop must exit without probing
    spawn wd.run
    select
    when fenced.receive
      fail "should not fence after stop"
    when timeout(200.milliseconds)
      # good: stopped watchdog never probed
    end
  end

  it "real disk probe succeeds on a working data dir" do
    dir = File.tempname
    Dir.mkdir_p dir
    begin
      LavinMQ::Clustering::DiskWatchdog.probe_disk(dir)
      File.exists?(File.join(dir, ".disk_watchdog")).should be_false # cleaned up
    ensure
      FileUtils.rm_rf dir
    end
  end
end
