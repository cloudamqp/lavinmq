require "spec"
require "wait_group"
require "../src/lavinmq/bool_channel"

describe BoolChannel do
  # set updates the value and then switches which channel is active, so two
  # concurrent sets with different values could leave the value and the
  # active channel disagreeing, waking or blocking waiters wrongly
  it "keeps the active channel in sync with the value under concurrent sets" do
    ctx = Fiber::ExecutionContext::Parallel.new("bool-channel", 4)
    mismatches = 0
    2_000.times do
      bc = BoolChannel.new(false)
      wg = WaitGroup.new
      4.times do |i|
        wg.add(1)
        ctx.spawn do
          500.times { i.even? ? bc.set(true) : bc.swap(false) }
        ensure
          wg.done
        end
      end
      wg.wait
      active = select
      when bc.when_true.receive
        true
      when bc.when_false.receive
        false
      else
        nil
      end
      mismatches += 1 unless active == bc.value
    end
    mismatches.should eq 0
  end

  it "wakes one receiver per activation and per signal in wake_one mode" do
    bc = BoolChannel.new(true)
    bc.when_false.wake_one = true
    woken = Channel(Int32).new(3)
    3.times do |i|
      spawn do
        bc.when_false.receive
        woken.send i
      end
    end
    nothing_woken = -> do
      select
      when woken.receive then false
      when timeout 20.milliseconds then true
      end
    end
    sleep 10.milliseconds    # let them park
    bc.when_false.signal_one # inactive: nothing to wake for
    nothing_woken.call.should be_true
    bc.set(false)
    woken.receive.should eq 0 # the first to park
    nothing_woken.call.should be_true
    bc.when_false.signal_one
    woken.receive.should eq 1
    bc.set(true)
    bc.when_false.signal_one # inactive again, the third stays parked
    nothing_woken.call.should be_true
    bc.set(false)
    woken.receive.should eq 2
    bc.when_false.signal_one # no one left to wake
    nothing_woken.call.should be_true
  end
end
