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
    deadline = Time.instant + 5.seconds
    2_000.times do
      break if Time.instant >= deadline
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

  it "counts fibers in wait_until_true as waiting until they resume" do
    bc = BoolChannel.new(false)
    abort = Channel(Nil).new
    done = Channel(Bool).new(2)
    2.times { spawn { done.send bc.wait_until_true(abort) } }
    Fiber.yield
    bc.waiting?.should be_true
    bc.set(true)
    bc.waiting?.should be_true # woken, but not yet resumed
    done.receive.should be_true
    done.receive.should be_true
    bc.waiting?.should be_false
  end

  it "re-parks a woken waiter if the value turned false before it resumed" do
    bc = BoolChannel.new(false)
    abort = Channel(Nil).new
    done = Channel(Bool).new(1)
    spawn { done.send bc.wait_until_true(abort) }
    Fiber.yield
    bc.set(true)
    bc.set(false)
    Fiber.yield
    bc.waiting?.should be_true
    abort.close
    done.receive.should be_false
    bc.waiting?.should be_false
  end
end
