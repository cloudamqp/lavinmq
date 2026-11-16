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
      wg = WaitGroup.new(4)
      4.times do |i|
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

  it "lets a fiber already in wait_until_true through before one that arrives later" do
    bc = BoolChannel.new(false)
    abort = Channel(Nil).new
    order = [] of Symbol
    spawn do
      bc.wait_until_true(Channel(Nil).new(1), abort) { }
      order << :first
    end
    Fiber.yield
    bc.set(true) # wakes the first waiter, which hasn't run yet
    bc.wait_until_true(Channel(Nil).new(1), abort) { }.should be_true
    order << :second
    order.should eq [:first, :second]
  end

  it "lets the next in line through when the first is aborted after its turn came" do
    bc = BoolChannel.new(false)
    abort = Channel(Nil).new
    second = Channel(Bool).new(1)
    spawn { bc.wait_until_true(Channel(Nil).new(1), abort) { } }
    spawn { second.send bc.wait_until_true(Channel(Nil).new(1), Channel(Nil).new) { } }
    Fiber.yield
    abort.close  # the first waiter picks abort but hasn't run yet
    bc.set(true) # so its turn comes after it chose to leave
    select
    when ok = second.receive
      ok.should be_true
    when timeout(1.second)
      fail "the second waiter was never let through"
    end
  end
end
