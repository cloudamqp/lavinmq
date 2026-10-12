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
end
