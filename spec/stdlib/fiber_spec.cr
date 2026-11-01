require "spec"
require "weak_ref"
require "wait_group"
require "../../src/stdlib/fiber"

# Spawns fibers that each capture a fresh object, waits for them to finish
# and returns the dead fibers and weak references to the objects
@[NoInline]
private def spawn_and_finish(count) : {Array(Fiber), Array(WeakRef(Array(Int32)))}
  refs = Array(WeakRef(Array(Int32))).new(count)
  wg = WaitGroup.new(count)
  fibers = Array(Fiber).new(count) do |i|
    captured = [i]
    refs << WeakRef.new(captured)
    spawn { captured.sum; wg.done }
  end
  wg.wait
  until fibers.all?(&.dead?)
    Fiber.yield
  end
  {fibers, refs}
end

describe Fiber do
  # A dead fiber can stay reachable from words left on live fibers' stacks,
  # e.g. a closed connection's read loop fiber, which then kept its socket
  it "doesn't keep its closure alive once dead" do
    fibers, refs = spawn_and_finish(100)
    GC.collect
    fibers.all?(&.dead?).should be_true
    # a few may still be found on a stack by the conservative GC
    refs.count(&.value).should be < 10
  end
end
