require "spec"
require "../src/lavinmq/standalone_runner"

describe LavinMQ::StandaloneRunner do
  it "yields to block immediately" do
    runner = LavinMQ::StandaloneRunner.new
    yielded = false

    spawn do
      runner.run do
        yielded = true
      end
    end

    Fiber.yield
    yielded.should be_true
    runner.stop
  end

  it "stops when stop is called" do
    runner = LavinMQ::StandaloneRunner.new
    stopped = false

    spawn do
      runner.run { }
      stopped = true
    end

    Fiber.yield
    stopped.should be_false

    runner.stop
    Fiber.yield
    stopped.should be_true
  end
end
