require "spec"
require "../src/lavinmq/clustering/standalone_elector"

describe LavinMQ::Clustering::StandaloneElector do
  it "yields to block immediately" do
    elector = LavinMQ::Clustering::StandaloneElector.new
    yielded = false

    spawn do
      elector.campaign do
        yielded = true
      end
    end

    Fiber.yield
    yielded.should be_true
    elector.stop
  end

  it "stops when stop is called" do
    elector = LavinMQ::Clustering::StandaloneElector.new
    stopped = false

    spawn do
      elector.campaign { }
      stopped = true
    end

    Fiber.yield
    stopped.should be_false

    elector.stop
    Fiber.yield
    stopped.should be_true
  end
end
