require "spec"
require "../src/lavinmq/clustering/standalone_backend"

describe LavinMQ::Clustering::StandaloneBackend do
  it "yields to block immediately" do
    backend = LavinMQ::Clustering::StandaloneBackend.new
    yielded = false

    spawn do
      backend.campaign do
        yielded = true
      end
    end

    Fiber.yield
    yielded.should be_true
    backend.stop
  end

  it "stops when stop is called" do
    backend = LavinMQ::Clustering::StandaloneBackend.new
    stopped = false

    spawn do
      backend.campaign { }
      stopped = true
    end

    Fiber.yield
    stopped.should be_false

    backend.stop
    Fiber.yield
    stopped.should be_true
  end
end
