require "spec"
require "../../src/stdlib/channel"

describe Channel do
  it "can send non blocking" do
    ch = Channel(Int32).new
    spawn do
      ch.receive.should eq 1
    end
    Fiber.yield # required to let the receiving fiber start
    ch.try_send(1).should be_true
    ch.try_send(2).should be_false
  end

  it "can send non blocking to buffered channel" do
    ch = Channel(Int32).new(1)
    spawn do
      ch.receive.should eq 1
    end
    Fiber.yield # required to let the receiving fiber start
    ch.try_send(1).should be_true
    ch.try_send(2).should be_true
    ch.try_send(3).should be_false
  end
end
