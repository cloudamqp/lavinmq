require "spec"
require "../../src/stdlib/channel"

describe Channel do
  it "can send non blocking" do
    ready = Channel(Nil).new
    ch = Channel(Int32).new
    spawn do
      ready.send nil
      ch.receive.should eq 1
    end
    ready.receive
    ch.try_send(1).should be_true
    ch.try_send(2).should be_false
  end

  it "can send non blocking to buffered channel" do
    ready = Channel(Nil).new
    ch = Channel(Int32).new(1)
    spawn do
      ready.send nil
      ch.receive.should eq 1
    end
    ready.receive
    ch.try_send(1).should be_true
    ch.try_send(2).should be_true
    ch.try_send(3).should be_false
  end
end
