require "spec"
require "socket"
require "../../src/stdlib/isolated_wait_for"

describe Fiber::ExecutionContext::Isolated do
  # On Crystal 1.22.0 a fiber waiting to read was resumed when its event loop
  # returned without a ready fiber, e.g. on a write readiness event after a
  # write, and the next read segfaulted.
  it "writes and then waits to read on a socket" do
    client, server = UNIXSocket.pair
    replies = 0
    ctx = Fiber::ExecutionContext::Isolated.new("request-reply") do
      buf = Bytes.new(4)
      20.times do
        client.write "ping".to_slice
        client.read_fully(buf)
        replies += 1 if String.new(buf) == "pong"
      end
    end
    buf = Bytes.new(4)
    20.times do
      server.read_fully(buf)
      sleep 1.millisecond # let the client wait for the reply
      server.write "pong".to_slice
    end
    ctx.wait
    replies.should eq 20
  ensure
    client.try &.close
    server.try &.close
  end
end
