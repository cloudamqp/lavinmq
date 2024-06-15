require "./spec_helper"

describe LavinMQ do
  describe "random" do
    pending "disconnects" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue(qname)
          loop do
            q.publish "a" * 5000
          end
        end
        with_channel(s) do |ch|
          conn = ch.@connection.@io.as(TCPSocket)
          conn.linger = 0
          q = ch.queue(qname)
          count = 0
          q.subscribe(no_ack: false) do |_msg|
            count += 1
          end
          q.publish "1"
          q.publish "2"
          Fiber.yield
          conn.close
          Fiber.yield
          count.should eq 0

          Fiber.yield
          Server.vhosts["/"].queues[qname].@ready.size.should eq 2
          Server.vhosts["/"].queues[qname].@unacked.size.should eq 0
        end
      end
    end
  end
end
