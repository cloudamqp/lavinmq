require "spec"
require "file_utils"
require "amqp-client"
require "benchmark"
require "../src/lavinmq/server"
require "../src/lavinmq/launcher"

describe LavinMQ::Server do
  around_each do |spec|
    dir = "/tmp/lavinmq-spec-#{rand}"
    server = LavinMQ::Server.new(dir)
    System.maximize_fd_limit
    tcp_server = TCPServer.new("::1", 55672)
    spawn server.listen(tcp_server)
    spec.run
    server.close
    FileUtils.rm_rf dir
  end

  pending "doesn't use much memory per queue" do
    count = 1500
    conn = AMQP::Client.new(host: "::1", port: 55672).connect
    ch = conn.channel
    fiber_count1 = Fiber.count
    vsize1 = `ps -o vsize= -p $PPID`.to_i64
    rss1 = `ps -o rss= -p $PPID`.to_i64
    count.times do |i|
      ch.queue_declare("p#{i}")
    end
    fiber_count2 = Fiber.count
    vsize2 = `ps -o vsize= -p $PPID`.to_i64
    rss2 = `ps -o rss= -p $PPID`.to_i64
    vsize = vsize2 - vsize1
    vsize.should be < 8_500 * count
    rss = rss2 - rss1
    rss.should be < 65 * count
    fiber_count = fiber_count2 - fiber_count1
    fiber_count.should be <= count
  end
end
