require "../spec_helper"

describe LavinMQ::Clustering::Client do
  # A replication client being closed by the leader-change listener when this
  # node wins the election: Controller#run's own close returns at once (the
  # client is already closing) and the Launcher starts serving while the first
  # close still waits for the follower loop. The metrics port must be free by
  # then, or promotion fails with a port conflict.
  it "doesn't hold the metrics port while it is being closed", tags: "slow" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.instance.dup
      config.data_dir = data_dir
      config.metrics_http_bind = "127.0.0.1"
      config.metrics_http_port = TCPServer.open("127.0.0.1", 0, &.local_address.port)

      client = LavinMQ::Clustering::Client.new(config, 1, "test_password", proxy: false)
      closed = Channel(Nil).new
      # Never followed, so this close waits out the follower loop timeout
      spawn(name: "first close spec") do
        client.close
        closed.close
      end
      Fiber.yield
      client.close # returns while the first close is still running

      metrics_server = LavinMQ::HTTP::MetricsServer.new
      begin
        metrics_server.bind_tcp(config.metrics_http_bind, config.metrics_http_port)
      ensure
        metrics_server.close
        closed.receive?
      end
    end
  end
end
