require "../spec_helper"

describe LavinMQ::Clustering::Client do
  add_etcd_around_each

  # Fixes #1366 - metrics_server is not closed when a follower client is closed
  it "can restart metrics_server after being a follower" do
    data_dir = File.tempname

    begin
      # Create a config for our test node
      config = LavinMQ::Config.instance.dup
      config.data_dir = data_dir
      config.metrics_http_port = 18765 # Use fixed non-default port for testing

      # Start as a follower with metrics_server running
      client = LavinMQ::Clustering::Client.new(config, 1, "test_password", proxy: false)
      Fiber.yield # let metrics_server start

      # Close the follower - this should clean up the metrics_server
      client.close
      Fiber.yield # let client close finish

      # Now try to start a new metrics server on the same port
      metrics_server = LavinMQ::HTTP::MetricsServer.new
      metrics_server.bind_tcp(config.metrics_http_bind, config.metrics_http_port)

      metrics_server.close
    end
  end
end
