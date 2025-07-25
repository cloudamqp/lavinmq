require "./spec_helper"

describe LavinMQ::HTTP::MetricsServer do
  it "should handle /metrics without authentication" do
    with_amqp_server do |s|
      metrics_server = LavinMQ::HTTP::MetricsServer.new(s)
      address = metrics_server.bind_tcp("127.0.0.1", 0) # Let system choose port
      spawn(name: "MetricsServer") { metrics_server.listen }
      Fiber.yield
      
      begin
        port = address.port
        client = HTTP::Client.new("127.0.0.1", port)
        response = client.get("/metrics")
        response.status_code.should eq(200)
        response.headers["Content-Type"].should eq("text/plain")
        response.body.should contain("lavinmq_")
      ensure
        client.try(&.close)
        metrics_server.close
      end
    end
  end

  it "should handle /metrics/detailed without authentication" do
    with_amqp_server do |s|
      metrics_server = LavinMQ::HTTP::MetricsServer.new(s)
      address = metrics_server.bind_tcp("127.0.0.1", 0) # Let system choose port
      spawn(name: "MetricsServer") { metrics_server.listen }
      Fiber.yield
      
      begin
        port = address.port
        client = HTTP::Client.new("127.0.0.1", port)
        response = client.get("/metrics/detailed?family=queue_coarse_metrics")
        response.status_code.should eq(200)
        response.headers["Content-Type"].should eq("text/plain")
      ensure
        client.try(&.close)
        metrics_server.close
      end
    end
  end

  it "should return 404 for other paths" do
    with_amqp_server do |s|
      metrics_server = LavinMQ::HTTP::MetricsServer.new(s)
      address = metrics_server.bind_tcp("127.0.0.1", 0) # Let system choose port
      spawn(name: "MetricsServer") { metrics_server.listen }
      Fiber.yield
      
      begin
        port = address.port
        client = HTTP::Client.new("127.0.0.1", port)
        response = client.get("/invalid")
        response.status_code.should eq(404)
        response.body.should eq("Not Found")
      ensure
        client.try(&.close)
        metrics_server.close
      end
    end
  end
end

describe LavinMQ::Config do
  it "should have default metrics_port of 15692" do
    config = LavinMQ::Config.new
    config.metrics_port.should eq 15692
  end

  it "should parse metrics_port from config file" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        data_dir = /tmp/lavinmq-spec
        [mgmt]
        metrics_port = 9090
      CONFIG
    end
    config = LavinMQ::Config.new
    config.config_file = config_file.path
    config.parse
    config.metrics_port.should eq 9090
  end
end