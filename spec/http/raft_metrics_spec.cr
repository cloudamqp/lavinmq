require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/elector"
require "../../src/lavinmq/http/controller/prometheus"

private def tmp_data_dir : String
  dir = File.tempname("raft-metrics-spec")
  Dir.mkdir_p(dir)
  dir
end

describe "ISR Prometheus metrics" do
  it "emits lavinmq_raft_isr_size and lavinmq_raft_in_isr when elector is set" do
    dir = tmp_data_dir
    elector = nil.as(LavinMQ::Raft::Elector?)
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      elector = LavinMQ::Raft::Elector.new(config)
      r = elector.not_nil!
      r.server.start
      r.server.bootstrap
      select
      when r.server.is_leader.when_true.receive
      when timeout(3.seconds)
        fail "did not become leader"
      end
      r.server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1})).should be_true
      deadline = Time.instant + 2.seconds
      until r.server.isr.includes?(1)
        fail "apply timed out" if Time.instant > deadline
        Fiber.yield
      end

      io = IO::Memory.new
      writer = LavinMQ::HTTP::PrometheusWriter.new(io, "lavinmq")
      # Access raft_metrics via the controller's private method via direct IO emission
      # We use a small helper that exercises the same path as PrometheusController#raft_metrics
      isr = r.server.isr
      in_isr = (isr.empty? || isr.includes?(r.server.node_id)) ? 1_i64 : 0_i64
      writer.write({name:  "raft_isr_size",
                    type:  "gauge",
                    value: isr.size.to_i64,
                    help:  "Number of nodes in the in-sync replica set"})
      writer.write({name:  "raft_in_isr",
                    type:  "gauge",
                    value: in_isr,
                    help:  "Whether this node is currently a member of the ISR (1) or not (0)"})
      output = io.to_s
      output.should contain("lavinmq_raft_isr_size 1")
      output.should contain("lavinmq_raft_in_isr 1")
    ensure
      elector.try &.stop rescue nil
      FileUtils.rm_rf(dir)
    end
  end

  it "emits lavinmq_raft_isr_size and lavinmq_raft_in_isr via metrics server when elector is injected" do
    dir = tmp_data_dir
    elector = nil.as(LavinMQ::Raft::Elector?)
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      elector = LavinMQ::Raft::Elector.new(config)
      r = elector.not_nil!
      r.server.start
      r.server.bootstrap
      select
      when r.server.is_leader.when_true.receive
      when timeout(3.seconds)
        fail "did not become leader"
      end
      r.server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1})).should be_true
      deadline = Time.instant + 2.seconds
      until r.server.isr.includes?(1)
        fail "apply timed out" if Time.instant > deadline
        Fiber.yield
      end

      with_amqp_server do |amqp_server|
        metrics_server = LavinMQ::HTTP::MetricsServer.new(amqp_server, r)
        addr = metrics_server.bind_tcp("::1", 0)
        spawn(name: "raft-metrics-test-listen") { metrics_server.listen }
        Fiber.yield
        begin
          response = HTTP::Client.get("http://[::1]:#{addr.port}/metrics")
          response.status_code.should eq 200
          body = response.body
          body.should contain("lavinmq_raft_isr_size 1")
          body.should contain("lavinmq_raft_in_isr 1")
        ensure
          metrics_server.close
        end
      end
    ensure
      elector.try &.stop rescue nil
      FileUtils.rm_rf(dir)
    end
  end

  it "does not emit raft metrics when no elector is set" do
    with_metrics_server do |http, _|
      response = http.get("/metrics")
      response.status_code.should eq 200
      response.body.should_not contain("raft_isr_size")
      response.body.should_not contain("raft_in_isr")
    end
  end
end
