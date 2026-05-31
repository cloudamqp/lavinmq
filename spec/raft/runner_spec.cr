require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/runner"

private def tmp_data_dir : String
  dir = File.tempname("raft-runner-spec")
  Dir.mkdir_p(dir)
  dir
end

describe LavinMQ::Raft::Runner do
  it "constructs and stops without crashing" do
    dir = tmp_data_dir
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      runner = LavinMQ::Raft::Runner.new(config)
      runner.node_id.should eq 1
      runner.stop
    ensure
      FileUtils.rm_rf(dir)
    end
  end

  it "auto-bootstraps a fresh node with no peers and no .join_target" do
    dir = tmp_data_dir
    runner = nil
    begin
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      runner = LavinMQ::Raft::Runner.new(config)
      spawn(name: "runner-test") do
        runner.not_nil!.run { Fiber.yield }
      end
      select
      when runner.not_nil!.server.is_leader.when_true.receive
        runner.not_nil!.server.is_leader.value.should be_true
      when timeout(3.seconds)
        fail "single-node runner did not auto-bootstrap into leadership"
      end
    ensure
      runner.try &.stop rescue nil
      FileUtils.rm_rf(dir)
    end
  end

  it "skips auto-bootstrap when .join_target exists" do
    dir = tmp_data_dir
    runner = nil.as(LavinMQ::Raft::Runner?)
    begin
      File.write(File.join(dir, ".join_target"), "http://unreachable.invalid:99999")
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      runner = LavinMQ::Raft::Runner.new(config)
      spawn(name: "runner-skipbootstrap-test") do
        begin
          runner.not_nil!.run { Fiber.yield }
        rescue
          # perform_join stub will raise since not implemented; that's fine
        end
      end
      sleep 200.milliseconds
      runner.not_nil!.server.is_leader.value.should be_false
      File.exists?(File.join(dir, ".join_target")).should be_true
    ensure
      runner.try &.stop rescue nil
      FileUtils.rm_rf(dir)
    end
  end
end
