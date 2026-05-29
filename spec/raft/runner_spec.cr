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
      config.clustering_port = 5679
      config.clustering_advertised_uri = "tcp://127.0.0.1:5679"
      runner = LavinMQ::Raft::Runner.new(config)
      runner.node_id.should eq 1
      runner.stop
    ensure
      FileUtils.rm_rf(dir)
    end
  end
end
