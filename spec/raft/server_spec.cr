require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/server"

private def tmp_data_dir : String
  dir = File.tempname("raft-server-spec")
  Dir.mkdir_p(dir)
  dir
end

describe LavinMQ::Raft::Server do
  describe "node_id" do
    it "generates and persists across reconstruction with the same data_dir" do
      dir = tmp_data_dir
      begin
        t1 = ::Raft::MemoryTransport.new(1_u64)
        s1 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t1,
        )
        first_id = s1.node_id
        s1.stop

        t2 = ::Raft::MemoryTransport.new(1_u64)
        s2 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t2,
        )
        s2.node_id.should eq first_id
        s2.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end
end
