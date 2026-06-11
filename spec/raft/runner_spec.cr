require "../spec_helper"
require "file_utils"
require "http/server"
require "json"
require "../../src/lavinmq/raft/runner"
require "../../src/lavinmq/http/raft_handler_wrapper"

private def tmp_data_dir : String
  dir = File.tempname("raft-runner-spec")
  Dir.mkdir_p(dir)
  dir
end

private def free_port : Int32
  TCPServer.open("127.0.0.1", 0, &.local_address.port)
end

private def runner_config(dir : String, raft_port : Int32, data_port : Int32) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = dir
  config.clustering_bind = "127.0.0.1"
  config.clustering_raft_port = raft_port
  config.clustering_port = data_port
  config.clustering_advertised_uri = "tcp://127.0.0.1:#{data_port}"
  config
end

private def retry_until(timeout : Time::Span = 2.seconds, &block : -> Bool)
  deadline = Time.instant + timeout
  until block.call
    fail "operation timed out" if Time.instant > deadline
    Fiber.yield
  end
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
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      runner = LavinMQ::Raft::Runner.new(config)
      spawn(name: "runner-skipbootstrap-test") do
        begin
          runner.not_nil!.run { Fiber.yield }
        rescue
          # perform_join will fail to connect; that's fine
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

  it "preserves .join_target when perform_join raises" do
    dir = tmp_data_dir
    runner = nil.as(LavinMQ::Raft::Runner?)
    begin
      File.write(File.join(dir, ".join_target"), "http://127.0.0.1:1")
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_bind = "127.0.0.1"
      config.clustering_raft_port = 0
      config.clustering_port = 0
      config.clustering_advertised_uri = "tcp://127.0.0.1:0"
      runner = LavinMQ::Raft::Runner.new(config)
      # Use an invalid scheme so perform_join raises IMMEDIATELY without the 30-attempt retry loop.
      expect_raises(Exception, /invalid leader URI scheme/) do
        runner.not_nil!.perform_join("garbage://target")
      end
      File.exists?(File.join(dir, ".join_target")).should be_true
    ensure
      runner.try &.stop rescue nil
      FileUtils.rm_rf(dir)
    end
  end

  describe "in_isr?" do
    it "returns true when ISR is empty (fresh bootstrap)" do
      dir = tmp_data_dir
      runner = nil.as(LavinMQ::Raft::Runner?)
      begin
        File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
        config = LavinMQ::Config.new
        config.data_dir = dir
        config.clustering_bind = "127.0.0.1"
        config.clustering_raft_port = 0
        config.clustering_port = 0
        config.clustering_advertised_uri = "tcp://127.0.0.1:0"
        runner = LavinMQ::Raft::Runner.new(config)
        runner.not_nil!.in_isr?.should be_true
      ensure
        runner.try &.stop rescue nil
        FileUtils.rm_rf(dir)
      end
    end

    it "returns true when ISR includes our node_id" do
      dir = tmp_data_dir
      runner = nil.as(LavinMQ::Raft::Runner?)
      begin
        File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
        config = LavinMQ::Config.new
        config.data_dir = dir
        config.clustering_bind = "127.0.0.1"
        config.clustering_raft_port = 0
        config.clustering_port = 0
        config.clustering_advertised_uri = "tcp://127.0.0.1:0"
        runner = LavinMQ::Raft::Runner.new(config)
        r = runner.not_nil!
        r.server.start
        r.server.bootstrap
        select
        when r.server.is_leader.when_true.receive
        when timeout(2.seconds)
          fail "did not become leader"
        end
        r.server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1})).should be_true
        deadline = Time.instant + 2.seconds
        until r.server.isr.includes?(1)
          fail "apply timed out" if Time.instant > deadline
          Fiber.yield
        end
        r.in_isr?.should be_true
      ensure
        runner.try &.stop rescue nil
        FileUtils.rm_rf(dir)
      end
    end

    it "returns false when ISR is non-empty and excludes our node_id" do
      dir = tmp_data_dir
      runner = nil.as(LavinMQ::Raft::Runner?)
      begin
        File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
        config = LavinMQ::Config.new
        config.data_dir = dir
        config.clustering_bind = "127.0.0.1"
        config.clustering_raft_port = 0
        config.clustering_port = 0
        config.clustering_advertised_uri = "tcp://127.0.0.1:0"
        runner = LavinMQ::Raft::Runner.new(config)
        r = runner.not_nil!
        r.server.start
        r.server.bootstrap
        select
        when r.server.is_leader.when_true.receive
        when timeout(2.seconds)
          fail "did not become leader"
        end
        r.server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{99})).should be_true
        deadline = Time.instant + 2.seconds
        until r.server.isr.includes?(99)
          fail "apply timed out" if Time.instant > deadline
          Fiber.yield
        end
        r.in_isr?.should be_false
      ensure
        runner.try &.stop rescue nil
        FileUtils.rm_rf(dir)
      end
    end
  end

  describe "in-sync leadership gate" do
    it "hands leadership back to an in-sync voter when elected while not in ISR, without serving" do
      a_dir = tmp_data_dir
      b_dir = tmp_data_dir
      File.write(File.join(a_dir, ".clustering_id"), 1.to_s(36))
      File.write(File.join(b_dir, ".clustering_id"), 2.to_s(36))
      runner_a = nil.as(LavinMQ::Raft::Runner?)
      runner_b = nil.as(LavinMQ::Raft::Runner?)
      admin = nil.as(HTTP::Server?)
      begin
        a = LavinMQ::Raft::Runner.new(runner_config(a_dir, free_port, free_port))
        runner_a = a
        b = LavinMQ::Raft::Runner.new(runner_config(b_dir, free_port, free_port))
        runner_b = b

        a.transport.start
        a.server.start
        a.server.bootstrap.should be_true
        select
        when a.server.is_leader.when_true.receive
        when timeout(5.seconds)
          fail "node A did not become leader after bootstrap"
        end

        # Only A is in ISR — B's data plane is out of sync
        a.server.propose(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1})).should be_true

        # Serve A's raft admin endpoint so B can join through the real path
        inner = ::Raft::HTTP::Handler(LavinMQ::Raft::ClusterCommand).new(
          a.server.node, a.transport, a.advertised_address)
        admin = HTTP::Server.new([LavinMQ::HTTP::RaftHandlerWrapper.new(inner)])
        admin_addr = admin.not_nil!.bind_tcp("127.0.0.1", 0)
        spawn(name: "stub-admin") { admin.not_nil!.listen }

        File.write(File.join(b_dir, ".join_target"), "http://#{admin_addr}")
        b_served = false
        spawn(name: "runner-b") do
          b.run { b_served = true }
        rescue ::Channel::ClosedError
          # closed at cleanup
        end

        # B joins, replicates the config and ISR, and is auto-promoted to voter
        retry_until(10.seconds) { a.server.voters.includes?(2_u64) }
        retry_until(5.seconds) { b.server.isr.includes?(1) }

        # Force the out-of-sync node to win the raft election
        retry_until(5.seconds) { a.server.transfer_leadership(to: 2) }

        # B must win the election, refuse to serve, and hand leadership back
        # to A — the only in-sync voter.
        b_was_leader = false
        retry_until(10.seconds) do
          b_was_leader ||= b.server.is_leader.value
          b_was_leader && a.server.is_leader.value
        end
        b.server.is_leader.value.should be_false
        b_served.should be_false
      ensure
        admin.try &.close rescue nil
        runner_b.try &.stop rescue nil
        runner_a.try &.stop rescue nil
        FileUtils.rm_rf(a_dir)
        FileUtils.rm_rf(b_dir)
      end
    end
  end

  describe "perform_join" do
    it "retries on 5xx until success" do
      attempts = 0
      stub = HTTP::Server.new do |context|
        attempts += 1
        if attempts < 3
          context.response.status_code = 503
          context.response.print "busy"
        else
          context.response.status_code = 200
          context.response.print({"status" => "added"}.to_json)
        end
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-flaky-leader") { stub.listen }
      runner = nil.as(LavinMQ::Raft::Runner?)
      begin
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
          runner.not_nil!.perform_join("http://#{addr}")
          attempts.should eq 3
        ensure
          FileUtils.rm_rf(dir)
        end
      ensure
        runner.try &.stop rescue nil
        stub.close
      end
    end

    it "POSTs to leader's /raft/admin/add_server/<id> with own address as JSON body" do
      received_path = nil.as(String?)
      received_body = nil.as(String?)
      stub = HTTP::Server.new do |context|
        received_path = context.request.path
        received_body = context.request.body.try(&.gets_to_end)
        context.response.status_code = 200
        context.response.content_type = "application/json"
        context.response.print({"status" => "added"}.to_json)
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-leader") { stub.listen }
      runner = nil.as(LavinMQ::Raft::Runner?)
      begin
        dir = tmp_data_dir
        begin
          File.write(File.join(dir, ".clustering_id"), 42.to_s(36))
          config = LavinMQ::Config.new
          config.data_dir = dir
          config.clustering_bind = "127.0.0.1"
          config.clustering_raft_port = 0
          config.clustering_port = 0
          config.clustering_advertised_uri = "tcp://127.0.0.1:0"
          runner = LavinMQ::Raft::Runner.new(config)
          runner.not_nil!.perform_join("http://#{addr}")
          received_path.should eq "/raft/admin/add_server/42"
          received_body.should_not be_nil
          parsed = JSON.parse(received_body.not_nil!)
          parsed["address"].as_s.should_not be_empty
        ensure
          FileUtils.rm_rf(dir)
        end
      ensure
        runner.try &.stop rescue nil
        stub.close
      end
    end
  end
end
