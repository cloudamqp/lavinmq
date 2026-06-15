require "../spec_helper"
require "http/server"
require "../../src/lavinmqctl/cli"
require "../../src/lavinmq/data_dir_lock"

describe "LavinMQCtl raft_*" do
  describe "raft_status" do
    it "GETs /raft/status from local node and pretty-prints the result" do
      stub = HTTP::Server.new do |context|
        if context.request.path == "/raft/status"
          context.response.status_code = 200
          context.response.content_type = "application/json"
          context.response.print({
            "id"             => 7_i64,
            "role"           => "leader",
            "term"           => 4_i64,
            "leader_id"      => 7_i64,
            "commit_index"   => 12_i64,
            "last_log_index" => 12_i64,
            "peers"          => [{"id" => 7_i64, "role" => "voter"}, {"id" => 8_i64, "role" => "voter"}],
          }.to_json)
        else
          context.response.status_code = 404
        end
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-raft-status") { stub.listen }
      begin
        stdout = IO::Memory.new
        original_argv = ARGV.dup
        begin
          ARGV.clear
          ARGV.concat(["--uri", "http://#{addr}", "raft_status"])
          cli = LavinMQCtl.new(stdout)
          cli.run_cmd
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end
        out = stdout.to_s
        out.should contain("leader")
        out.should contain("Term:")
        out.should contain("7") # node id
      ensure
        stub.close
      end
    end
  end

  describe "raft_reset" do
    it "refuses without --force when node is in a multi-peer cluster" do
      stub = HTTP::Server.new do |context|
        if context.request.path == "/raft/status"
          context.response.status_code = 200
          context.response.content_type = "application/json"
          context.response.print({
            "id"             => 1_i64,
            "role"           => "leader",
            "term"           => 1_i64,
            "leader_id"      => 1_i64,
            "commit_index"   => 1_i64,
            "last_log_index" => 1_i64,
            "peers"          => [
              {"id" => 1_i64, "role" => "voter"},
              {"id" => 2_i64, "role" => "voter"},
            ],
          }.to_json)
        end
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-reset-multi") { stub.listen }
      begin
        data_dir = File.tempname("raft-reset-multi-spec")
        begin
          Dir.mkdir_p(data_dir)
          File.write(File.join(data_dir, ".clustering_id"), "1")
          # Hold the data dir lock, as a running server would.
          lock = LavinMQ::DataDirLock.new(data_dir)
          lock.try_acquire.should be_true

          stdout = IO::Memory.new
          original_argv = ARGV.dup
          begin
            ARGV.clear
            ARGV.concat(["--uri", "http://#{addr}", "raft_reset",
                         "--data-dir=#{data_dir}"])
            cli = LavinMQCtl.new(stdout)
            expect_raises(LavinMQCtl::CtlExit) do
              cli.run_cmd
            end
          ensure
            ARGV.clear
            ARGV.concat(original_argv)
            lock.release
          end
          # State should remain untouched (refusal happened before signal).
          File.exists?(File.join(data_dir, ".clustering_id")).should be_true
          stdout.to_s.should contain("refusing")
        ensure
          FileUtils.rm_rf(data_dir)
        end
      ensure
        stub.close
      end
    end

    it "refuses without --force when the running node is a follower (control socket answers 503)" do
      stub = HTTP::Server.new do |context|
        # A follower's control socket answers 503 to everything.
        context.response.status_code = 503
        context.response.print "This node is a follower"
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-reset-follower") { stub.listen }
      begin
        data_dir = File.tempname("raft-reset-follower-spec")
        begin
          Dir.mkdir_p(File.join(data_dir, "raft"))
          File.write(File.join(data_dir, "raft", "raft_meta"), "fake meta")
          lock = LavinMQ::DataDirLock.new(data_dir)
          lock.try_acquire.should be_true

          stdout = IO::Memory.new
          original_argv = ARGV.dup
          begin
            ARGV.clear
            ARGV.concat(["--uri", "http://#{addr}", "raft_reset",
                         "--data-dir=#{data_dir}"])
            cli = LavinMQCtl.new(stdout)
            expect_raises(LavinMQCtl::CtlExit) do
              cli.run_cmd
            end
          ensure
            ARGV.clear
            ARGV.concat(original_argv)
            lock.release
          end
          Dir.exists?(File.join(data_dir, "raft")).should be_true
          stdout.to_s.should contain("--force")
        ensure
          FileUtils.rm_rf(data_dir)
        end
      ensure
        stub.close
      end
    end

    it "refuses to touch a data dir locked from another host" do
      stub = HTTP::Server.new do |context|
        context.response.status_code = 200
        context.response.content_type = "application/json"
        context.response.print({
          "id"    => 1_i64,
          "role"  => "leader",
          "peers" => [{"id" => 1_i64, "role" => "voter"}],
        }.to_json)
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-reset-otherhost") { stub.listen }
      begin
        data_dir = File.tempname("raft-reset-otherhost-spec")
        begin
          Dir.mkdir_p(File.join(data_dir, "raft"))
          File.write(File.join(data_dir, "raft", "raft_meta"), "fake meta")
          lock = LavinMQ::DataDirLock.new(data_dir)
          lock.try_acquire.should be_true
          # Simulate a holder on a shared (NFS) data dir: the flock is held,
          # but the recorded holder lives on another machine.
          File.write(File.join(data_dir, ".lock"), "PID 4711 @ another-host")

          stdout = IO::Memory.new
          original_argv = ARGV.dup
          begin
            ARGV.clear
            ARGV.concat(["--uri", "http://#{addr}", "raft_reset",
                         "--data-dir=#{data_dir}"])
            cli = LavinMQCtl.new(stdout)
            expect_raises(LavinMQCtl::CtlExit) do
              cli.run_cmd
            end
          ensure
            ARGV.clear
            ARGV.concat(original_argv)
            lock.release
          end
          Dir.exists?(File.join(data_dir, "raft")).should be_true
          stdout.to_s.should contain("another host")
        ensure
          FileUtils.rm_rf(data_dir)
        end
      ensure
        stub.close
      end
    end

    it "wipes raft state directories but preserves .clustering_id and other files" do
      data_dir = File.tempname("raft-reset-spec")
      begin
        Dir.mkdir_p(File.join(data_dir, "raft"))
        File.write(File.join(data_dir, "raft", "snapshot"), "fake snapshot")
        File.write(File.join(data_dir, "raft", "raft_meta"), "fake meta")
        Dir.mkdir_p(File.join(data_dir, "raft-transport"))
        File.write(File.join(data_dir, "raft-transport", "transport_peers"), "fake peers")
        File.write(File.join(data_dir, ".clustering_id"), "1")
        sentinel = File.join(data_dir, "queue-data.bin")
        File.write(sentinel, "amqp messages")

        stdout = IO::Memory.new
        original_argv = ARGV.dup
        begin
          ARGV.clear
          # --force: no node is running here, so the safety check can't reach
          # /raft/status and would otherwise fail closed.
          ARGV.concat(["raft_reset", "--data-dir=#{data_dir}", "--force"])
          cli = LavinMQCtl.new(stdout)
          cli.run_cmd
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end

        Dir.exists?(File.join(data_dir, "raft")).should be_false
        Dir.exists?(File.join(data_dir, "raft-transport")).should be_false
        File.exists?(File.join(data_dir, ".clustering_id")).should be_true
        File.exists?(sentinel).should be_true
      ensure
        FileUtils.rm_rf(data_dir)
      end
    end

    it "refuses to wipe a stopped node that can't be verified without --force" do
      data_dir = File.tempname("raft-reset-stopped-spec")
      begin
        Dir.mkdir_p(File.join(data_dir, "raft"))
        File.write(File.join(data_dir, "raft", "raft_meta"), "fake meta")
        File.write(File.join(data_dir, ".clustering_id"), "1")

        stdout = IO::Memory.new
        original_argv = ARGV.dup
        begin
          ARGV.clear
          # No node is running (closed port): its cluster membership can't be
          # verified, so the reset must fail closed rather than silently wipe.
          ARGV.concat(["--uri", "http://127.0.0.1:1", "raft_reset", "--data-dir=#{data_dir}"])
          cli = LavinMQCtl.new(stdout)
          expect_raises(LavinMQCtl::CtlExit) do
            cli.run_cmd
          end
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end

        Dir.exists?(File.join(data_dir, "raft")).should be_true
        stdout.to_s.should contain("--force")
      ensure
        FileUtils.rm_rf(data_dir)
      end
    end

    it "refuses without --force when /raft/status omits the peer list" do
      stub = HTTP::Server.new do |context|
        context.response.status_code = 200
        context.response.content_type = "application/json"
        # 200 but no "peers" — cluster size is unknown, so we must not assume 0.
        context.response.print({"id" => 1_i64, "role" => "leader"}.to_json)
      end
      addr = stub.bind_tcp("127.0.0.1", 0)
      spawn(name: "stub-reset-nopeers") { stub.listen }
      begin
        data_dir = File.tempname("raft-reset-nopeers-spec")
        begin
          Dir.mkdir_p(File.join(data_dir, "raft"))
          File.write(File.join(data_dir, "raft", "raft_meta"), "fake meta")

          stdout = IO::Memory.new
          original_argv = ARGV.dup
          begin
            ARGV.clear
            ARGV.concat(["--uri", "http://#{addr}", "raft_reset", "--data-dir=#{data_dir}"])
            cli = LavinMQCtl.new(stdout)
            expect_raises(LavinMQCtl::CtlExit) do
              cli.run_cmd
            end
          ensure
            ARGV.clear
            ARGV.concat(original_argv)
          end

          Dir.exists?(File.join(data_dir, "raft")).should be_true
          stdout.to_s.should contain("peer list")
        ensure
          FileUtils.rm_rf(data_dir)
        end
      ensure
        stub.close
      end
    end
  end

  describe "raft_join" do
    it "wipes raft state and writes .join_target with leader URI" do
      data_dir = File.tempname("raft-join-spec")
      begin
        Dir.mkdir_p(File.join(data_dir, "raft"))
        File.write(File.join(data_dir, "raft", "snapshot"), "fake")
        File.write(File.join(data_dir, ".clustering_id"), "1")

        stdout = IO::Memory.new
        original_argv = ARGV.dup
        begin
          ARGV.clear
          ARGV.concat(["raft_join", "http://leader.example:15672", "--data-dir=#{data_dir}", "--force"])
          cli = LavinMQCtl.new(stdout)
          cli.run_cmd
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end

        Dir.exists?(File.join(data_dir, "raft")).should be_false
        File.exists?(File.join(data_dir, ".clustering_id")).should be_true
        marker = File.join(data_dir, ".join_target")
        File.exists?(marker).should be_true
        File.read(marker).strip.should eq "http://leader.example:15672"
      ensure
        FileUtils.rm_rf(data_dir)
      end
    end

    it "rejects invalid URIs" do
      data_dir = File.tempname("raft-join-bad-spec")
      begin
        Dir.mkdir_p(data_dir)
        stdout = IO::Memory.new
        original_argv = ARGV.dup
        begin
          ARGV.clear
          ARGV.concat(["raft_join", "ftp://wrong", "--data-dir=#{data_dir}"])
          cli = LavinMQCtl.new(stdout)
          expect_raises(LavinMQCtl::CtlExit) do
            cli.run_cmd
          end
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end
        File.exists?(File.join(data_dir, ".join_target")).should be_false
      ensure
        FileUtils.rm_rf(data_dir)
      end
    end
  end
end
