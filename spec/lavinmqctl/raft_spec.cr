require "../spec_helper"
require "http/server"
require "../../src/lavinmqctl/cli"

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
        pidfile = File.tempname("raft-reset-multi-pid")
        begin
          Dir.mkdir_p(data_dir)
          # Write our own pid into the pidfile so liveness check returns true.
          File.write(pidfile, Process.pid.to_s)
          File.write(File.join(data_dir, ".clustering_id"), "1")

          stdout = IO::Memory.new
          original_argv = ARGV.dup
          begin
            ARGV.clear
            ARGV.concat(["--uri", "http://#{addr}", "raft_reset",
                         "--data-dir=#{data_dir}", "--pidfile=#{pidfile}"])
            cli = LavinMQCtl.new(stdout)
            expect_raises(LavinMQCtl::CtlExit) do
              cli.run_cmd
            end
          ensure
            ARGV.clear
            ARGV.concat(original_argv)
          end
          # State should remain untouched (refusal happened before signal).
          File.exists?(File.join(data_dir, ".clustering_id")).should be_true
          stdout.to_s.should contain("refusing")
        ensure
          File.delete?(pidfile)
          FileUtils.rm_rf(data_dir)
        end
      ensure
        stub.close
      end
    end

    it "wipes raft state directories and .clustering_id; leaves other files" do
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
          ARGV.concat(["raft_reset", "--data-dir=#{data_dir}"])
          cli = LavinMQCtl.new(stdout)
          cli.run_cmd
        ensure
          ARGV.clear
          ARGV.concat(original_argv)
        end

        Dir.exists?(File.join(data_dir, "raft")).should be_false
        Dir.exists?(File.join(data_dir, "raft-transport")).should be_false
        File.exists?(File.join(data_dir, ".clustering_id")).should be_false
        File.exists?(sentinel).should be_true
      ensure
        FileUtils.rm_rf(data_dir)
      end
    end
  end

  it "recognizes raft_join as a valid command" do
    stdout_capture = IO::Memory.new
    original_argv = ARGV.dup
    begin
      ARGV.clear
      ARGV.concat(["raft_join", "http://leader:15672"])
      cli = LavinMQCtl.new(stdout_capture)
      expect_raises(Exception, "raft_join not implemented") do
        cli.run_cmd
      end
    ensure
      ARGV.clear
      ARGV.concat(original_argv)
    end
  end
end
