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

  it "recognizes raft_reset as a valid command" do
    stdout_capture = IO::Memory.new
    original_argv = ARGV.dup
    begin
      ARGV.clear
      ARGV.concat(["raft_reset"])
      cli = LavinMQCtl.new(stdout_capture)
      expect_raises(Exception, "raft_reset not implemented") do
        cli.run_cmd
      end
    ensure
      ARGV.clear
      ARGV.concat(original_argv)
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
