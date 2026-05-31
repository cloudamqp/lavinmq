require "../spec_helper"
require "../../src/lavinmqctl/cli"

describe "LavinMQCtl raft_*" do
  it "recognizes raft_status as a valid command" do
    stdout_capture = IO::Memory.new
    original_argv = ARGV.dup
    begin
      ARGV.clear
      ARGV.concat(["raft_status"])
      cli = LavinMQCtl.new(stdout_capture)
      expect_raises(Exception, "raft_status not implemented") do
        cli.run_cmd
      end
    ensure
      ARGV.clear
      ARGV.concat(original_argv)
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
