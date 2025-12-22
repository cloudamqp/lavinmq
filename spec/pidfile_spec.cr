require "./spec_helper"
require "../src/lavinmq/pidfile"

describe LavinMQ::Pidfile do
  describe "#acquire" do
    it "creates pidfile with current PID" do
      path = File.tempname("pidfile", ".pid")
      begin
        pidfile = LavinMQ::Pidfile.new(path)
        pidfile.acquire.should be_true
        File.read(path).should eq Process.pid.to_s
      ensure
        File.delete?(path)
      end
    end

    it "replaces stale pidfile" do
      path = File.tempname("pidfile", ".pid")
      begin
        File.write(path, "99999")
        pidfile = LavinMQ::Pidfile.new(path)
        pidfile.acquire.should be_true
        File.read(path).should eq Process.pid.to_s
      ensure
        File.delete?(path)
      end
    end

    it "fails when pidfile contains running process" do
      path = File.tempname("pidfile", ".pid")
      begin
        File.write(path, "1") # PID 1 is always running (init/systemd)
        pidfile = LavinMQ::Pidfile.new(path)
        pidfile.acquire.should be_false
        File.read(path).should eq "1" # unchanged
      ensure
        File.delete?(path)
      end
    end

    it "fails when pidfile contains invalid data" do
      path = File.tempname("pidfile", ".pid")
      begin
        File.write(path, "notanumber")
        pidfile = LavinMQ::Pidfile.new(path)
        pidfile.acquire.should be_false
        File.read(path).should eq "notanumber" # unchanged
      ensure
        File.delete?(path)
      end
    end

    it "fails when pidfile is locked by another process" do
      path = File.tempname("pidfile", ".pid")
      begin
        # First process acquires the lock
        pidfile1 = LavinMQ::Pidfile.new(path)
        pidfile1.acquire.should be_true

        # Second process tries to acquire
        pidfile2 = LavinMQ::Pidfile.new(path)
        pidfile2.acquire.should be_false
      ensure
        File.delete?(path)
      end
    end

    it "returns false for empty path" do
      pidfile = LavinMQ::Pidfile.new("")
      pidfile.acquire.should be_false
    end

    it "removes pidfile on graceful shutdown" do
      path = File.tempname("pidfile", ".pid")
      # Spawn a subprocess that acquires the pidfile and exits gracefully
      script = <<-CRYSTAL
        require "log"
        module LavinMQ
          Log = ::Log
        end
        require "./src/lavinmq/pidfile"
        LavinMQ::Pidfile.new(#{path.inspect}).acquire
        CRYSTAL
      process = Process.new(
        "crystal", ["eval", script],
        output: Process::Redirect::Close,
        error: Process::Redirect::Close
      )
      status = process.wait
      status.success?.should be_true
      File.exists?(path).should be_false
    end
  end
end
