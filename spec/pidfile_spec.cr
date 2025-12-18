require "./spec_helper"
require "../src/lavinmq/pidfile"

describe LavinMQ::Pidfile do
  describe "#check_state" do
    it "returns Empty for empty file" do
      File.tempfile("pidfile", ".pid") do |file|
        pidfile = LavinMQ::Pidfile.new(file.path)
        pidfile.check_state(file).should eq LavinMQ::Pidfile::State::Empty
      end
    end

    it "returns Invalid for non-numeric content" do
      File.tempfile("pidfile", ".pid") do |file|
        file.print("notanumber")
        file.flush
        pidfile = LavinMQ::Pidfile.new(file.path)
        pidfile.check_state(file).should eq LavinMQ::Pidfile::State::Invalid
      end
    end

    it "returns Running for running process" do
      File.tempfile("pidfile", ".pid") do |file|
        file.print(Process.pid)
        file.flush
        pidfile = LavinMQ::Pidfile.new(file.path)
        pidfile.check_state(file).should eq LavinMQ::Pidfile::State::Running
      end
    end

    it "returns Stale for non-existent process" do
      File.tempfile("pidfile", ".pid") do |file|
        file.print("99999")
        file.flush
        pidfile = LavinMQ::Pidfile.new(file.path)
        pidfile.check_state(file).should eq LavinMQ::Pidfile::State::Stale
      end
    end
  end

  describe "#acquire" do
    it "creates pidfile with current PID" do
      path = File.tempname("pidfile", ".pid")
      begin
        pidfile = LavinMQ::Pidfile.new(path)
        pidfile.acquire.should be_true
        File.read(path).should eq Process.pid.to_s
      ensure
        pidfile.try &.release
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
        pidfile.try &.release
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
        pidfile1.try &.release
        File.delete?(path)
      end
    end

    it "returns false for empty path" do
      pidfile = LavinMQ::Pidfile.new("")
      pidfile.acquire.should be_false
    end
  end

  describe "#release" do
    it "deletes the pidfile" do
      path = File.tempname("pidfile", ".pid")
      pidfile = LavinMQ::Pidfile.new(path)
      pidfile.acquire.should be_true
      File.exists?(path).should be_true
      pidfile.release
      File.exists?(path).should be_false
    end

    it "does nothing if not acquired" do
      path = File.tempname("pidfile", ".pid")
      File.write(path, "99999")
      pidfile = LavinMQ::Pidfile.new(path)
      # Don't call acquire
      pidfile.release
      File.exists?(path).should be_true # file should still exist
      File.delete?(path)
    end
  end
end
