require "./spec_helper"
require "file_utils"
require "../src/lavinmq/data_dir_lock"

describe LavinMQ::DataDirLock do
  describe "#try_acquire" do
    it "acquires a free lock and records the holder" do
      dir = File.tempname("lock-spec")
      Dir.mkdir_p(dir)
      begin
        lock = LavinMQ::DataDirLock.new(dir)
        lock.try_acquire.should be_true
        File.read(File.join(dir, ".lock")).should contain("PID #{Process.pid}")
        lock.release
      ensure
        FileUtils.rm_rf(dir)
      end
    end

    it "returns false while another holder has the lock, true after release" do
      dir = File.tempname("lock-spec")
      Dir.mkdir_p(dir)
      begin
        holder = LavinMQ::DataDirLock.new(dir)
        holder.try_acquire.should be_true
        # flock is per open file description, so a second instance contends
        # even within the same process — same as another process would.
        contender = LavinMQ::DataDirLock.new(dir)
        contender.try_acquire.should be_false
        holder.release
        contender.try_acquire.should be_true
        contender.release
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end

  describe "#holder_info" do
    it "reads what the holder wrote" do
      dir = File.tempname("lock-spec")
      Dir.mkdir_p(dir)
      begin
        holder = LavinMQ::DataDirLock.new(dir)
        holder.try_acquire.should be_true
        contender = LavinMQ::DataDirLock.new(dir)
        contender.holder_info.should eq "PID #{Process.pid} @ #{System.hostname}"
        holder.release
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end
end
