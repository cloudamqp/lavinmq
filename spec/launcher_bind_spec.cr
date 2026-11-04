require "./spec_helper"
require "../src/lavinmq/launcher"

# Without listeners other than the lavinmqctl socket
private def standalone_config(data_dir) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.data_dir_lock = false
  config.amqp_port = config.amqps_port = config.http_port = config.https_port = -1
  config.mqtt_port = config.mqtts_port = config.metrics_http_port = -1
  config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
  config.control_unix_path = File.join(data_dir, "control.sock")
  config
end

describe LavinMQ::Launcher do
  it "aborts startup when the AMQP listener can not bind" do
    blocker = TCPServer.new("127.0.0.1", 0)
    with_datadir do |data_dir|
      launcher : LavinMQ::Launcher? = nil
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.data_dir_lock = false
      config.amqp_bind = "127.0.0.1"
      config.amqp_port = blocker.local_address.port
      config.amqps_port = -1
      config.http_port = -1
      config.https_port = -1
      config.mqtt_port = -1
      config.mqtts_port = -1
      config.metrics_http_port = -1
      config.control_unix_path = File.join(data_dir, "control.sock")
      launcher = LavinMQ::Launcher.new(config)

      expect_raises(SpecExit, /Exiting with code 1/) do
        launcher.run
      end
    ensure
      launcher.try &.stop
    end
  ensure
    blocker.try &.close
  end

  it "serves the API over the lavinmqctl socket, and removes it when stopped" do
    with_datadir do |data_dir|
      config = standalone_config(data_dir)
      launcher = LavinMQ::Launcher.new(config)
      done = Channel(Nil).new
      spawn(name: "launcher spec") do
        launcher.run
      ensure
        done.close
      end
      # A 503 until the broker serves
      wait_for do
        HTTP::Client.new(UNIXSocket.new(config.control_unix_path)).get("/api/whoami").status_code == 200
      rescue Socket::Error
        false
      end
      launcher.stop
      select
      when done.receive?
      when timeout(10.seconds)
        fail "launcher didn't stop"
      end
      File.exists?(config.control_unix_path).should be_false
    end
  end

  it "aborts when another process serves the lavinmqctl socket" do
    with_datadir do |data_dir|
      config = standalone_config(data_dir)
      other = UNIXServer.new(config.control_unix_path)
      launcher = LavinMQ::Launcher.new(config)
      expect_raises(SpecExit, /Exiting with code 1/) { launcher.run }
      File.exists?(config.control_unix_path).should be_true # left to the process serving it
    ensure
      other.try &.close
    end
  end

  it "exits when the data directory is locked by another process" do
    with_datadir do |data_dir|
      lock = LavinMQ::DataDirLock.new(data_dir)
      lock.acquire
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      expect_raises(SpecExit, /Exiting with code 1/) do
        LavinMQ::Launcher.new(config)
      end
    ensure
      lock.try &.release
    end
  end
end

describe LavinMQ::DataDirLock do
  it "raises when the lock is already held" do
    with_datadir do |data_dir|
      first = LavinMQ::DataDirLock.new(data_dir)
      first.acquire
      expect_raises(LavinMQ::DataDirLock::Error, /Data directory locked by 'PID #{Process.pid}/) do
        LavinMQ::DataDirLock.new(data_dir).acquire
      end
      first.release
      second = LavinMQ::DataDirLock.new(data_dir)
      second.acquire
      second.release
    end
  end
end
