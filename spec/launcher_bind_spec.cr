require "./spec_helper"
require "../src/lavinmq/launcher"

class LavinMQ::Launcher
  def start_for_spec
    start
  end
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
        launcher.not_nil!.start_for_spec
      end
    ensure
      launcher.try &.stop
    end
  ensure
    blocker.try &.close
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

describe LavinMQ::Launcher do
  it "binds the metrics port of a standalone node only once it has the data dir lock" do
    with_datadir do |data_dir|
      # Another instance holding the shared data dir
      holder = LavinMQ::DataDirLock.new(data_dir).tap &.acquire
      metrics_port = TCPServer.open("127.0.0.1", 0, &.local_address.port)
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
      config.amqp_port = config.http_port = config.mqtt_port = 0
      config.amqps_port = config.https_port = config.mqtts_port = -1
      config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
      config.metrics_http_bind = "127.0.0.1"
      config.metrics_http_port = metrics_port
      config.control_unix_path = File.join(data_dir, "control.sock")
      launcher = LavinMQ::Launcher.new(config)
      spawn(name: "launcher spec") { launcher.run }
      sleep 200.milliseconds
      # A standby has nothing to report and may share the host
      TCPServer.open("127.0.0.1", metrics_port) { }
      holder.release
      wait_for { (HTTP::Client.get("http://127.0.0.1:#{metrics_port}/metrics").body rescue "").includes? "lavinmq_uptime" }
    ensure
      launcher.try &.stop
    end
  end
end
