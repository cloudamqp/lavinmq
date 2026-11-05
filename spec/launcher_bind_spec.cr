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

# Test-only seam: run with the spec's runner instead of the configured one
class LavinMQ::Launcher
  def runner_for_spec=(@runner : LavinMQ::Runner)
  end
end

# Stops the launcher just before yielding to start serving, as a shutdown can
# race with a clustering controller's election. With *during_start* it stops
# it from #start's first call back into the runner instead, as a shutdown can
# arrive while the broker loads.
private class StoppingRunner < LavinMQ::StandaloneRunner
  def initialize(@stop_launcher : Proc(Nil), @during_start = false)
  end

  def run(&)
    @stop_launcher.call unless @during_start
    super { yield }
  end

  def new_replicator : LavinMQ::Clustering::Server?
    @stop_launcher.call if @during_start
    nil
  end
end

# Not the leader anymore, as the coordinator reports it
private class StaleCoordinator < LavinMQ::Clustering::Coordinator
  def update_isr(synced_node_ids : Set(Int32)) : Nil
    raise LavinMQ::Clustering::Coordinator::StaleLeadership.new("Not the leader (spec)")
  end

  def password : String
    "spec"
  end
end

# Serves with a replicator that has lost leadership, as when it's lost
# while the broker loads
private class StaleLeaderRunner < LavinMQ::StandaloneRunner
  def initialize(@config : LavinMQ::Config)
  end

  def new_replicator : LavinMQ::Clustering::Server?
    LavinMQ::Clustering::Server.new(@config, StaleCoordinator.new, 1)
  end
end

# Runs *launcher* until #run returns, when the process would exit
private def run_until_stopped(launcher : LavinMQ::Launcher) : Nil
  done = Channel(Nil).new
  spawn(name: "launcher spec") do
    launcher.run
  ensure
    done.close
  end
  select
  when done.receive?
  when timeout(10.seconds)
    fail "launcher didn't stop"
  end
end

describe LavinMQ::Launcher do
  it "doesn't start serving once stopped" do
    with_datadir do |data_dir|
      launcher = LavinMQ::Launcher.new(standalone_config(data_dir))
      launcher.runner_for_spec = StoppingRunner.new(-> { launcher.stop })
      begin
        run_until_stopped(launcher)
        launcher.@server.should be_nil
      ensure
        launcher.@server.try &.close # before the data dir is removed
      end
    end
  end

  it "closes the broker it was starting when stopped meanwhile" do
    with_datadir do |data_dir|
      launcher = LavinMQ::Launcher.new(standalone_config(data_dir))
      launcher.runner_for_spec = StoppingRunner.new(-> { launcher.stop }, during_start: true)
      begin
        run_until_stopped(launcher)
        # The process exits once #run has returned, what's open then is never closed
        server = launcher.@server.should_not be_nil
        server.closed?.should be_true
      ensure
        launcher.@server.try &.close # before the data dir is removed
      end
    end
  end

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

  # What Server.new opened before a replicated write failed couldn't be
  # closed when stopping to serve
  it "exits when it loses leadership while the broker loads" do
    with_datadir do |data_dir|
      config = standalone_config(data_dir)
      launcher = LavinMQ::Launcher.new(config)
      launcher.runner_for_spec = StaleLeaderRunner.new(config)
      exited = Channel(Int32?).new(1)
      spawn(name: "launcher spec") do
        launcher.run
        exited.send nil
      rescue ex : SpecExit
        exited.send ex.code
      end
      select
      when code = exited.receive
        code.should eq 3
      when timeout(10.seconds)
        fail "launcher didn't exit"
      end
    ensure
      launcher.try &.stop
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
