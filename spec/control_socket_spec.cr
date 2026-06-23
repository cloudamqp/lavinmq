require "./spec_helper"
require "../src/lavinmq/clustering/client"

describe "control socket" do
  # Regression test for running multiple instances on one host: the control
  # socket path must be configurable, and both the bind and the no-auth bypass
  # must follow the configured path rather than a hardcoded one.
  it "authenticates connections over the configured control socket as the direct user" do
    config = LavinMQ::Config.instance
    original_path = config.control_unix_path
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    config.control_unix_path = socket_path
    begin
      with_amqp_server do |s|
        h = LavinMQ::HTTP::Server.new(s)
        h.bind_internal_unix
        spawn(name: "control socket listen") { h.listen }
        Fiber.yield
        begin
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
        ensure
          h.close
        end
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  it "uses the socket bound at startup even after the config is reloaded" do
    config = LavinMQ::Config.instance
    original_path = config.control_unix_path
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    config.control_unix_path = socket_path
    begin
      with_amqp_server do |s|
        h = LavinMQ::HTTP::Server.new(s) # captures socket_path
        h.bind_internal_unix
        spawn(name: "control socket listen") { h.listen }
        Fiber.yield
        # Simulate a SIGHUP reload that changes the configured path
        config.control_unix_path = File.tempname("lavinmqctl-reloaded", ".sock")
        begin
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
        ensure
          h.close
        end
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  describe "prepare_control_socket" do
    it "does nothing if the path does not exist" do
      path = File.tempname("ctl", ".sock")
      LavinMQ::HTTP::Server.prepare_control_socket(path)
      File.exists?(path).should be_false
    end

    it "raises if the path exists but is not a socket" do
      path = File.tempname("ctl", ".sock")
      File.touch(path)
      expect_raises(Exception, /not a socket/) do
        LavinMQ::HTTP::Server.prepare_control_socket(path)
      end
      File.exists?(path).should be_true
    ensure
      File.delete?(path) if path
    end

    it "deletes a stale socket no one is listening on" do
      path = File.tempname("ctl", ".sock")
      sock = Socket.unix
      sock.bind(Socket::UNIXAddress.new(path))
      sock.close
      File.info(path, follow_symlinks: false).type.socket?.should be_true
      LavinMQ::HTTP::Server.prepare_control_socket(path)
      File.exists?(path).should be_false
    ensure
      File.delete?(path) if path
    end

    it "raises if the socket is in use" do
      path = File.tempname("ctl", ".sock")
      server = UNIXServer.new(path)
      expect_raises(LavinMQ::HTTP::ControlSocketInUseError, /already in use/) do
        LavinMQ::HTTP::Server.prepare_control_socket(path)
      end
      File.exists?(path).should be_true
    ensure
      server.try &.close
      File.delete?(path) if path
    end
  end

  describe "follower_internal_socket_http_server" do
    it "skips binding when another node serves the control socket" do
      config = LavinMQ::Config.instance
      original_path = config.control_unix_path
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      config.control_unix_path = socket_path
      leader = UNIXServer.new(socket_path)
      LavinMQ::HTTP::Server.follower_internal_socket_http_server.should be_nil
    ensure
      leader.try &.close
      config.control_unix_path = original_path if config && original_path
      File.delete?(socket_path) if socket_path
    end

    it "skips binding instead of raising when the socket can not be prepared" do
      config = LavinMQ::Config.instance
      original_path = config.control_unix_path
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      config.control_unix_path = socket_path
      File.touch(socket_path) # not a socket: prepare_control_socket raises a plain Exception
      LavinMQ::HTTP::Server.follower_internal_socket_http_server.should be_nil
    ensure
      config.control_unix_path = original_path if config && original_path
      File.delete?(socket_path) if socket_path
    end

    it "binds when alone on the machine" do
      config = LavinMQ::Config.instance
      original_path = config.control_unix_path
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      config.control_unix_path = socket_path
      server = LavinMQ::HTTP::Server.follower_internal_socket_http_server
      server.should_not be_nil
      UNIXSocket.open(socket_path) { }
    ensure
      server.try &.close
      config.control_unix_path = original_path if config && original_path
      File.delete?(socket_path) if socket_path
    end
  end

  describe "clustering follower control socket" do
    it "does not bind before the leader host is known" do
      original_config = LavinMQ::Config.instance
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      config = LavinMQ::Config.new
      config.control_unix_path = socket_path
      config.metrics_http_port = -1

      begin
        LavinMQ::Config.instance = config
        with_datadir do |data_dir|
          config.data_dir = data_dir
          client : LavinMQ::Clustering::Client? = nil
          begin
            client = LavinMQ::Clustering::Client.new(config, 1, "secret", proxy: false)
            File.exists?(socket_path).should be_false
          ensure
            client.try &.close
          end
        end
      ensure
        LavinMQ::Config.instance = original_config
        File.delete?(socket_path)
      end
    end

    it "recognizes loopback leader hosts as local" do
      {"localhost", "LOCALHOST", "127.0.0.1", "127.1", "::1", "[::1]", System.hostname}.each do |host|
        LavinMQ::Clustering::Client.local_leader_host?(host).should be_true
      end

      LavinMQ::Clustering::Client.local_leader_host?("192.0.2.10").should be_false
    end
  end
end
