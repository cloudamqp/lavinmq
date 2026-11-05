require "./spec_helper"
require "../src/lavinmq/http/control_socket"

# Serves *http_server*'s API on a control socket at *path*, as the Launcher
# does while the node serves
private def with_control_api(http_server, path, &)
  socket = LavinMQ::HTTP::ControlSocket.new(path)
  socket.bind!
  socket.api = http_server.handler
  yield
ensure
  socket.try &.close
end

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
        with_control_api(s.http_server, socket_path) do
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
          client.close
        end
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  it "lets only the owner and its group connect" do
    path = File.tempname("lavinmqctl-spec", ".sock")
    socket = LavinMQ::HTTP::ControlSocket.new(path)
    socket.bind.should be_true
    File.info(path).permissions.value.should eq 0o660
    socket.close
    File.exists?(path).should be_false
  ensure
    socket.try &.close
    File.delete?(path) if path
  end

  it "uses the socket bound at startup even after the config is reloaded" do
    config = LavinMQ::Config.instance
    original_path = config.control_unix_path
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    config.control_unix_path = socket_path
    begin
      with_amqp_server do |s|
        h = s.http_server # captures socket_path
        with_control_api(h, socket_path) do
          # Simulate a SIGHUP reload that changes the configured path
          config.control_unix_path = File.tempname("lavinmqctl-reloaded", ".sock")
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
          client.close
        end
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  describe "ControlSocket.prepare" do
    it "does nothing if the path does not exist" do
      path = File.tempname("ctl", ".sock")
      LavinMQ::HTTP::ControlSocket.prepare(path)
      File.exists?(path).should be_false
    end

    it "raises if the path exists but is not a socket" do
      path = File.tempname("ctl", ".sock")
      File.touch(path)
      expect_raises(Exception, /not a socket/) do
        LavinMQ::HTTP::ControlSocket.prepare(path)
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
      LavinMQ::HTTP::ControlSocket.prepare(path)
      File.exists?(path).should be_false
    ensure
      File.delete?(path) if path
    end

    it "raises if the socket is in use" do
      path = File.tempname("ctl", ".sock")
      server = UNIXServer.new(path)
      expect_raises(LavinMQ::HTTP::ControlSocketInUseError, /already in use/) do
        LavinMQ::HTTP::ControlSocket.prepare(path)
      end
      File.exists?(path).should be_true
    ensure
      server.try &.close
      File.delete?(path) if path
    end
  end

  describe "ControlSocket#bind" do
    it "skips binding when another node serves the control socket" do
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      leader = UNIXServer.new(socket_path)
      socket = LavinMQ::HTTP::ControlSocket.new(socket_path)
      socket.bind.should be_false
      expect_raises(LavinMQ::HTTP::ControlSocketInUseError) { socket.bind! }
      socket.close
      File.exists?(socket_path).should be_true # left to the node serving it
    ensure
      leader.try &.close
      File.delete?(socket_path) if socket_path
    end

    it "skips binding instead of raising when the socket can not be prepared" do
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      File.touch(socket_path) # not a socket: ControlSocket.prepare raises
      LavinMQ::HTTP::ControlSocket.new(socket_path).bind.should be_false
    ensure
      File.delete?(socket_path) if socket_path
    end

    it "answers GET /api/cluster with the given cluster status while there's no API" do
      socket_path = File.tempname("lavinmqctl-spec", ".sock")
      socket = LavinMQ::HTTP::ControlSocket.new(socket_path, -> { %({"leader":null}).as(String?) })
      socket.bind.should be_true
      client = HTTP::Client.new(UNIXSocket.new(socket_path))
      response = client.get("/api/cluster")
      response.status_code.should eq 200
      response.body.should eq %({"leader":null})
      client.get("/api/queues").status_code.should eq 503
    ensure
      client.try &.close
      socket.try &.close
      File.delete?(socket_path) if socket_path
    end
  end
end
