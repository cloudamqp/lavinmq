require "./spec_helper"

# Binds a ControlSocket at *path* serving the API of *s*, like the Launcher
private def control_socket(s, path) : LavinMQ::HTTP::ControlSocket
  LavinMQ::HTTP::ControlSocket.new(path).tap do |socket|
    socket.bind
    socket.api = s.http_server.handler
  end
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
        socket = control_socket(s, socket_path)
        client = HTTP::Client.new(UNIXSocket.new(socket_path))
        response = client.get("/api/whoami")
        response.status_code.should eq 200
        response.body.should contain "__direct"
        client.close
      ensure
        socket.try &.close
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  it "lets only the owner and its group connect" do
    path = File.tempname("lavinmqctl-spec", ".sock")
    socket = LavinMQ::HTTP::ControlSocket.new(path)
    socket.bind
    File.info(path).permissions.value.should eq 0o660
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
        socket = control_socket(s, socket_path) # captures socket_path
        # Simulate a SIGHUP reload that changes the configured path
        config.control_unix_path = File.tempname("lavinmqctl-reloaded", ".sock")
        client = HTTP::Client.new(UNIXSocket.new(socket_path))
        response = client.get("/api/whoami")
        response.status_code.should eq 200
        response.body.should contain "__direct"
        client.close
      ensure
        socket.try &.close
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end

  it "removes the control socket it bound when closed" do
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    socket = LavinMQ::HTTP::ControlSocket.new(socket_path)
    socket.bind
    File.exists?(socket_path).should be_true
    socket.close
    File.exists?(socket_path).should be_false
  ensure
    File.delete?(socket_path) if socket_path
  end

  it "leaves a control socket it didn't bind when closed" do
    config = LavinMQ::Config.instance
    original_path = config.control_unix_path
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    serving = UNIXServer.new(socket_path) # like another node's
    config.control_unix_path = socket_path
    begin
      with_http_server { } # listens on TCP only
      File.exists?(socket_path).should be_true
    ensure
      config.control_unix_path = original_path
      serving.close
    end
  end

  it "raises instead of binding when another node serves the control socket" do
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    leader = UNIXServer.new(socket_path)
    socket = LavinMQ::HTTP::ControlSocket.new(socket_path)
    expect_raises(LavinMQ::HTTP::ControlSocketInUseError) { socket.bind }
    socket.bound?.should be_false
  ensure
    leader.try &.close
    File.delete?(socket_path) if socket_path
  end

  it "answers GET /api/cluster with the given cluster status while not serving" do
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    socket = LavinMQ::HTTP::ControlSocket.new(socket_path, -> { %({"leader":null}).as(String?) })
    socket.bind
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

  describe "ControlSocket.prepare" do
    it "does nothing if the path does not exist" do
      path = File.tempname("ctl", ".sock")
      LavinMQ::HTTP::ControlSocket.prepare(path)
      File.exists?(path).should be_false
    end

    it "raises if the path exists but is not a socket" do
      path = File.tempname("ctl", ".sock")
      File.touch(path)
      expect_raises(LavinMQ::HTTP::ControlSocketError, /not a socket/) do
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
end
