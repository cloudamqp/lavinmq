require "./spec_helper"

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
        mqtt_server = LavinMQ::MQTT::Server.new(s)
        h = LavinMQ::HTTP::Server.new(s, amqp(s), mqtt_server)
        begin
          h.bind_internal_unix
          spawn(name: "control socket listen") { h.listen }
          Fiber.yield
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
        ensure
          h.close
          mqtt_server.close
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
        mqtt_server = LavinMQ::MQTT::Server.new(s)
        h = LavinMQ::HTTP::Server.new(s, amqp(s), mqtt_server) # captures socket_path
        begin
          h.bind_internal_unix
          spawn(name: "control socket listen") { h.listen }
          Fiber.yield
          # Simulate a SIGHUP reload that changes the configured path
          config.control_unix_path = File.tempname("lavinmqctl-reloaded", ".sock")
          client = HTTP::Client.new(UNIXSocket.new(socket_path))
          response = client.get("/api/whoami")
          response.status_code.should eq 200
          response.body.should contain "__direct"
        ensure
          h.close
          mqtt_server.close
        end
      end
    ensure
      config.control_unix_path = original_path
      File.delete?(socket_path)
    end
  end
end
