require "../spec_helper"

describe LavinMQ::AMQP::Server do
  it "accepts connections on a bound TCP socket" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    amqp_server = LavinMQ::AMQP::Server.new(server)
    begin
      amqp_server.bind_tcp(tcp_server)
      spawn(name: "amqp direct listen spec") { amqp_server.listen }
      wait_for { amqp_server.@listeners.includes?(tcp_server) }

      amqp_connection = AMQP::Client.new(host: "127.0.0.1", port: tcp_server.local_address.port).connect
    ensure
      amqp_connection.try &.close
      amqp_server.close
      server.close unless server.closed?
    end
  end

  it "sources url from the bound TCP listener" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    amqp_server = LavinMQ::AMQP::Server.new(server)
    begin
      amqp_server.bind_tcp(tcp_server)
      amqp_server.url.should eq "amqp://#{tcp_server.local_address}"
    ensure
      amqp_server.close
      server.close unless server.closed?
    end
  end

  it "closes idempotently" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    amqp_server = LavinMQ::AMQP::Server.new(server)
    begin
      amqp_server.bind_tcp(tcp_server)

      amqp_server.close
      amqp_server.close

      amqp_server.closed?.should be_true
      amqp_server.listening?.should be_false
      amqp_server.@listeners.empty?.should be_true
    ensure
      server.close unless server.closed?
    end
  end

  it "keeps listening state when a second listen call is rejected" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    amqp_server = LavinMQ::AMQP::Server.new(server)
    begin
      amqp_server.bind_tcp(tcp_server)
      spawn(name: "amqp duplicate listen spec") { amqp_server.listen }
      wait_for { amqp_server.listening? }

      expect_raises(Exception, "Can't start running AMQP server") do
        amqp_server.listen
      end

      amqp_server.listening?.should be_true
    ensure
      amqp_server.close
      server.close unless server.closed?
    end
  end

  it "accepts connections after the server restarts and the frontend is replaced" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    port = tcp_server.local_address.port
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    amqp_server = LavinMQ::AMQP::Server.new(server)
    begin
      amqp_server.bind_tcp(tcp_server)
      spawn(name: "amqp restart listen spec") { amqp_server.listen }
      wait_for { amqp_server.@listeners.includes?(tcp_server) }

      amqp_server.close
      restart_server(server)
      amqp_server = LavinMQ::AMQP::Server.new(server)
      tcp_server = TCPServer.new("127.0.0.1", port)
      amqp_server.bind_tcp(tcp_server)
      spawn(name: "amqp restart replacement listen spec") { amqp_server.listen }
      wait_for { amqp_server.@listeners.includes?(tcp_server) }

      amqp_connection = AMQP::Client.new(host: "127.0.0.1", port: tcp_server.local_address.port).connect
    ensure
      amqp_connection.try &.close
      amqp_server.close
      server.close unless server.closed?
    end
  end
end
