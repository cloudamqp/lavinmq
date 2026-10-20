require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/clustering/client"

# Starts a leader with an AMQPS listener and a follower proxying its AMQPS port to it,
# yields the host and the follower's AMQPS port
private def with_follower_amqps_proxy(&)
  config = LavinMQ::Config.instance
  config.clustering = true
  replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
  follower_data_dir = File.tempname
  Dir.mkdir follower_data_dir
  begin
    with_amqp_server(tls: true, replicator: replicator) do |s|
      # localhost can be ::1, so bind the follower to the address the leader listens on
      leader_tls = s.amqp_server.@tls_contexts.keys.compact_map(&.as?(TCPServer)).first.local_address
      host = leader_tls.address
      repli_server = TCPServer.new(host, 0)
      spawn(replicator.listen(repli_server), name: "repli server spec")

      # The proxy is only bound when amqps_port > 0, so reserve a free port for it
      reserved = TCPServer.new(host, 0)
      follower_amqps_port = reserved.local_address.port
      reserved.close

      follower_config = config.dup
      follower_config.data_dir = follower_data_dir
      follower_config.amqp_bind = follower_config.http_bind = follower_config.mqtt_bind = host
      follower_config.amqp_port = follower_config.http_port = follower_config.mqtt_port = 0
      follower_config.https_port = follower_config.mqtts_port = 0
      follower_config.amqps_port = follower_amqps_port
      follower_config.tls_cert_path = "spec/resources/server_certificate.pem"
      repli = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password)
      # The follower forwards to the same port on the leader, which can't be bound twice
      # on one host, so point it at the leader's port now that its listener is bound
      follower_config.amqps_port = leader_tls.port
      follower_stopped = WaitGroup.new(1)
      spawn(name: "follow spec") do
        repli.follow(host, repli_server.local_address.port)
        follower_stopped.done
      end
      begin
        wait_for { replicator.followers.size == 1 }
        yield host, follower_amqps_port
      ensure
        repli.close
        follower_stopped.wait
      end
      expect_raises(Socket::ConnectError) { TCPSocket.new(host, follower_amqps_port) }
    ensure
      replicator.close
    end
  ensure
    config.clustering = false
    FileUtils.rm_rf follower_data_dir
  end
end

# Connects to *port* over TLS, sending *proxy_header* first, and reads Connection.Start
private def amqps_start(host, port, proxy_header : Bytes? = nil) : Socket::IPAddress
  socket = TCPSocket.new(host, port)
  socket.write proxy_header if proxy_header
  client_ctx = OpenSSL::SSL::Context::Client.new
  client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  tls = OpenSSL::SSL::Socket::Client.new(socket, client_ctx, sync_close: true)
  tls.write LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
  tls.flush
  tls.read_timeout = 5.seconds
  frame = Bytes.new(7)
  tls.read_fully(frame)
  frame[0].should eq 1u8 # Connection.Start method frame
  socket.local_address
ensure
  tls.try &.close rescue nil
  socket.try &.close rescue nil
end

describe LavinMQ::Clustering::Client do
  it "proxies AMQPS from the follower to the leader, keeping the client's address" do
    with_follower_amqps_proxy do |host, follower_amqps_port|
      Log.capture("lmq.server", :info) do |logs|
        client_address = amqps_start(host, follower_amqps_port)
        # The address of the client, not of the follower's connection to the leader,
        # formatted as the leader logs it (no brackets around IPv6)
        expected = LavinMQ::ConnectionInfo::IPAddress.new(client_address).to_s
        logs.check(:info, /^#{Regex.escape(expected)} connected with /)
      end
    end
  end

  it "forwards a trusted load balancer's PROXY header to the leader" do
    LavinMQ::Config.instance.tcp_proxy_protocol = true
    with_follower_amqps_proxy do |host, follower_amqps_port|
      Log.capture("lmq.server", :info) do |logs|
        # PROXY v2 from 10.1.2.3:54321 with an AUTHORITY TLV, as a load balancer passing TLS through sends it
        header = IO::Memory.new
        header.write LavinMQ::ProxyProtocol::V2::Signature.to_slice
        header.write Bytes[0x21, 0x11, 0x00, 0x13, 10, 1, 2, 3, 127, 0, 0, 1, 0xd4, 0x31, 0x16, 0x27]
        header.write Bytes[0x02, 0x00, 0x04]
        header.write "host".to_slice
        amqps_start(host, follower_amqps_port, header.to_slice)
        logs.check(:info, /^10\.1\.2\.3:54321 connected with /)
      end
    end
  end
end
