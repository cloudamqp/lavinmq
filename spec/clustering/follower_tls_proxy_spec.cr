require "log/spec"
require "../spec_helper"
require "../../src/lavinmq/clustering/client"

describe LavinMQ::Clustering::Client do
  it "proxies AMQPS from the follower to the leader, keeping the client's address" do
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
        # The follower and the leader can't both bind the same port on one host
        follower_config.clustering_amqps_forward_port = leader_tls.port
        repli = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password)
        follower_stopped = WaitGroup.new(1)
        spawn(name: "follow spec") do
          repli.follow(host, repli_server.local_address.port)
          follower_stopped.done
        end
        begin
          wait_for { replicator.followers.size == 1 }

          Log.capture("lmq.server", :info) do |logs|
            socket = TCPSocket.new(host, follower_amqps_port)
            client_address = socket.local_address
            client_ctx = OpenSSL::SSL::Context::Client.new
            client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
            tls = OpenSSL::SSL::Socket::Client.new(socket, client_ctx, sync_close: true)
            tls.write LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
            tls.flush
            tls.read_timeout = 5.seconds
            frame = Bytes.new(7)
            tls.read_fully(frame)
            frame[0].should eq 1u8 # Connection.Start method frame
            tls.close
            # The address of the client, not of the follower's connection to the leader,
            # formatted as the leader logs it (no brackets around IPv6)
            expected = LavinMQ::ConnectionInfo::IPAddress.new(client_address).to_s
            logs.check(:info, /^#{Regex.escape(expected)} connected with /)
          end
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

  it "forwards AMQP to the configured forward port on the leader" do
    config = LavinMQ::Config.instance
    config.clustering = true
    replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
    follower_data_dir = File.tempname
    Dir.mkdir follower_data_dir
    begin
      with_amqp_server(replicator: replicator) do |s|
        leader_amqp = s.amqp_server.@listeners.compact_map(&.as?(TCPServer)).first.local_address
        host = leader_amqp.address
        repli_server = TCPServer.new(host, 0)
        spawn(replicator.listen(repli_server), name: "repli server spec")

        reserved = TCPServer.new(host, 0)
        follower_amqp_port = reserved.local_address.port
        reserved.close

        follower_config = config.dup
        follower_config.data_dir = follower_data_dir
        follower_config.amqp_bind = follower_config.http_bind = follower_config.mqtt_bind = host
        follower_config.amqp_port = follower_amqp_port
        follower_config.http_port = follower_config.mqtt_port = 0
        follower_config.tls_cert_path = ""
        # The follower listens on another port than the leader, so without a
        # forward port it would forward to a port nothing listens on
        follower_config.clustering_amqp_forward_port = leader_amqp.port
        repli = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password)
        follower_stopped = WaitGroup.new(1)
        spawn(name: "follow spec") do
          repli.follow(host, repli_server.local_address.port)
          follower_stopped.done
        end
        begin
          wait_for { replicator.followers.size == 1 }
          socket = TCPSocket.new(host, follower_amqp_port)
          socket.write LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
          socket.read_timeout = 5.seconds
          frame = Bytes.new(7)
          socket.read_fully(frame)
          frame[0].should eq 1u8 # Connection.Start method frame
          socket.close
        ensure
          repli.close
          follower_stopped.wait
        end
      ensure
        replicator.close
      end
    ensure
      config.clustering = false
      FileUtils.rm_rf follower_data_dir
    end
  end
end
