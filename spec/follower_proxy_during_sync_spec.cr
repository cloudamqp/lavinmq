require "./spec_helper"
require "../src/lavinmq/launcher"
require "../src/lavinmq/clustering/client"
require "../src/lavinmq/proxy_protocol"

# Create a custom slow clustering server for testing
class SlowClusteringServer < LavinMQ::Clustering::Server
  # Override files_with_hash to add delays during sync to simulate slow network
  def files_with_hash(& : Tuple(String, Bytes) -> Nil)
    sha1 = Digest::SHA1.new
    @files.each do |path, mfile|
      if calculated_hash = @checksums[path]?
        yield({path, calculated_hash})
      else
        if file = mfile
          sha1.update file.to_slice
          file.dontneed
        else
          filename = File.join(@data_dir, path)
          next unless File.exists? filename
          sha1.file filename
        end
        hash = sha1.final
        @checksums[path] = hash
        sha1.reset

        # Add delay to slow down sync when testing
        sleep 0.1.seconds

        Fiber.yield
        yield({path, hash})
      end
    end
  end

  # Override close to avoid checksums.sha1 file write issues in tests
  def close
    @listeners.each &.close
    @lock.synchronize do
      @followers.each &.close
      @followers.clear
    end
    Fiber.yield # required for follower/listener fibers to actually finish
    # Skip @checksums.store to avoid file write errors in tests
  end
end

describe "extract_conn_info during full_sync with syncing_followers" do
  add_etcd_around_each

  it "should handle PROXY protocol from syncing followers during full_sync" do
    leader_config = LavinMQ::Config.instance.dup
    FileUtils.mkdir_p(leader_config.data_dir)
    slow_replicator = SlowClusteringServer.new(leader_config, LavinMQ::Etcd.new("localhost:12379"), 0)
    leader_tcp_server = TCPServer.new("localhost", 0)
    spawn(slow_replicator.listen(leader_tcp_server), name: "slow leader clustering")

    begin
      with_amqp_server(replicator: slow_replicator) do |leader_s|
        leader_s.@config.clustering = true
        with_channel(leader_s) do |ch|
          q = ch.queue("test_queue")
          20.times { |i| q.publish_confirm "message #{i}" }
        end

        follower_data_dir = File.tempname
        Dir.mkdir follower_data_dir

        begin
          follower_config = leader_config.dup.tap &.data_dir = follower_data_dir
          follower_config.metrics_http_port = -1
          follower_client = LavinMQ::Clustering::Client.new(follower_config, 2, slow_replicator.password, proxy: false)

          spawn(name: "slow follower sync") do
            follower_client.follow("localhost", leader_tcp_server.local_address.port)
          end

          sleep 0.5.seconds # Give time for sync to start but not complete

          server_port = amqp_port(leader_s)

          # Ensure we have a syncing follower
          slow_replicator.syncing_followers.size.should be > 0

          client_socket = TCPSocket.new("localhost", server_port)

          begin
            # Since we're connecting from 127.0.0.1 (same as follower), this should work
            proxy_header = "PROXY TCP4 192.168.1.100 127.0.0.1 54321 #{server_port}\r\n"
            client_socket.write(proxy_header.to_slice)
            client_socket.write("AMQP\u0000\u0000\t\u0001".to_slice)
            client_socket.flush

            sleep 0.1.seconds

            buffer = Bytes.new(128)
            client_socket.read_timeout = 0.5.seconds
            bytes_read = client_socket.read(buffer)

            # Verify we got a proper AMQP Connection.Start response
            response_str = String.new(buffer[0, bytes_read])
            response_str.should contain("LavinMQ")
          ensure
            client_socket.close rescue nil
          end
        ensure
          follower_client.try &.close
          FileUtils.rm_rf follower_data_dir if follower_data_dir
        end
      end
    ensure
      slow_replicator.try &.close
    end
  end

  it "should reject PROXY protocol when no followers are syncing or synced" do
    with_amqp_server do |server|
      server.@config.clustering = true

      server_port = amqp_port(server)
      client_socket = TCPSocket.new("localhost", server_port)

      begin
        proxy_header = "PROXY TCP4 192.168.1.100 127.0.0.1 54321 #{server_port}\r\n"
        client_socket.write(proxy_header.to_slice)

        amqp_header = "AMQP\u0000\u0000\t\u0001"
        client_socket.write(amqp_header.to_slice)
        client_socket.flush

        buffer = Bytes.new(8)
        client_socket.read_timeout = 0.5.seconds
        client_socket.read(buffer)

        buffer.should eq LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
      ensure
        client_socket.close rescue nil
      end
    end
  end

  it "should handle PROXY protocol after follower has fully synced" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |leader_s|
        leader_s.@config.clustering = true
        with_channel(leader_s) do |ch|
          q = ch.queue("test_queue")
          q.publish_confirm "test message"
        end

        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

        server_port = amqp_port(leader_s)
        client_socket = TCPSocket.new("localhost", server_port)

        begin
          # Get the actual follower's remote address for PROXY header
          follower_remote_addr = cluster.replicator.followers.first.remote_address

          proxy_header = "PROXY TCP4 #{follower_remote_addr.address} 127.0.0.1 #{follower_remote_addr.port} #{server_port}\r\n"
          client_socket.write(proxy_header.to_slice)
          client_socket.write("AMQP\u0000\u0000\t\u0001".to_slice)
          client_socket.flush

          buffer = Bytes.new(128)
          begin
            client_socket.read_timeout = 0.5.seconds
            bytes_read = client_socket.read(buffer)

            # Verify we got a proper AMQP Connection.Start response
            response_str = String.new(buffer[0, bytes_read])
            response_str.should contain("LavinMQ")
          end
        ensure
          client_socket.close rescue nil
        end

        cluster.stop
      end
    end
  end
end
