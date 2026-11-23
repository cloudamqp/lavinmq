require "log/spec"
require "./spec_helper"
require "../src/lavinmq/launcher"
require "../src/lavinmq/clustering/client"
require "../src/lavinmq/clustering/runner"

alias IndexTree = LavinMQ::MQTT::TopicTree(String)

private def metric_value(body : String, name : String, labels : Hash(String, String)) : Float64?
  body.each_line do |line|
    next unless line.starts_with?("#{name}{")
    close = line.index('}')
    next unless close
    parsed = Hash(String, String).new
    line[(name.size + 1)...close].split(", ").each do |pair|
      key, _, value = pair.partition('=')
      parsed[key] = value.strip('"')
    end
    next unless labels.all? { |k, v| parsed[k]? == v }
    return line[(close + 1)..].strip.to_f
  end
  nil
end

private def populate_msg_store(msg_store)
  segment_size = LavinMQ::Config.instance.segment_size
  msg_size = 1000_u64
  num_messages = (segment_size // msg_size) + 10
  props = LavinMQ::AMQP::Properties.new
  num_messages.times do
    msg = LavinMQ::Message.new(Time.utc.to_unix_ms, "exchange", "rk", props, msg_size, IO::Memory.new("x" * msg_size.to_i))
    msg_store.push(msg)
  end
  msg_store.@segments.size.should be > 1
end

private def do_full_sync(tcp_server, replicator, wg : WaitGroup? = nil) : Fiber::ExecutionContext::Isolated
  Fiber::ExecutionContext::Isolated.new("test-follower") do
    client_io = TCPSocket.new("localhost", tcp_server.local_address.port)
    begin
      # Handshake and authentication
      client_io.write LavinMQ::Clustering::Start
      client_io.write_bytes replicator.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
      client_io.write replicator.password.to_slice
      client_io.read_byte
      client_io.write_bytes 2i32, IO::ByteFormat::LittleEndian
      client_io.flush
      # Signal that full sync is about to start
      wg.try &.done
      sha1_size = Digest::SHA1.new.digest_size
      client_lz4 = Compress::LZ4::Reader.new(client_io)
      # Do the full sync two times without requesting files (everything is up
      # to date)
      2.times do
        loop do
          filename_len = client_lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?
          client_lz4.skip filename_len
          client_lz4.skip sha1_size
        end
        # 0 means "requesting files done"
        client_io.write_bytes 0i32
        client_io.flush
      end
    ensure
      client_io.close
    end
  end
end

describe LavinMQ::Clustering::Runner do
  describe ".advertised_uri" do
    it "advertises the client ports the node listens on" do
      config = LavinMQ::Config.new
      config.clustering_advertised_uri = "tcp://leader:5679"
      config.amqp_port = 5682
      config.mqtt_port = 1884
      config.http_port = 15682
      uri = URI.parse(LavinMQ::Clustering::Runner.advertised_uri(config))
      uri.host.should eq "leader"
      uri.port.should eq 5679
      uri.query_params.to_h.should eq({"amqp" => "5682", "mqtt" => "1884", "http" => "15682"})
    end

    it "advertises TLS ports only when TLS is configured" do
      config = LavinMQ::Config.new
      config.clustering_advertised_uri = "tcp://leader:5679"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.amqps_port = 5681
      config.mqtts_port = 0 # disabled
      params = URI.parse(LavinMQ::Clustering::Runner.advertised_uri(config)).query_params
      params["amqps"].should eq "5681"
      params["https"].should eq config.https_port.to_s
      params.has_key?("mqtts").should be_false
    end

    it "keeps ports given in the configured advertised_uri" do
      config = LavinMQ::Config.new
      config.clustering_advertised_uri = "tcp://leader:5679?amqp=30672"
      config.amqp_port = 5672
      params = URI.parse(LavinMQ::Clustering::Runner.advertised_uri(config)).query_params
      params.fetch_all("amqp").should eq ["30672"]
      params["http"].should eq config.http_port.to_s
    end

    it "skips disabled ports" do
      config = LavinMQ::Config.new
      config.clustering_advertised_uri = "tcp://leader:5679"
      config.mqtt_port = -1
      params = URI.parse(LavinMQ::Clustering::Runner.advertised_uri(config)).query_params
      params.has_key?("mqtt").should be_false
    end
  end
end

describe LavinMQ::Clustering::Client do
  describe "Leader" do
    it "reads the client ports a leader advertises" do
      leader = LavinMQ::Clustering::Client::Leader.parse(URI.parse("tcp://leader:5680?amqp=5682&https=15681&foo=1&mqtt=x&http=0"))
      leader.host.should eq "leader"
      leader.port.should eq 5680
      leader.forward_port("amqp", 5672).should eq 5682
      leader.forward_port("https", 15671).should eq 15681
      leader.forward_port("mqtt", 1883).should eq 1883   # invalid, the follower's own port
      leader.forward_port("http", 15672).should eq 15672 # out of range, the follower's own port
      leader.ports.has_key?("foo").should be_false
    end

    it "forwards to the follower's own ports for a leader that doesn't advertise them" do
      leader = LavinMQ::Clustering::Client::Leader.parse(URI.parse("tcp://leader"))
      leader.port.should eq 5679
      leader.forward_port("amqp", 5672).should eq 5672
    end
  end

  it "stops following a leader that advertises other ports" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      client = LavinMQ::Clustering::Client.new(config, 1, "secret")
      spawn client.follow("tcp://127.0.0.1:1?amqp=5682") # no leader there, keeps retrying
      # The follow fiber may not have run yet, e.g. on another thread
      wait_for { client.follows?("tcp://127.0.0.1:1?amqp=5682") }
      client.follows?("tcp://127.0.0.1:1?amqp=5683").should be_false
      client.follows?("tcp://127.0.0.1:1").should be_false
      client.follows?("127.0.0.1", 1).should be_true
    ensure
      client.try &.close
    end
  end
end

describe LavinMQ::Clustering::Client, tags: "slow" do
  it "can stream changes" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        with_channel(s) do |ch|
          q = ch.queue("repli", durable: true)
          q.publish_confirm "hello world", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
        end
        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
        cluster.stop
      end

      server = LavinMQ::Server.new(cluster.follower_config)
      begin
        q = server.vhosts["/"].queue("repli").as(LavinMQ::AMQP::DurableQueue)
        q.message_count.should eq 1
        q.basic_get(true) do |env|
          String.new(env.message.body).to_s.should eq "hello world"
        end.should be_true
      ensure
        server.close
      end
    end
  end

  it "confirms publishes once synced locally and replicated to followers" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          ch.confirm_select
          q = ch.queue("repli_confirm", durable: true)
          100.times { q.publish "msg", props: AMQP::Client::Properties.new(delivery_mode: 2_u8) }
          # Confirms only arrive once all synced followers have acked the
          # replicated bytes; this would block forever if it never returned.
          ch.wait_for_confirms.should be_true
        end
        s.vhosts["/"].queue("repli_confirm").message_count.should eq 100
        cluster.replicator.followers.first?.try &.lag_in_bytes.should eq 0
      end
    end
  end

  it "exposes inter-node replication byte counters" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        with_channel(s) do |ch|
          q = ch.queue("repli", durable: true)
          q.publish_confirm "hello world", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
        end
        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

        follower_id = cluster.replicator.followers.first.id.to_s(36)

        serve_metrics(s) do |http|
          body = http.get("/metrics").body
          sent = metric_value(body, "lavinmq_follower_bytes_sent_total", {"id" => follower_id}).should_not be_nil
          acked = metric_value(body, "lavinmq_follower_bytes_acked_total", {"id" => follower_id}).should_not be_nil
          sent.should be > 0
          acked.should be > 0
        end

        serve_follower_metrics(cluster.repli) do |http|
          body = http.get("/metrics").body
          line = body.lines.find(&.starts_with?("lavinmq_cluster_received_bytes_total ")).should_not be_nil
          line.split(' ').last.to_f.should be > 0
        end
      end
    end
  end

  it "confirms via syncfs while the only follower is still syncing" do
    # Regression: a publish written while all followers are syncing isn't streamed
    # to them. The confirm must not stall waiting for an ack from a follower that
    # flips to synced before the persister drains.
    Dir.mkdir_p LavinMQ::Config.instance.data_dir
    replicator = LavinMQ::Clustering::Server.new(
      LavinMQ::Config.instance, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)
    spawn(replicator.listen(tcp_server), name: "repli server spec")

    # Fake follower: handshake, read the file list, then hang without finishing
    # the file-request phase, so it stays in the Syncing state on the leader.
    client_io = TCPSocket.new("localhost", tcp_server.local_address.port)
    client_io.write LavinMQ::Clustering::Start
    client_io.write_bytes replicator.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
    client_io.write replicator.password.to_slice
    client_io.read_byte
    client_io.write_bytes 2i32, IO::ByteFormat::LittleEndian
    client_io.flush
    client_lz4 = Compress::LZ4::Reader.new(client_io)
    sha1_size = Digest::SHA1.new.digest_size
    spawn(name: "syncing follower spec") do
      loop do
        filename_len = client_lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
        break if filename_len.zero?
        client_lz4.skip filename_len
        client_lz4.skip sha1_size
      end
      # Intentionally never send the "0 files requested" reply: stay syncing.
    rescue IO::Error
    end

    wait_for { replicator.syncing_followers.size == 1 }
    replicator.followers.should be_empty # none synced

    with_amqp_server(replicator: replicator) do |s|
      with_channel(s) do |ch|
        ch.confirm_select
        q = ch.queue("syncing_confirm", durable: true)
        10.times { q.publish "m", props: AMQP::Client::Properties.new(delivery_mode: 2_u8) }
        # Confirmed via syncfs (no synced follower); must not stall or be skipped.
        ch.wait_for_confirms.should be_true
      end
      s.vhosts["/"].queue("syncing_confirm").message_count.should eq 10
    end
  ensure
    client_io.try &.close
    replicator.try &.close
    tcp_server.try &.close
    FileUtils.rm_rf LavinMQ::Config.instance.data_dir
  end

  # Opens a message store, publishes some messages, then saves replicator.@files
  # Then opens a new message store in the same directory and verifies that the same
  # files are registered in the new replicator (verifies that meta files are registered and replicated).
  it "registers meta files on startup" do
    coordinator = NullCoordinator.new
    msg_dir = File.join(LavinMQ::Config.instance.data_dir, "meta_test_queue")
    FileUtils.mkdir_p(msg_dir)
    node_id = 0

    begin
      replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, node_id)
      msg_store = LavinMQ::MessageStore.new(msg_dir, replicator)
      segment_size = LavinMQ::Config.instance.segment_size
      msg_size = 1000_u64
      num_messages = (segment_size // msg_size) + 100 # ensure we create multiple segments
      props = LavinMQ::AMQP::Properties.new
      msg = LavinMQ::Message.new(Time.utc.to_unix_ms, "exchange", "rk", props, msg_size, IO::Memory.new("x" * msg_size.to_i))
      num_messages.times { msg_store.push(msg) }
      msg_store.@segments.size.should be > 1

      msg_store.close
      files_before = replicator.@file_index.shared { |files, _| files.keys }.sort!
      replicator.close

      # Re-open the message store and verify the same files are registered
      replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, node_id + 1)
      msg_store = LavinMQ::MessageStore.new(msg_dir, replicator)
      msg_store.close
      files_after = replicator.@file_index.shared { |files, _| files.keys }.sort!

      files_before.size.should be > 2
      files_before.find(&.ends_with?("meta.0000000001")).should_not be_nil
      files_before.should eq files_after
      replicator.close
    ensure
      FileUtils.rm_rf(msg_dir)
    end
  end

  it "registers the retained messages it restores, so a follower's full sync keeps them" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "retain_store")
    store = LavinMQ::MQTT::RetainStore.new(dir, nil)
    store.retain(MQTT::Protocol::Publish.new(topic: "topic1", payload: "body1".to_slice,
      packet_id: nil, dup: false, qos: 0u8, retain: true))
    store.close
    msg_file = Dir.children(dir).find!(&.ends_with?(".msg"))
    with_clustering do |cluster|
      store = LavinMQ::MQTT::RetainStore.new(dir, cluster.replicator)
      files = cluster.replicator.@file_index.shared { |f, _| f.keys }
      files.should contain "retain_store/#{msg_file}"
    ensure
      store.try &.close
    end
  ensure
    FileUtils.rm_rf(dir) if dir
  end

  it "replicates and streams retained messages to followers" do
    with_clustering do |cluster|
      replicator = cluster.replicator
      retain_store = LavinMQ::MQTT::RetainStore.new("#{cluster.config.data_dir}/retain_store", replicator)
      # Let the follower catch up before appending. If we don't, data may have
      # been appended to the file before it's written to socket, meaning that
      # the lag_size has changed.
      wait_for { replicator.followers.first?.try &.lag_in_bytes == 0 }

      pub1 = MQTT::Protocol::Publish.new(topic: "topic1", payload: "body1".to_slice,
        packet_id: nil, dup: false, qos: 0u8, retain: true)
      pub2 = MQTT::Protocol::Publish.new(topic: "topic2", payload: "body2".to_slice,
        packet_id: nil, dup: false, qos: 0u8, retain: true)
      retain_store.retain(pub1)
      retain_store.retain(pub2)

      wait_for { replicator.followers.first?.try &.lag_in_bytes == 0 }
      cluster.stop

      follower_retain_store = LavinMQ::MQTT::RetainStore.new("#{cluster.follower_config.data_dir}/retain_store", nil)
      a = Array(String).new(2)
      b = Array(String).new(2)
      follower_retain_store.each("#") do |topic, body_io, body_bytesize|
        a << topic
        b << body_io.read_string(body_bytesize)
      end

      a.sort!.should eq(["topic1", "topic2"])
      b.sort!.should eq(["body1", "body2"])
      follower_retain_store.@index.size.should eq(2)
    ensure
      follower_retain_store.try &.close
      retain_store.try &.close
    end
  end

  it "can stream full file" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        s.users.create("u1", "p1")
        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
        cluster.stop
      end

      server = LavinMQ::Server.new(cluster.follower_config)
      begin
        server.users["u1"].should_not be_nil
      ensure
        server.close
      end
    end
  end

  it "names the leader, for forwarding clients, only while connected to it" do
    with_datadir do |data_dir|
      config = LavinMQ::Config.instance.dup.tap &.data_dir = data_dir
      repli = LavinMQ::Clustering::Client.new(config, 1, "secret")
      spawn(name: "follow unreachable spec") { repli.follow("tcp://localhost:#{free_port}?amqp=5682") }
      sleep 100.milliseconds # retrying the refused connection
      repli.leader.should be_nil
      repli.close
    end
    with_clustering do |cluster|
      wait_for { cluster.repli.leader.try(&.host) == "localhost" }
      cluster.replicator.close
      wait_for { cluster.repli.leader.nil? }
    end
  end

  it "won't deadlock under high load when a follower disconnects [#926]" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)
    spawn(replicator.listen(tcp_server), name: "repli server spec")

    client_io = TCPSocket.new("localhost", tcp_server.local_address.port)
    # This is raw clustering negotiation
    client_io.write LavinMQ::Clustering::Start
    client_io.write_bytes replicator.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
    client_io.write replicator.password.to_slice
    # Read the password accepted byte (we assume it's correct)
    client_io.read_byte
    # Send the follower id
    client_io.write_bytes 2i32, IO::ByteFormat::LittleEndian
    client_io.flush
    client_lz4 = Compress::LZ4::Reader.new(client_io)
    # Two full syncs
    sha1_size = Digest::SHA1.new.digest_size
    2.times do
      loop do
        filename_len = client_lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
        break if filename_len.zero?
        client_lz4.skip filename_len
        client_lz4.skip sha1_size
      end
      # 0 means we're done requesting files for this full sync
      client_io.write_bytes 0i32
      client_io.flush
    end

    test_path = "#{LavinMQ::Config.instance.data_dir}/path"
    Dir.mkdir_p LavinMQ::Config.instance.data_dir
    replicator.register_file(test_path)
    payload = "ABCD".to_slice

    appended = Channel(Bool).new
    spawn do
      # Fill the socket buffer
      offset = 0i64
      loop do
        replicator.append_bytes(test_path, payload, offset)
        offset += payload.bytesize
        appended.send true
      rescue IO::Error | Socket::Error | Channel::ClosedError
        break
      end
    end

    # Wait for lag to increase
    loop do
      select
      when appended.receive?
      when timeout 0.1.seconds
      end
      break if replicator.@followers.first?.try &.lag_in_bytes.>(0)
    end

    # Now disconnect the follower. Our "fill" fiber should continue or exit
    client_io.close

    select
    when appended.receive?
    when timeout 5.seconds
      deadlock = true
    end

    # If the fiber is blocked on a write to a closed socket, it might never send to 'appended'
    # but we want to make sure the replicator can still be closed
    replicator.try &.close
    deadlock = false if !deadlock # if it didn't deadlock yet, we're good

    appended.close
    if deadlock
      fail "deadlock detected"
    end
  ensure
    FileUtils.mkdir_p LavinMQ::Config.instance.data_dir
    replicator.try &.close
  end

  describe "checksum caching on followers" do
    it "can persist checksums for promotion" do
      with_clustering do |cluster|
        with_amqp_server(replicator: cluster.replicator) do |s|
          with_channel(s) do |ch|
            # Create a queue which will create files during streaming
            q = ch.queue("checksum_test")
            q.publish_confirm "test message"
          end

          # Wait for replication to complete
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

          # Brief wait for any async operations
          sleep 0.1.seconds
        end

        # Close follower (simulating promotion) - this persists checksums
        follower = cluster.repli
        follower.close

        # Verify checksums were persisted to disk
        checksums_file = File.join(cluster.follower_config.data_dir, "checksums.sha1")
        File.exists?(checksums_file).should be_true
      end
    end

    it "checksums files during streaming" do
      with_clustering do |cluster|
        with_amqp_server(replicator: cluster.replicator) do |s|
          with_channel(s) do |ch|
            # Create a queue and publish messages
            q = ch.queue("checksum_test", durable: true)
            10.times { |i| q.publish_confirm "message #{i}", props: AMQP::Client::Properties.new(delivery_mode: 2) }
          end

          # Wait for replication to complete
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

          # Brief wait for any async operations
          sleep 0.1.seconds
        end

        follower = cluster.repli
        follower.close

        # Verify checksums file exists and has entries
        checksums_file = File.join(cluster.follower_config.data_dir, "checksums.sha1")
        File.exists?(checksums_file).should be_true

        content = File.read(checksums_file)
        lines = content.lines

        # Should have checksums for multiple files (queue definition + message segments)
        lines.size.should be >= 2

        # Verify each line has correct checksum format: 40 hex chars, covered size, asterisk, path
        lines.each do |line|
          line.should match(/^[0-9a-f]{40} \d+ \*/)
        end

        # Should have checksum for the queue's message segment file
        # Queue creates files in %default/queues/<sha1 of queue name>/
        lines.any?(&.includes?("msgs.")).should be_true
      end
    end

    it "handles file deletions without crashing" do
      with_clustering do |cluster|
        with_amqp_server(replicator: cluster.replicator) do |s|
          with_channel(s) do |ch|
            # Create and delete a queue, which will delete its files
            q = ch.queue("checksum_delete_test")
            q.publish_confirm "test"
            q.delete
          end

          # Wait for replication to complete
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

          # Brief wait for any async operations
          sleep 0.1.seconds
        end

        follower = cluster.repli

        # Should not crash when closing
        # Just call it and verify no exception is raised
        follower.close
      end
    end
  end

  describe "full sync when message store is closed" do
    it "succeeds when message store is already closed before sync" do
      msg_dir = File.join(LavinMQ::Config.instance.data_dir, "sync_after_close_test")
      FileUtils.mkdir_p(msg_dir)
      replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
      msg_store = LavinMQ::MessageStore.new(msg_dir, replicator)
      populate_msg_store(msg_store)

      msg_store.close
      Fiber.yield # let the spawned close fiber run so MFiles are unmapped

      tcp_server = TCPServer.new("localhost", 0)
      spawn(replicator.listen(tcp_server), name: "repli server spec")

      do_full_sync(tcp_server, replicator).wait
    ensure
      replicator.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf msg_dir if msg_dir
    end

    it "is not aborted when message store is closed concurrently" do
      msg_dir = File.join(LavinMQ::Config.instance.data_dir, "sync_close_concurrent_test")
      FileUtils.mkdir_p(msg_dir)
      replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
      msg_store = LavinMQ::MessageStore.new(msg_dir, replicator)
      populate_msg_store(msg_store)

      tcp_server = TCPServer.new("localhost", 0)
      spawn(replicator.listen(tcp_server), name: "repli server spec")

      wg = WaitGroup.new(1)
      follower_ctx = do_full_sync(tcp_server, replicator, wg)

      wg.wait         # suspend until negotiation is done and files_with_hash has started
      msg_store.close # concurrent close — simulates a corrupt segment triggering close
      Fiber.yield     # let the spawned close fiber run: wg.wait → segment.close (munmap)
      follower_ctx.wait
    ensure
      replicator.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf msg_dir if msg_dir
    end
  end

  it "replicates .queue file when queue is created" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          ch.queue("dotqueue_test", durable: true)
        end
        vhost = s.vhosts["/"]
        dotqfile = File.join(vhost.queue("dotqueue_test").as(LavinMQ::AMQP::Queue).@data_dir, ".queue")
        dotqfile_relative = dotqfile[(s.data_dir.size + 1)..]
        replicated_dotqfile = File.join(cluster.follower_config.data_dir, dotqfile_relative)
        wait_for { File.exists?(replicated_dotqfile) }
        File.exists?(replicated_dotqfile).should be_true
      end
    end
  end

  it "removes .queue file from follower when queue is deleted", tags: "slow" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          q = ch.queue("dotqueue_test", durable: true)
          vhost = s.vhosts["/"]
          dotqfile = File.join(vhost.queue("dotqueue_test").as(LavinMQ::AMQP::Queue).@data_dir, ".queue")
          dotqfile_relative = dotqfile[(s.data_dir.size + 1)..]
          replicated_dotqfile = File.join(cluster.follower_config.data_dir, dotqfile_relative)
          wait_for { File.exists?(replicated_dotqfile) }
          q.delete
          wait_for { !File.exists?(replicated_dotqfile) }
          File.exists?(replicated_dotqfile).should be_false
        end
      end
    end
  end

  it "removes empty queue dir from follower when queue is deleted" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          q = ch.queue("dirdelete_test", durable: true)
          q.publish_confirm "hello"
          qdir = s.vhosts["/"].queue("dirdelete_test").as(LavinMQ::AMQP::Queue).@data_dir
          qdir_relative = qdir[(s.data_dir.size + 1)..]
          replicated_qdir = File.join(cluster.follower_config.data_dir, qdir_relative)
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
          wait_for { Dir.exists?(replicated_qdir) }
          q.delete
          wait_for { !Dir.exists?(replicated_qdir) }
        end
      end
    end
  end

  it "removes the vhost dir from follower when vhost is deleted" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        vhost = s.vhosts.create("churn")
        with_channel(s, vhost: "churn") do |ch|
          q = ch.queue("q", durable: true)
          q.publish_confirm "hello"
        end
        replicated_dir = File.join(cluster.follower_config.data_dir, vhost.dir)
        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
        Dir.exists?(replicated_dir).should be_true
        s.vhosts.delete("churn")
        wait_for { !Dir.exists?(replicated_dir) }
      end
    end
  end

  it "keeps the queue dir on follower when a segment is deleted but the queue isn't" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          q = ch.queue("segdelete_test", durable: true)
          body = "x" * (LavinMQ::Config.instance.segment_size // 100)
          # publish enough to span more than one segment, so an older fully-acked
          # segment can be deleted while the active write segment + .queue remain
          101.times { q.publish_confirm body }

          dq = s.vhosts["/"].queue("segdelete_test").as(LavinMQ::AMQP::DurableQueue)
          dq.@msg_store.@segments.size.should be > 1
          repl_qdir = File.join(cluster.follower_config.data_dir, dq.@data_dir[(s.data_dir.size + 1)..])
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
          files_before = Dir.children(repl_qdir).size

          # consume and ack everything; fully-acked non-write segments get deleted
          q.subscribe(no_ack: false, &.ack)
          wait_for { dq.message_count == 0 && dq.@msg_store.@segments.size == 1 }
          wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }

          # a segment file was deleted, but the queue dir exists
          Dir.exists?(repl_qdir).should be_true
          wait_for { Dir.children(repl_qdir).size < files_before } # unlinked after the persister's sync
        end
      end
    end
  end

  it "compacts and replicates a stream's consumer_offsets file when it fills [#2068]" do
    with_clustering do |cluster|
      expected = Bytes.new(0)
      follower_offsets_path = ""
      ctag = "ctag-" + "x" * 100
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        queue_name = Random::Secure.hex
        with_channel(s) do |ch|
          q = ch.queue(queue_name, args: AMQP::Client::Arguments.new({"x-queue-type": "stream"}))
          q.publish_confirm "m"
        end

        store = s.vhosts["/"].queue(queue_name).as(LavinMQ::AMQP::Queue).@msg_store.as(LavinMQ::AMQP::StreamMessageStore)
        offsets_path = store.@consumer_offsets.@mfile.path
        follower_offsets_path = File.join(cluster.follower_config.data_dir, offsets_path[(s.data_dir.size + 1)..])

        # Fill consumer_offsets past its capacity to trigger compaction. Before
        # #2068's fix the first compaction raised ArgumentError because the
        # rebuilt .tmp MFile was never replication-registered.
        cap = store.@consumer_offsets.@mfile.capacity
        entry = 1 + ctag.bytesize + 8
        ((cap // entry) + 10).times do |i|
          store.store_consumer_offset(ctag, i.to_i64 + 1)
        end
        store.@consumer_offsets.size.should be < cap # got compacted

        # A write AFTER compaction must still succeed and replicate, proving the
        # MFile stayed registered through replace_file.
        store.store_consumer_offset(ctag, 9999_i64)
        store.last_offset_by_consumer_tag(ctag).should eq 9999_i64

        wait_for { cluster.replicator.followers.first?.try &.lag_in_bytes == 0 }
        expected = store.@consumer_offsets.@mfile.to_slice.dup
      end

      # The follower received the compacted file verbatim — its real data
      # length, not the sparse ftruncate capacity.
      File.exists?(follower_offsets_path).should be_true
      follower_bytes = File.open(follower_offsets_path, &.getb_to_end)
      follower_bytes.should eq expected
    end
  end

  it "does not replicate .queue file for non-durable queue" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) do |ch|
          ch.queue("dotqueue_transient", durable: false)
        end
        vhost = s.vhosts["/"]
        dotqfile = File.join(vhost.queue("dotqueue_transient").as(LavinMQ::AMQP::Queue).@data_dir, ".queue")
        dotqfile_relative = dotqfile[(s.data_dir.size + 1)..]
        wait_for { File.exists?(dotqfile) }
        Fiber.yield
        cluster.replicator.with_file(dotqfile_relative) do |f, _size|
          f.should be_nil
        end
      end
    end
  end

  it "replicates a durable queue's paused state" do
    with_clustering do |cluster|
      follower_paused = ""
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        with_channel(s) { |ch| ch.queue("paused_q", durable: true) }
        queue = s.vhosts["/"].queue("paused_q").as(LavinMQ::AMQP::Queue)
        relative = File.join(queue.@data_dir, "paused")[(s.data_dir.size + 1)..]
        follower_paused = File.join(cluster.follower_config.data_dir, relative)

        queue.pause!
        wait_for { File.exists?(follower_paused) }
        queue.resume!
        wait_for { !File.exists?(follower_paused) }

        queue.pause!
        wait_for { File.exists?(follower_paused) }
        # A leader that starts with a paused queue has to offer the file in a full sync
        restart_server(s)
        cluster.replicator.with_file(relative) { |f, _size| f.should_not be_nil }
        cluster.stop
      end

      server = LavinMQ::Server.new(cluster.follower_config)
      begin
        server.vhosts["/"].queue("paused_q").state.paused?.should be_true
      ensure
        server.close
      end
    end
  end

  # An in-place upgrade: the queue and the shovel were paused by a version that
  # didn't replicate the paused files, and left the queue's one empty
  it "replicates paused files that exist when the leader starts" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
    follower_config = LavinMQ::Config.instance.dup.tap &.data_dir = File.tempname
    tcp_server = TCPServer.new("localhost", 0)
    client = nil
    begin
      with_amqp_server(replicator: replicator) do |s|
        with_channel(s) { |ch| ch.queue("paused_before_upgrade", durable: true) }
        queue = s.vhosts["/"].queue("paused_before_upgrade").as(LavinMQ::AMQP::Queue)
        queue_paused = File.join(queue.@data_dir, "paused")[(s.data_dir.size + 1)..]
        File.touch(File.join(s.data_dir, queue_paused))
        shovel_paused = "shovels.#{Digest::SHA1.hexdigest "paused_before_upgrade"}.paused"
        File.write(File.join(s.data_dir, shovel_paused), "paused_before_upgrade")
        shovel_config = %({"src-uri": "amqp://localhost", "src-queue": "q", "dest-uri": "amqp://localhost", "dest-queue": "q2"})
        s.vhosts["/"].add_parameter(LavinMQ::Parameter.new("shovel", "paused_before_upgrade", JSON.parse(shovel_config)))
        restart_server(s)
        s.vhosts["/"].queue("paused_before_upgrade").state.paused?.should be_true
        wait_for { s.vhosts["/"].shovels["paused_before_upgrade"]?.try &.state.paused? }

        spawn(replicator.listen(tcp_server), name: "repli server spec")
        client = follower = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password)
        spawn(name: "follow spec") { follower.follow("localhost", tcp_server.local_address.port) }
        wait_for { replicator.followers.first?.try &.synced? }

        File.exists?(File.join(follower_config.data_dir, queue_paused)).should be_true
        File.exists?(File.join(follower_config.data_dir, shovel_paused)).should be_true
      end
    ensure
      client.try &.close
      replicator.close
      tcp_server.close
      FileUtils.rm_rf follower_config.data_dir
    end
  end

  it "replicates a shovel's paused state" do
    with_clustering do |cluster|
      with_amqp_server(replicator: cluster.replicator) do |s|
        wait_for { cluster.replicator.followers.first?.try &.synced? }
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new("paused-shovel", [URI.parse("amqp://localhost")], "paused_shovel_q",
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::AMQPDestination.new("paused-shovel", URI.parse("amqp://localhost"), "paused_shovel_q2",
          direct_user: s.users.direct_user)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "paused-shovel", vhost)
        relative = shovel.@paused_file_path[(s.data_dir.size + 1)..]
        follower_paused = File.join(cluster.follower_config.data_dir, relative)

        shovel.pause
        wait_for { File.exists?(follower_paused) }
        # A shovel that starts paused has to offer the file in a full sync
        LavinMQ::Shovel::Runner.new(source, dest, "paused-shovel", vhost).state.paused?.should be_true
        cluster.replicator.with_file(relative) { |f, _size| f.should_not be_nil }

        shovel.resume
        wait_for { !File.exists?(follower_paused) }
        shovel.delete
      end
    end
  end
end
