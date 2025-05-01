require "./spec_helper"
require "./clustering/spec_helper"
require "../src/lavinmq/launcher"
require "../src/lavinmq/clustering/client"
require "../src/lavinmq/clustering/controller"

alias IndexTree = LavinMQ::MQTT::TopicTree(String)

describe LavinMQ::Clustering::Client do
  follower_data_dir = "/tmp/lavinmq-follower"
  around_each do |spec|
    FileUtils.rm_rf follower_data_dir
    spec.run
  ensure
    FileUtils.rm_rf follower_data_dir
  end
  add_etcd_around_each

  it "can stream changes" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
    tcp_server = TCPServer.new("localhost", 0)
    spawn(replicator.listen(tcp_server), name: "repli server spec")
    config = LavinMQ::Config.new.tap &.data_dir = follower_data_dir
    repli = LavinMQ::Clustering::Client.new(config, 1, replicator.password, proxy: false)
    done = Channel(Nil).new
    spawn(name: "follow spec") do
      repli.follow("localhost", tcp_server.local_address.port)
      done.send nil
    end
    wait_for { replicator.followers.size == 1 }
    with_amqp_server(replicator: replicator) do |s|
      with_channel(s) do |ch|
        q = ch.queue("repli")
        q.publish_confirm "hello world"
      end
      repli.close
      done.receive
    end

    server = LavinMQ::Server.new(config)
    begin
      q = server.vhosts["/"].queues["repli"].as(LavinMQ::AMQP::DurableQueue)
      q.message_count.should eq 1
      q.basic_get(true) do |env|
        String.new(env.message.body).to_s.should eq "hello world"
      end.should be_true
    ensure
      server.close
    end
  ensure
    replicator.try &.close
  end

  it "replicates and streams retained messages to followers" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
    tcp_server = TCPServer.new("localhost", 0)

    spawn(replicator.listen(tcp_server), name: "repli server spec")
    config = LavinMQ::Config.new.tap &.data_dir = follower_data_dir
    repli = LavinMQ::Clustering::Client.new(config, 1, replicator.password, proxy: false)
    done = Channel(Nil).new
    spawn(name: "follow spec") do
      repli.follow("localhost", tcp_server.local_address.port)
      done.send nil
    end
    wait_for { replicator.followers.size == 1 }

    retain_store = LavinMQ::MQTT::RetainStore.new("#{LavinMQ::Config.instance.data_dir}/retain_store", replicator)
    wait_for { replicator.followers.first?.try &.lag_in_bytes == 0 }

    props = LavinMQ::AMQP::Properties.new
    msg1 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body1"))
    msg2 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body2"))
    retain_store.retain("topic1", msg1.body_io, msg1.bodysize)
    retain_store.retain("topic2", msg2.body_io, msg2.bodysize)

    wait_for { replicator.followers.first?.try &.lag_in_bytes == 0 }
    repli.close
    done.receive

    follower_retain_store = LavinMQ::MQTT::RetainStore.new("#{follower_data_dir}/retain_store", LavinMQ::Clustering::NoopServer.new)
    a = Array(String).new(2)
    b = Array(String).new(2)
    follower_retain_store.each("#") do |topic, body_io, body_bytesize|
      a << topic
      b << body_io.read_string(body_bytesize)
    end

    a.sort!.should eq(["topic1", "topic2"])
    b.sort!.should eq(["body1", "body2"])
    follower_retain_store.retained_messages.should eq(2)
  ensure
    replicator.try &.close
  end

  it "can stream full file" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
    tcp_server = TCPServer.new("localhost", 0)
    spawn(replicator.listen(tcp_server), name: "repli server spec")
    config = LavinMQ::Config.new.tap &.data_dir = follower_data_dir
    repli = LavinMQ::Clustering::Client.new(config, 1, replicator.password, proxy: false)
    done = Channel(Nil).new
    spawn(name: "follow spec") do
      repli.follow("localhost", tcp_server.local_address.port)
      done.send nil
    end
    wait_for { replicator.followers.size == 1 }
    with_amqp_server(replicator: replicator) do |s|
      s.users.create("u1", "p1")
      wait_for { replicator.followers.first?.try &.lag_in_bytes == 0 }
      repli.close
      done.receive
    end

    server = LavinMQ::Server.new(config)
    begin
      server.users["u1"].should_not be_nil
    ensure
      server.close
    end
  end

  it "will failover" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/failover1"
    config1.clustering_etcd_endpoints = "localhost:12379"
    config1.clustering_advertised_uri = "tcp://localhost:5681"
    config1.clustering_port = 5681
    config1.amqp_port = 5671
    config1.http_port = 15671
    controller1 = LavinMQ::Clustering::Controller.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/failover2"
    config2.clustering_etcd_endpoints = "localhost:12379"
    config2.clustering_advertised_uri = "tcp://localhost:5682"
    config2.clustering_port = 5682
    config2.amqp_port = 5672
    config2.http_port = 15672
    controller2 = LavinMQ::Clustering::Controller.new(config2)

    listen = Channel(String?).new
    spawn(name: "etcd elect leader spec") do
      etcd = LavinMQ::Etcd.new("localhost:12379")
      etcd.elect_listen("lavinmq/leader") do |value|
        listen.send value
      end
    rescue SpecExit
      # expect this when etcd nodes are terminated
    end
    sleep 0.5.seconds
    spawn(name: "failover1") do
      controller1.run { }
    end
    spawn(name: "failover2") do
      controller2.run { }
    end
    sleep 0.1.seconds
    leader = listen.receive
    case leader
    when /1$/
      controller1.stop
      listen.receive.should match /2$/
      sleep 0.1.seconds
      controller2.stop
    when /2$/
      controller2.stop
      listen.receive.should match /1$/
      sleep 0.1.seconds
      controller1.stop
    else fail("no leader elected")
    end
  end

  it "will release lease on shutdown" do
    config = LavinMQ::Config.new
    config.data_dir = "/tmp/release-lease"
    config.clustering = true
    config.clustering_etcd_endpoints = "localhost:12379"
    config.clustering_advertised_uri = "tcp://localhost:5681"
    launcher = LavinMQ::Launcher.new(config)

    election_done = Channel(Nil).new
    etcd = LavinMQ::Etcd.new(config.clustering_etcd_endpoints)
    spawn do
      etcd.elect_listen("lavinmq/leader") { election_done.close }
    end

    spawn { launcher.run }

    # Wait until our "launcher" is leader
    election_done.receive?

    # The spec gets a lease to use in an election campaign
    lease = etcd.lease_grant(5)

    # graceful stop...
    spawn { launcher.stop }

    # Let the spec campaign for leadership...
    elected = Channel(Nil).new
    spawn do
      etcd.election_campaign("lavinmq/leader", "spec", lease.id)
      elected.close
    end

    # ... and verify spec is elected
    select
    when elected.receive?
    when timeout(1.seconds)
      fail("election campaign did not finish in time, leadership not released on launcher stop?")
    end
  end

  it "wont deadlock under high load when a follower disconnects [#926]" do
    LavinMQ::Config.instance.clustering_max_unsynced_actions = 1
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379"), 0)
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

    appended = Channel(Bool).new
    spawn do
      # Fill the action queue
      loop do
        replicator.append("path", 1)
        appended.send true
      rescue Channel::ClosedError
        break
      end
    end

    # Wait for the action queue to fill up
    loop do
      select
      when appended.receive?
      when timeout 0.1.seconds
        # @action is a Channel. Let's look at its internal deque
        action_queue = replicator.@followers.first.@actions.@queue.not_nil!("no deque? no follower?")
        break if action_queue.size == action_queue.@capacity # full?
      end
    end

    # Now disconnect the follower. Our "fill action queue" fiber should continue
    client_io.close

    select
    when appended.receive?
    when timeout 0.1.seconds
      replicator.@followers.first.@actions.close
      deadlock = true
    end

    appended.close
    if deadlock
      fail "deadlock detected"
    end
  ensure
    replicator.try &.close
  end
end
