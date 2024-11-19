require "./spec_helper"
require "../src/lavinmq/clustering/client"
require "../src/lavinmq/clustering/controller"

alias IndexTree = LavinMQ::MQTT::TopicTree(String)

describe LavinMQ::Clustering::Client do
  follower_data_dir = "/tmp/lavinmq-follower"

  around_each do |spec|
    FileUtils.rm_rf follower_data_dir
    p = Process.new("etcd", {
      "--data-dir=/tmp/clustering-spec.etcd",
      "--logger=zap",
      "--log-level=error",
      "--unsafe-no-fsync=true",
      "--force-new-cluster=true",
    }, output: STDOUT, error: STDERR)

    client = HTTP::Client.new("127.0.0.1", 2379)
    i = 0
    loop do
      sleep 0.02.seconds
      response = client.get("/version")
      if response.status.ok?
        next if response.body.includes? "not_decided"
        break
      end
    rescue e : Socket::ConnectError
      i += 1
      raise "Cant connect to etcd on port 2379. Giving up after 100 tries. (#{e.message})" if i >= 100
      next
    end
    client.close
    begin
      spec.run
    ensure
      p.terminate(graceful: false)
      FileUtils.rm_rf "/tmp/clustering-spec.etcd"
      FileUtils.rm_rf follower_data_dir
    end
  end

  it "can stream changes" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new, 0)
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

    server = LavinMQ::Server.new(follower_data_dir)
    begin
      q = server.vhosts["/"].queues["repli"].as(LavinMQ::AMQP::DurableQueue)
      q.message_count.should eq 1
      q.basic_get(true) do |env|
        String.new(env.message.body).to_s.should eq "hello world"
      end.should be_true
    ensure
      server.close
    end
  end

  it "replicates and streams retained messages to followers" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new, 0)
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
    props = LavinMQ::AMQP::Properties.new
    msg1 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body1"))
    msg2 = LavinMQ::Message.new(100, "test", "rk", props, 10, IO::Memory.new("body2"))
    retain_store.retain("topic1", msg1.body_io, msg1.bodysize)
    retain_store.retain("topic2", msg2.body_io, msg2.bodysize)

    wait_for(10.seconds) { replicator.followers.first?.try &.lag_in_bytes == 0 }
    repli.close
    done.receive

    follower_retain_store = LavinMQ::MQTT::RetainStore.new("#{follower_data_dir}/retain_store", LavinMQ::Clustering::NoopServer.new)
    a = Array(String).new(2)
    b = Array(String).new(2)
    follower_retain_store.each("#") do |topic, bytes|
      a << topic
      b << String.new(bytes)
    end

    a.sort!.should eq(["topic1", "topic2"])
    b.sort!.should eq(["body1", "body2"])
    follower_retain_store.retained_messages.should eq(2)
  ensure
    replicator.try &.close
  end

  it "can stream full file" do
    replicator = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, LavinMQ::Etcd.new, 0)
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

    server = LavinMQ::Server.new(follower_data_dir)
    begin
      server.users["u1"].should_not be_nil
    ensure
      server.close
    end
  end

  it "will failover" do
    config1 = LavinMQ::Config.new
    config1.data_dir = "/tmp/failover1"
    config1.clustering_advertised_uri = "tcp://localhost:5681"
    config1.clustering_port = 5681
    config1.amqp_port = 5671
    config1.http_port = 15671
    controller1 = LavinMQ::Clustering::Controller.new(config1)

    config2 = LavinMQ::Config.new
    config2.data_dir = "/tmp/failover2"
    config2.clustering_advertised_uri = "tcp://localhost:5682"
    config2.clustering_port = 5682
    config2.amqp_port = 5672
    config2.http_port = 15672
    controller2 = LavinMQ::Clustering::Controller.new(config2)

    listen = Channel(String).new
    spawn(name: "etcd elect leader spec") do
      etcd = LavinMQ::Etcd.new
      etcd.elect_listen("lavinmq/leader") do |value|
        listen.send value
      end
    rescue LavinMQ::Etcd::Error
      # expect this when etcd nodes are terminated
    end
    sleep 0.5.seconds
    spawn(name: "failover1") do
      controller1.run
    end
    spawn(name: "failover2") do
      controller2.run
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
end
