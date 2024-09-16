require "spec"
require "../src/lavinmq/etcd"
require "file_utils"

describe LavinMQ::Etcd do
  it "can put and get" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      etcd.del("foo")
      etcd.put("foo", "bar").should eq nil
      etcd.get("foo").should eq "bar"
      etcd.put("foo", "bar2").should eq "bar"
    end
  end

  it "can watch" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      w = Channel(String?).new(1)
      spawn(name: "etcd watch spec") do
        etcd.get("foo").should be_nil
        w.send nil
        etcd.watch("foo") do |val|
          w.send val
        end
      rescue LavinMQ::Etcd::Error
        # expect this when etcd nodes are terminated
      end
      w.receive # sync
      sleep 0.05.seconds
      etcd.put "foo", "bar"
      w.receive.should eq "bar"
      etcd.put "foo", "rab"
      w.receive.should eq "rab"
      etcd.del "foo"
      w.receive.should eq nil
    end
  end

  it "can elect leader" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      leader = Channel(String).new
      key = "foo/#{rand}"
      spawn(name: "etcd elect leader spec") do
        etcd.elect_listen(key) do |value|
          leader.send value
        end
      rescue LavinMQ::Etcd::Error
        # expect this when etcd nodes are terminated
      end
      lease = etcd.elect(key, "bar", 1)
      leader.receive.should eq "bar"
      spawn(name: "elect other leader spec") do
        begin
          etcd.elect(key, "bar2", 1)
        rescue LavinMQ::Etcd::Error
          # expect this when etcd nodes are terminated
        end
      end
      select
      when new = leader.receive
        fail "should not lose the leadership to #{new}"
      when timeout(2.seconds)
      end
      lease.close
    end
  end

  it "will lose leadership when loosing quorum" do
    cluster = EtcdCluster.new
    cluster.run do |etcds|
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", 1)
      etcds.first(2).each &.terminate(graceful: false)
      select
      when lease.receive?
      when timeout(15.seconds)
        fail "should lose the leadership"
      end
    end
  end

  it "will not lose leadership when only one etcd node is lost" do
    cluster = EtcdCluster.new
    cluster.run do |etcds|
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", 1)
      etcds.sample.terminate(graceful: false)
      select
      when lease.receive?
        fail "should not lose the leadership"
      when timeout(6.seconds)
      end
    end
  end

  pending "learns new cluster endpoints" do
    cluster = EtcdCluster.new
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints.split(",").first)
      etcd.get("foo") # have to do a connection to be able to learn about new endpoints
      endpoints = etcd.endpoints
      endpoints.size.should eq 3
      endpoints.each &.should start_with "127.0.0.1:23"
    end
  end
end

class EtcdCluster
  @ports : Array(Int32)

  def initialize(nodes = 3)
    @ports = nodes.times.map { rand(899) + 100 }.to_a
  end

  def endpoints
    @ports.map { |p| "127.0.0.1:23#{p}" }.join(",")
  end

  def run(&)
    etcds = start
    begin
      wait_until_online
      yield etcds
    ensure
      stop(etcds)
    end
  end

  def start : Array(Process)
    initial_cluster = @ports.map_with_index { |p, i| "l#{i}=http://127.0.0.1:24#{p}" }.join(",")
    @ports.map_with_index do |p, i|
      FileUtils.rm_rf "/tmp/l#{i}.etcd"
      Process.new("etcd", {
        "--name=l#{i}",
        "--data-dir=/tmp/l#{i}.etcd",
        "--listen-peer-urls=http://127.0.0.1:24#{p}",
        "--listen-client-urls=http://127.0.0.1:23#{p}",
        "--advertise-client-urls=http://127.0.0.1:23#{p}",
        "--initial-advertise-peer-urls=http://127.0.0.1:24#{p}",
        "--initial-cluster", initial_cluster,
        "--logger=zap",
        "--log-level=error",
        "--unsafe-no-fsync=true",
      }, output: STDOUT, error: STDERR)
    end
  end

  def stop(etcds)
    etcds.each { |p| p.terminate(graceful: false) if p.exists? }
    etcds.size.times { |i| FileUtils.rm_rf "/tmp/l#{i}.etcd" }
  end

  private def wait_until_online
    @ports.each do |port|
      i = 0
      client = HTTP::Client.new("127.0.0.1", 23000 + port)
      loop do
        sleep 0.02.seconds
        response = client.get("/version")
        if response.status.ok?
          next if response.body.includes? "not_decided"
          break
        end
      rescue e : Exception
        i += 1
        raise "Etcd on ort #{23000 + port} not up? (#{e.message})" if i >= 100
        next
      end
    end
  end
end
