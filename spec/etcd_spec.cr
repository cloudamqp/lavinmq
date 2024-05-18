require "spec"
require "../src/lavinmq/etcd"
require "file_utils"

describe LavinMQ::Etcd do
  it "can put and get" do
    cluster = EtcdCluster.new
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      etcd.del("foo")
      etcd.put("foo", "bar").should eq nil
      etcd.get("foo").should eq "bar"
      etcd.put("foo", "bar2").should eq "bar"
    end
  end

  it "can watch" do
    cluster = EtcdCluster.new
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      w = Channel(String?).new
      spawn do
        etcd.watch("foo") do |val|
          w.send val
        end
      end
      etcd.put "foo", "bar"
      w.receive.should eq "bar"
      etcd.put "foo", "rab"
      w.receive.should eq "rab"
      etcd.del "foo"
      w.receive.should eq nil
    end
  end

  it "can elect leader" do
    cluster = EtcdCluster.new
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      leader = Channel(String).new
      key = "foo/#{rand}"
      spawn do
        etcd.elect_listen(key) do |value|
          leader.send value
        end
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
      when timeout(10.seconds)
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
      when timeout(9.seconds)
      end
    end
  end

  it "learns new cluster endpoints" do
    cluster = EtcdCluster.new
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints.split(",").first)
      etcd.get("foo") # have to do a connection to be able to learn about new endpoints
      endpoints = etcd.endpoints.sort!
      endpoints.should eq ["127.0.0.1:23790", "127.0.0.1:23791", "127.0.0.1:23792"]
    end
  end
end

class EtcdCluster
  def initialize(@nodes = 3)
  end

  def endpoints
    @nodes.times.map { |i| "127.0.0.1:2379#{i}" }.join(",")
  end

  def run(&)
    etcds = start
    sleep 0.1
    begin
      yield etcds
    ensure
      stop(etcds)
    end
  end

  def start : Array(Process)
    cluster_token = "etcd#{rand(Int32::MAX)}"
    initial_cluster = @nodes.times.map { |i| "l#{i}=http://127.0.0.1:2380#{i}" }.join(",")
    Array(Process).new(@nodes) do |i|
      Process.new("etcd", {
        "--name=l#{i}",
        "--data-dir=/tmp/l#{i}.etcd",
        "--listen-peer-urls=http://127.0.0.1:2380#{i}",
        "--listen-client-urls=http://127.0.0.1:2379#{i}",
        "--advertise-client-urls=http://127.0.0.1:2379#{i}",
        "--initial-advertise-peer-urls=http://127.0.0.1:2380#{i}",
        "--initial-cluster-token=#{cluster_token}",
        "--initial-cluster-state=new",
        "--initial-cluster", initial_cluster,
        "--logger=zap",
        "--log-level=error",
      }, output: STDOUT, error: STDERR)
    end
  end

  def stop(etcds)
    etcds.each { |p| p.terminate(graceful: false) if p.exists? }
    etcds.each { |i| FileUtils.rm_rf "/tmp/l#{i}.etcd" }
  end
end
