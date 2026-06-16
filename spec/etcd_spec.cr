require "spec"
require "../src/lavinmq/etcd"
require "../src/lavinmq/clustering/etcd_backend"
require "file_utils"
require "http/client"
require "./spec_helper"

describe LavinMQ::Etcd, tags: "etcd" do
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

  describe "#put_or_get" do
    it "should set and return value if key is non-existent", tags: "slow" do
      cluster = EtcdCluster.new(1)
      cluster.run do
        etcd = LavinMQ::Etcd.new(cluster.endpoints)
        etcd.del("foo")
        etcd.put_or_get("foo", "bar").should eq "bar"
        etcd.get("foo").should eq "bar"
      end
    end

    it "should get existing value if key exists", tags: "slow" do
      cluster = EtcdCluster.new(1)
      cluster.run do
        etcd = LavinMQ::Etcd.new(cluster.endpoints)
        etcd.put("foo", "bar")
        etcd.put_or_get("foo", "baz").should eq "bar"
        etcd.get("foo").should eq "bar"
      end
    end
  end

  it "can watch", tags: "slow" do
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
      rescue SpecExit
        # expect this when etcd nodes are terminated
      end
      w.receive # sync
      sleep 50.milliseconds
      etcd.put "foo", "bar"
      w.receive.should eq "bar"
      etcd.put "foo", "rab"
      w.receive.should eq "rab"
      etcd.del "foo"
      w.receive.should eq nil
    end
  end

  it "can elect leader", tags: "slow" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      leader = Channel(String?).new
      key = "foo/#{rand}"
      spawn(name: "etcd elect leader spec") do
        etcd.elect_listen(key) do |value|
          leader.send value
        end
      rescue SpecExit
        # expect this when etcd nodes are terminated
      end
      lease = etcd.elect(key, "bar", 1)
      leader.receive.should eq "bar"
      spawn(name: "elect other leader spec") do
        begin
          etcd.elect(key, "bar2", 1)
        rescue SpecExit
          # expect this when etcd nodes are terminated
        end
      end
      select
      when new = leader.receive
        fail "should not lose the leadership to #{new}"
      when timeout(2.seconds)
      end
      lease.release
    end
  end

  it "will lose lease when loosing quorum", tags: "slow" do
    cluster = EtcdCluster.new
    cluster.run do |etcds|
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", ttl: 1)
      etcds.first(2).each &.terminate(graceful: false)

      expect_raises(LavinMQ::Etcd::Lease::Expired) do
        lease.wait(20.seconds)
      end
    end
  end

  it "signals expired channel with the underlying error", tags: "slow" do
    cluster = EtcdCluster.new
    cluster.run do |etcds|
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", ttl: 1)
      etcds.first(2).each &.terminate(graceful: false)

      select
      when err = lease.expired.receive?
        err.should be_a(LavinMQ::Etcd::Error)
      when timeout(20.seconds)
        fail "lease.expired channel never signaled"
      end
    end
  end

  it "signals expired channel when all lease is revoked", tags: "slow" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", ttl: 10)

      etcd.lease_revoke(lease.id)

      select
      when err = lease.expired.receive?
        err.should be_a(LavinMQ::Etcd::Error)
      when timeout(20.seconds)
        fail "lease.expired channel never signaled"
      end
    end
  end

  it "will not lose leadership when only one etcd node is lost", tags: "slow" do
    cluster = EtcdCluster.new
    cluster.run do |etcds|
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      key = "foo/#{rand}"
      lease = etcd.elect(key, "bar", ttl: 1)
      etcds.sample.terminate(graceful: false)

      lease.wait(6.seconds) # should not lose the leadership
    end
  end

  it "raises LeaseNotFound when using an invalid lease", tags: "slow" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      etcd = LavinMQ::Etcd.new(cluster.endpoints)
      expect_raises(LavinMQ::Etcd::LeaseNotFound) do
        etcd.election_campaign("test/leader", "node1", lease: 999999i64)
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

  describe "basic auth and TLS support" do
    it "maintains backwards compatibility with endpoints getter" do
      etcd = LavinMQ::Etcd.new("http://user:pass@127.0.0.1:2379,localhost:2380")
      endpoints = etcd.endpoints
      endpoints.should eq ["127.0.0.1:2379", "localhost:2380"]
    end

    it "handles traditional host:port format" do
      etcd = LavinMQ::Etcd.new("127.0.0.1:2379,localhost:2380")
      endpoints = etcd.endpoints
      endpoints.should eq ["127.0.0.1:2379", "localhost:2380"]
    end

    it "uses default port when not specified" do
      etcd = LavinMQ::Etcd.new("127.0.0.1")
      endpoints = etcd.endpoints
      endpoints.should eq ["127.0.0.1:2379"]
    end

    it "supports HTTPS endpoints with auth" do
      etcd = LavinMQ::Etcd.new("https://user:pass@etcd.example.com:2379")
      endpoints = etcd.endpoints
      endpoints.should eq ["etcd.example.com:2379"]
    end

    it "supports mixed HTTP/HTTPS endpoints" do
      etcd = LavinMQ::Etcd.new("https://user:pass@secure.etcd.com:2379,http://insecure.etcd.com:2380,127.0.0.1:2381")
      endpoints = etcd.endpoints
      endpoints.should eq ["secure.etcd.com:2379", "insecure.etcd.com:2380", "127.0.0.1:2381"]
    end

    it "uses default port for HTTPS URLs" do
      etcd = LavinMQ::Etcd.new("https://user:pass@etcd.example.com")
      endpoints = etcd.endpoints
      endpoints.should eq ["etcd.example.com:2379"]
    end
  end
end

private def etcd_backend_config(data_dir, endpoints, prefix, advertised_uri, node_id)
  File.write(File.join(data_dir, ".clustering_id"), node_id.to_s(36))
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering = true
  config.clustering_etcd_endpoints = endpoints
  config.clustering_etcd_prefix = prefix
  config.clustering_advertised_uri = advertised_uri
  config
end

describe LavinMQ::Clustering::EtcdBackend do
  it "returns false from update_isr before winning the election (unarmed fence)" do
    with_datadir do |dir|
      config = LavinMQ::Config.new
      config.data_dir = dir
      config.clustering_etcd_endpoints = "localhost:12379"
      backend = LavinMQ::Clustering::EtcdBackend.new(config)
      # No election token yet: the fence short-circuits before any etcd write.
      backend.update_isr(Set{1, 2}).should be_false
    end
  end

  it "fences ISR writes on the election key: a deposed leader's update_isr returns false", tags: "etcd" do
    cluster = EtcdCluster.new(1)
    cluster.run do
      prefix = "lavinmq/#{rand}"
      isr_key = "#{prefix}/isr"
      with_datadir do |dir_a|
        with_datadir do |dir_b|
          backend_a = LavinMQ::Clustering::EtcdBackend.new(
            etcd_backend_config(dir_a, cluster.endpoints, prefix, "tcp://localhost:6001", 1))
          backend_b = LavinMQ::Clustering::EtcdBackend.new(
            etcd_backend_config(dir_b, cluster.endpoints, prefix, "tcp://localhost:6002", 2))

          test_etcd = LavinMQ::Etcd.new(cluster.endpoints)
          # Each backend's election binds to a lease keyed by its node id.
          lease_a = test_etcd.lease_grant(id: backend_a.node_id)
          lease_b = test_etcd.lease_grant(id: backend_b.node_id)
          begin
            # A wins, arms its fence, commits an ISR.
            backend_a.win_election
            backend_a.update_isr(Set{1, 2}).should be_true
            test_etcd.get(isr_key).should eq "1,2"

            # A is deposed: releasing its lease frees the election key. B wins.
            lease_a.release
            elected = Channel(Nil).new(1)
            spawn(name: "fence spec b") do
              backend_b.win_election
              elected.send(nil)
            end
            select
            when elected.receive
            when timeout(5.seconds)
              fail "B did not win the election after A's lease was released"
            end

            # A's fenced write is rejected (election key is no longer its lease)
            # and returns false instead of overwriting; B's write succeeds.
            backend_a.update_isr(Set{9}).should be_false
            test_etcd.get(isr_key).should eq "1,2"
            backend_b.update_isr(Set{2}).should be_true
            test_etcd.get(isr_key).should eq "2"
          ensure
            lease_b.release rescue nil
          end
        end
      end
    end
  end
end

class EtcdCluster
  @ports : Array(Int32)

  def initialize(nodes = 3)
    @ports = (100..999).sample(nodes)
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
    ensure_etcd_in_path!
    @ports.map_with_index do |p, i|
      start_process(p, i)
    end
  end

  def start_process(port, node_number)
    initial_cluster = @ports.map_with_index { |p, i| "l#{i}=http://127.0.0.1:24#{p}" }.join(",")
    FileUtils.rm_rf "/tmp/l#{node_number}.etcd"
    Process.new("etcd", {
      "--name=l#{node_number}",
      "--data-dir=/tmp/l#{node_number}.etcd",
      "--listen-peer-urls=http://127.0.0.1:24#{port}",
      "--listen-client-urls=http://127.0.0.1:23#{port}",
      "--advertise-client-urls=http://127.0.0.1:23#{port}",
      "--initial-advertise-peer-urls=http://127.0.0.1:24#{port}",
      "--initial-cluster", initial_cluster,
      "--logger=zap",
      "--log-level=error",
      "--unsafe-no-fsync=true",
    }, output: STDOUT, error: STDERR)
  end

  def stop(etcds)
    etcds.each { |p| p.terminate(graceful: false) if p.exists? }
    etcds.size.times { |i| FileUtils.rm_rf "/tmp/l#{i}.etcd" }
  end

  private def wait_until_online(retries = 3)
    @ports.each_with_index do |port, idx|
      i = 0
      client = HTTP::Client.new("127.0.0.1", 23000 + port)
      loop do
        sleep 20.milliseconds
        response = client.get("/version")
        if response.status.ok?
          next if response.body.includes? "not_decided"
          break
        end
      rescue e : Exception
        i += 1
        if i >= 100
          retries -= 1
          raise "Etcd on port #{23000 + port} not up? (#{e.message})" if retries == 0

          start_process(port, idx)
          wait_until_online(retries)
        end
        next
      end
    end
  end
end
