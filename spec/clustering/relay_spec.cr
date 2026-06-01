require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/client"

# A relay is a node that follows an upstream (foreign-region) leader as a
# Clustering::Client while re-serving the same stream to its own downstream
# followers via a Clustering::Server. This is the building block for
# cross-region disaster recovery: only one node per DR region crosses the
# region boundary, the rest replicate from it locally.
describe "Clustering relay (DR cascade)", tags: "etcd" do
  add_etcd_around_each

  it "cascades changes from an upstream leader through a relay to a downstream follower" do
    etcd = LavinMQ::Etcd.new("localhost:12379")
    base = LavinMQ::Config.instance.dup
    base.metrics_http_port = -1 # don't start a metrics server per client

    leader_dir = File.tempname("lavinmq", "leader")
    relay_dir = File.tempname("lavinmq", "relay")
    downstream_dir = File.tempname("lavinmq", "downstream")
    Dir.mkdir_p leader_dir
    Dir.mkdir_p relay_dir
    Dir.mkdir_p downstream_dir

    # --- Upstream "region A" leader ---
    leader_config = base.dup
    leader_config.data_dir = leader_dir
    leader_config.clustering_etcd_prefix = "regionA"
    leader = LavinMQ::Clustering::Server.new(leader_config, LavinMQ::Clustering::EtcdCoordinator.new(leader_config, etcd), 0)
    leader_tcp = TCPServer.new("localhost", 0)
    spawn(leader.listen(leader_tcp), name: "leader listen spec")

    # A file that exists before the relay connects; it must reach the
    # downstream follower through the relay's full-sync, exercising
    # register_data_dir.
    File.write(File.join(leader_dir, "definitions.json"), "hello")
    leader.replace_file(File.join(leader_dir, "definitions.json"))

    # --- "region B" relay: a downstream server fed by an upstream client ---
    relay_config = base.dup
    relay_config.data_dir = relay_dir
    relay_config.clustering_etcd_prefix = "regionB"
    relay_server = LavinMQ::Clustering::Server.new(relay_config, LavinMQ::Clustering::EtcdCoordinator.new(relay_config, etcd), 1)
    relay_tcp = TCPServer.new("localhost", 0)
    spawn(relay_server.listen(relay_tcp), name: "relay listen spec")

    relay_client = LavinMQ::Clustering::Client.new(
      relay_config, 1, leader.password, proxy: false, relay: relay_server)
    spawn(relay_client.follow("localhost", leader_tcp.local_address.port), name: "relay follow spec")

    wait_for { leader.followers.size == 1 }    # relay fully synced to the leader
    wait_for { relay_server.nr_of_files >= 1 } # relay registered its data dir

    # --- "region B" downstream follower of the relay ---
    downstream_config = base.dup
    downstream_config.data_dir = downstream_dir
    downstream_config.clustering_etcd_prefix = "regionB"
    downstream_client = LavinMQ::Clustering::Client.new(
      downstream_config, 2, relay_server.password, proxy: false)
    spawn(downstream_client.follow("localhost", relay_tcp.local_address.port), name: "downstream follow spec")

    wait_for { relay_server.followers.size == 1 } # downstream fully synced to relay

    # The pre-existing file made it all the way through the cascade via full-sync.
    wait_for { File.exists?(File.join(downstream_dir, "definitions.json")) }
    File.read(File.join(downstream_dir, "definitions.json")).should eq "hello"

    # A streamed append on the leader cascades through the relay to downstream.
    leader.append(File.join(leader_dir, "messages.dat"), "ABC".to_slice)
    wait_for do
      path = File.join(downstream_dir, "messages.dat")
      File.exists?(path) && File.read(path) == "ABC"
    end

    # A streamed file replace cascades too.
    File.write(File.join(leader_dir, "definitions.json"), "world")
    leader.replace_file(File.join(leader_dir, "definitions.json"))
    wait_for { (File.read(File.join(downstream_dir, "definitions.json")) == "world") rescue false }

    # A streamed delete cascades too.
    leader.delete_file(File.join(leader_dir, "messages.dat"))
    wait_for { !File.exists?(File.join(downstream_dir, "messages.dat")) }
  ensure
    downstream_client.try &.close
    relay_client.try &.close
    relay_server.try &.close
    leader.try &.close
    FileUtils.rm_rf leader_dir if leader_dir
    FileUtils.rm_rf relay_dir if relay_dir
    FileUtils.rm_rf downstream_dir if downstream_dir
  end

  # A downstream follower that connects before the relay has synced from its
  # upstream leader must not full-sync against the relay's empty/stale index and
  # wipe its own data dir. In DR mode the relay gates downstream full-syncs until
  # its first upstream sync completes.
  it "does not serve a downstream full-sync until the relay has synced from upstream" do
    etcd = LavinMQ::Etcd.new("localhost:12379")
    base = LavinMQ::Config.instance.dup
    base.metrics_http_port = -1

    leader_dir = File.tempname("lavinmq", "leader")
    relay_dir = File.tempname("lavinmq", "relay")
    downstream_dir = File.tempname("lavinmq", "downstream")
    Dir.mkdir_p leader_dir
    Dir.mkdir_p relay_dir
    Dir.mkdir_p downstream_dir

    # --- Upstream "region A" leader with a pre-existing file ---
    leader_config = base.dup
    leader_config.data_dir = leader_dir
    leader_config.clustering_etcd_prefix = "regionA"
    leader = LavinMQ::Clustering::Server.new(leader_config, LavinMQ::Clustering::EtcdCoordinator.new(leader_config, etcd), 0)
    leader_tcp = TCPServer.new("localhost", 0)
    spawn(leader.listen(leader_tcp), name: "leader listen spec")
    File.write(File.join(leader_dir, "definitions.json"), "hello")
    leader.replace_file(File.join(leader_dir, "definitions.json"))

    # --- "region B" relay in DR mode: gates downstream until upstream sync ---
    relay_config = base.dup
    relay_config.data_dir = relay_dir
    relay_config.clustering_etcd_prefix = "regionB"
    relay_server = LavinMQ::Clustering::Server.new(relay_config, LavinMQ::Clustering::EtcdCoordinator.new(relay_config, etcd), 1)
    relay_server.relay_mode!
    relay_tcp = TCPServer.new("localhost", 0)
    spawn(relay_server.listen(relay_tcp), name: "relay listen spec")

    # --- Downstream follower connects BEFORE the relay has synced upstream ---
    # It carries a sentinel file that exists nowhere upstream.
    File.write(File.join(downstream_dir, "sentinel"), "keep")
    downstream_config = base.dup
    downstream_config.data_dir = downstream_dir
    downstream_config.clustering_etcd_prefix = "regionB"
    downstream_client = LavinMQ::Clustering::Client.new(
      downstream_config, 2, relay_server.password, proxy: false)
    spawn(downstream_client.follow("localhost", relay_tcp.local_address.port), name: "downstream follow spec")

    # The gate holds: no full-sync runs, so the downstream data dir is untouched.
    sleep 0.2.seconds
    relay_server.followers.size.should eq 0
    File.exists?(File.join(downstream_dir, "sentinel")).should be_true

    # Now let the relay sync from the upstream leader.
    relay_client = LavinMQ::Clustering::Client.new(
      relay_config, 1, leader.password, proxy: false, relay: relay_server)
    spawn(relay_client.follow("localhost", leader_tcp.local_address.port), name: "relay follow spec")

    # Once the relay is synced the gate opens and the downstream full-syncs the
    # correct dataset: it gets the leader's file and drops its sentinel (absent
    # upstream) — not an empty wipe against an unready relay.
    wait_for { relay_server.followers.size == 1 }
    wait_for { File.exists?(File.join(downstream_dir, "definitions.json")) }
    File.read(File.join(downstream_dir, "definitions.json")).should eq "hello"
    wait_for { !File.exists?(File.join(downstream_dir, "sentinel")) }
  ensure
    downstream_client.try &.close
    relay_client.try &.close
    relay_server.try &.close
    leader.try &.close
    FileUtils.rm_rf leader_dir if leader_dir
    FileUtils.rm_rf relay_dir if relay_dir
    FileUtils.rm_rf downstream_dir if downstream_dir
  end

  # When the relay loses its upstream link and later reconnects, its catch-up
  # full-sync can replace/delete files that were never streamed to already
  # connected downstream followers. The relay must force those followers to
  # re-sync so the DR copy doesn't silently diverge.
  it "re-syncs connected downstream followers after the relay reconnects to upstream" do
    etcd = LavinMQ::Etcd.new("localhost:12379")
    base = LavinMQ::Config.instance.dup
    base.metrics_http_port = -1

    leader_dir = File.tempname("lavinmq", "leader")
    relay_dir = File.tempname("lavinmq", "relay")
    downstream_dir = File.tempname("lavinmq", "downstream")
    Dir.mkdir_p leader_dir
    Dir.mkdir_p relay_dir
    Dir.mkdir_p downstream_dir

    leader_config = base.dup
    leader_config.data_dir = leader_dir
    leader_config.clustering_etcd_prefix = "regionA"
    leader = LavinMQ::Clustering::Server.new(leader_config, LavinMQ::Clustering::EtcdCoordinator.new(leader_config, etcd), 0)
    leader_tcp = TCPServer.new("localhost", 0)
    spawn(leader.listen(leader_tcp), name: "leader listen spec")
    File.write(File.join(leader_dir, "definitions.json"), "hello")
    leader.replace_file(File.join(leader_dir, "definitions.json"))

    relay_config = base.dup
    relay_config.data_dir = relay_dir
    relay_config.clustering_etcd_prefix = "regionB"
    relay_server = LavinMQ::Clustering::Server.new(relay_config, LavinMQ::Clustering::EtcdCoordinator.new(relay_config, etcd), 1)
    relay_server.relay_mode!
    relay_tcp = TCPServer.new("localhost", 0)
    spawn(relay_server.listen(relay_tcp), name: "relay listen spec")

    relay_client = LavinMQ::Clustering::Client.new(
      relay_config, 1, leader.password, proxy: false, relay: relay_server)
    spawn(relay_client.follow("localhost", leader_tcp.local_address.port), name: "relay follow spec")
    wait_for { leader.followers.size == 1 }

    downstream_config = base.dup
    downstream_config.data_dir = downstream_dir
    downstream_config.clustering_etcd_prefix = "regionB"
    downstream_client = LavinMQ::Clustering::Client.new(
      downstream_config, 2, relay_server.password, proxy: false)
    spawn(downstream_client.follow("localhost", relay_tcp.local_address.port), name: "downstream follow spec")
    wait_for { relay_server.followers.size == 1 }
    wait_for { (File.read(File.join(downstream_dir, "definitions.json")) == "hello") rescue false }

    # Simulate an upstream outage during which a change is missed: drop the
    # relay's upstream link while the downstream stays connected to the relay,
    # then mutate the leader. This change is never streamed to the relay.
    relay_client.close
    File.write(File.join(leader_dir, "definitions.json"), "world")
    leader.replace_file(File.join(leader_dir, "definitions.json"))

    # The relay reconnects with the same downstream Server. Its catch-up
    # full-sync pulls "world" and, being a reconnect, forces the downstream to
    # re-sync rather than leaving it stale on "hello".
    relay_client2 = LavinMQ::Clustering::Client.new(
      relay_config, 1, leader.password, proxy: false, relay: relay_server)
    spawn(relay_client2.follow("localhost", leader_tcp.local_address.port), name: "relay follow spec 2")

    wait_for { (File.read(File.join(downstream_dir, "definitions.json")) == "world") rescue false }
  ensure
    downstream_client.try &.close
    relay_client2.try &.close
    relay_client.try &.close
    relay_server.try &.close
    leader.try &.close
    FileUtils.rm_rf leader_dir if leader_dir
    FileUtils.rm_rf relay_dir if relay_dir
    FileUtils.rm_rf downstream_dir if downstream_dir
  end
end
