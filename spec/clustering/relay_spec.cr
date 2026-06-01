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
end
