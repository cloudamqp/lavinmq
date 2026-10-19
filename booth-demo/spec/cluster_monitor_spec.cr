require "./spec_helper"

private def monitor
  nodes = [
    Booth::Config::Node.new("pi-1", "10.0.0.1"),
    Booth::Config::Node.new("pi-2", "10.0.0.2"),
    Booth::Config::Node.new("pi-3", "10.0.0.3"),
  ]
  m = Booth::ClusterMonitor.new(Booth::Config.new(nodes), Booth::Events.new)
  m.nodes.each &.online = true
  m
end

describe Booth::Prometheus do
  it "reads unlabeled samples and skips the rest" do
    metrics = Booth::Prometheus.parse(<<-TEXT)
      # HELP lavinmq_raft_is_leader 1 if this node is the leader
      # TYPE lavinmq_raft_is_leader gauge
      lavinmq_raft_is_leader 1
      lavinmq_raft_term 7
      lavinmq_raft_peer_connected{peer="abc"} 1
      TEXT
    metrics["lavinmq_raft_is_leader"].should eq 1
    metrics["lavinmq_raft_term"].should eq 7
    metrics.size.should eq 2
  end
end

describe Booth::ClusterMonitor do
  it "takes the host out of raft and replication addresses" do
    Booth::ClusterMonitor.address_host("10.0.0.1:5680").should eq "10.0.0.1"
    Booth::ClusterMonitor.address_host("tcp://pi-1.local:5679").should eq "pi-1.local"
    Booth::ClusterMonitor.address_host("[::1]:5680").should eq "::1"
  end

  it "maps the raft cluster status onto the nodes" do
    m = monitor
    m.apply_raft_status(JSON.parse(<<-JSON))
      {"leader":"10.0.0.2:5680","term":7,"isr":["a","b"],"members":[
        {"address":"10.0.0.1:5680","node_id":"a","role":"voter","in_isr":true,"match_index":9,"caught_up":true,"leader":false},
        {"address":"10.0.0.2:5680","node_id":"b","role":"voter","in_isr":true,"match_index":9,"caught_up":true,"leader":true},
        {"address":"10.0.0.3:5680","node_id":"c","role":"learner","in_isr":false,"match_index":3,"caught_up":false,"leader":false}]}
      JSON
    m.term.should eq 7
    m.leader.try(&.name).should eq "pi-2"
    m.nodes.map(&.state).should eq ["in_sync", "leader", "syncing"]
    m.nodes[2].voter.should be_false
  end

  it "doesn't name an offline node leader from a stale status" do
    m = monitor
    m.nodes[1].online = false
    m.apply_raft_status(JSON.parse(%({"leader":"10.0.0.2:5680","term":3,"members":[]})))
    m.leader.should be_nil
  end

  it "uses the leader's followers with the etcd backend" do
    m = monitor
    m.nodes[0].leader = true
    m.apply_etcd_followers(JSON.parse(<<-JSON), "10.0.0.1")
      [{"name":"pi-1","followers":[{"id":"x","remote_address":"10.0.0.3:41234","lag_in_bytes":0}]}]
      JSON
    m.nodes.map(&.state).should eq ["leader", "syncing", "in_sync"]
  end

  it "orders hosts leader first, offline nodes last" do
    m = monitor
    m.nodes[0].online = false
    m.nodes[2].leader = true
    m.hosts_leader_first.should eq ["10.0.0.3", "10.0.0.2", "10.0.0.1"]
  end
end
