require "../spec_helper"
require "uri"
require "../../src/lavinmq/raft/bootstrap_decision"

private def uris : Array(URI)
  [] of URI
end

private def uris(*hosts : String) : Array(URI)
  hosts.map { |h| URI.parse("http://#{h}:15672") }.to_a
end

describe LavinMQ::Raft::BootstrapDecision do
  it "resumes when the node already has peers" do
    LavinMQ::Raft::BootstrapDecision.decide("a", uris("a", "b"), has_peers: true)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Resume
  end

  it "bootstraps (legacy) when no seeds are configured" do
    LavinMQ::Raft::BootstrapDecision.decide("a", uris, has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Bootstrap
  end

  it "bootstraps when our advertised host is the lowest seed" do
    LavinMQ::Raft::BootstrapDecision.decide("a", uris("c", "a", "b"), has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Bootstrap
  end

  it "joins when present but not the lowest seed" do
    LavinMQ::Raft::BootstrapDecision.decide("b", uris("a", "b", "c"), has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Join
  end

  it "joins (fail-safe) when our host is absent from the seed list" do
    LavinMQ::Raft::BootstrapDecision.decide("z", uris("a", "b", "c"), has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Join
  end

  it "joins (fail-safe) when the lowest seed host is ambiguous (duplicated)" do
    seeds = ["http://a:15672", "http://a:5555", "http://b:15672"].map { |s| URI.parse(s) }
    LavinMQ::Raft::BootstrapDecision.decide("a", seeds, has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Join
  end

  it "normalizes host case and IPv6 brackets when matching" do
    LavinMQ::Raft::BootstrapDecision.decide("A", uris("B", "A"), has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Bootstrap
    seeds = ["http://[::1]:15672", "http://node-b:15672"].map { |s| URI.parse(s) }
    LavinMQ::Raft::BootstrapDecision.decide("::1", seeds, has_peers: false)
      .should eq LavinMQ::Raft::BootstrapDecision::Action::Bootstrap
  end
end
