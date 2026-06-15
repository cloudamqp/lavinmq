require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/control_session"

alias ControlSession = LavinMQ::Clustering::VR::ControlSession
alias Ctl = LavinMQ::Clustering::VR::Control

describe LavinMQ::Clustering::VR::ControlSession do
  it "handshakes and exchanges messages in both directions" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port
    accepted = Channel(ControlSession?).new
    spawn { accepted.send ControlSession.accept(server.accept, "secret") }

    dialer = ControlSession.dial(TCPSocket.new("127.0.0.1", port), "secret", self_id: 1, peer_id: 2)
    dialer.should_not be_nil
    acceptor = accepted.receive
    acceptor.should_not be_nil
    dialer = dialer.not_nil!
    acceptor = acceptor.not_nil!

    dialer.peer_id.should eq 2   # dialer knew the target id
    acceptor.peer_id.should eq 1 # acceptor learned the dialer's id

    hb = Ctl::Heartbeat.new(view: 4u64, op: 50u64, commit_op: 49u64, from_id: 1)
    dialer.send(hb)
    acceptor.receive.should eq hb

    svc = Ctl::StartViewChange.new(view: 5u64, from_id: 2)
    acceptor.send(svc)
    dialer.receive.should eq svc
  ensure
    server.try &.close
  end

  it "rejects a wrong secret" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port
    accepted = Channel(ControlSession?).new
    spawn { accepted.send ControlSession.accept(server.accept, "right") }

    expect_raises(LavinMQ::Clustering::AuthenticationError) do
      ControlSession.dial(TCPSocket.new("127.0.0.1", port), "wrong", self_id: 1, peer_id: 2)
    end
    accepted.receive.should be_nil
  ensure
    server.try &.close
  end
end
