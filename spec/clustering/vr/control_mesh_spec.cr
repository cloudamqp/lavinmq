require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/control_mesh"

alias ControlMesh = LavinMQ::Clustering::VR::ControlMesh
alias MeshMembership = LavinMQ::Clustering::VR::Membership

describe LavinMQ::Clustering::VR::ControlMesh do
  it "connects a two-node mesh and routes broadcast + targeted sends" do
    secret = "s3cr3t"
    # Node 2 listens; node 1 (lower id) dials it. Node 1's own port is never
    # dialed (nobody has a higher peer than 2 here), so any value works.
    server2 = TCPServer.new("127.0.0.1", 0)
    port2 = server2.local_address.port
    roster = "1=tcp://127.0.0.1:1,2=tcp://127.0.0.1:#{port2}"

    m1 = ControlMesh.new(MeshMembership.parse(roster, "tcp://127.0.0.1:1"), secret, reconnect_interval: 100.milliseconds)
    m2 = ControlMesh.new(MeshMembership.parse(roster, "tcp://127.0.0.1:#{port2}"), secret, reconnect_interval: 100.milliseconds)

    spawn(name: "mesh spec accept") do
      while socket = server2.accept?
        spawn { m2.handle_accept(socket) }
      end
    end
    m1.start # dials node 2

    wait_for { m1.connected_ids.includes?(2) && m2.connected_ids.includes?(1) }

    # node 1 broadcasts; node 2 receives it
    hb = Ctl::Heartbeat.new(view: 1u64, op: 9u64, commit_op: 8u64, from_id: 1)
    m1.broadcast(hb)
    select
    when msg = m2.inbound.receive
      msg.should eq hb
    when timeout(2.seconds)
      fail "node 2 never received the broadcast"
    end

    # node 2 targets node 1 over the same link
    svc = Ctl::StartViewChange.new(view: 2u64, from_id: 2)
    m2.send_to(1, svc)
    select
    when msg = m1.inbound.receive
      msg.should eq svc
    when timeout(2.seconds)
      fail "node 1 never received the targeted send"
    end
  ensure
    m1.try &.close
    m2.try &.close
    server2.try &.close
  end
end
