require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/static_members"
require "../../src/lavinmq/clustering/password_store"
require "../../src/lavinmq/clustering/vr_controller"
require "../../src/lavinmq/clustering/vr_state"
require "lz4"

private def sync_vr_follower(server, port, id : Int32, v2 = true) : TCPSocket
  io = TCPSocket.new("localhost", port)
  io.write(v2 ? LavinMQ::Clustering::StartV2 : LavinMQ::Clustering::Start)
  io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
  io.write server.password.to_slice
  io.read_byte.should eq 0
  io.write_bytes id, IO::ByteFormat::LittleEndian
  io.write_byte LavinMQ::Clustering::ConnectionKind::Replication.value if v2
  io.flush
  lz4 = Compress::LZ4::Reader.new(io)
  sha1_size = Digest::SHA1.new.digest_size
  2.times do
    loop do
      len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
      break if len.zero?
      lz4.skip len
      lz4.skip sha1_size
    end
    io.write_bytes 0i32
    io.flush
  end
  lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian) if v2
  io
end

private def vr_config(data_dir : String, node_id : Int32, members : String, secret = "vr-spec-secret")
  LavinMQ::Config.instance.dup.tap do |config|
    config.data_dir = data_dir
    config.clustering_members = members
    config.clustering_node_id = node_id
    config.clustering_secret = secret
    config.metrics_http_port = -1
  end
end

private def unused_local_port : Int32
  server = TCPServer.new("localhost", 0)
  server.local_address.port
ensure
  server.try &.close
end

private def start_vr_server(config, members, state, id : Int32, tcp_server)
  server = LavinMQ::Clustering::Server.new(
    config,
    LavinMQ::Clustering::VRCoordinator.new(config),
    id,
    members,
    state)
  spawn(server.listen(tcp_server), name: "vr spec server #{id}")
  server
end

private def vr_control_vote(server, port, requester_id : Int32, view : Int64, op_number : Int64)
  io = TCPSocket.new("localhost", port)
  io.write LavinMQ::Clustering::StartV2
  io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
  io.write server.password.to_slice
  io.read_byte.should eq 0
  io.write_bytes requester_id, IO::ByteFormat::LittleEndian
  io.write_byte LavinMQ::Clustering::ConnectionKind::Control.value
  io.write_byte LavinMQ::Clustering::ControlRequest::Vote.value
  io.write_bytes view, IO::ByteFormat::LittleEndian
  io.write_bytes requester_id, IO::ByteFormat::LittleEndian
  io.write_bytes op_number, IO::ByteFormat::LittleEndian
  io.flush
  {
    granted:   io.read_byte == 1,
    view:      io.read_bytes(Int64, IO::ByteFormat::LittleEndian),
    op_number: io.read_bytes(Int64, IO::ByteFormat::LittleEndian),
  }
ensure
  io.try &.close
end

class LavinMQ::Clustering::VRController
  def win_election_for_spec : Bool
    win_election?
  end

  def authority_status_for_spec(view : Int64) : {Bool, Int64}
    authority_status(view)
  end
end

describe LavinMQ::Clustering::StaticMembers do
  it "parses static members and picks primaries by view" do
    members = LavinMQ::Clustering::StaticMembers.parse("2=tcp://b:5679,1=tcp://a:5679,3=tcp://c:5679")
    members.ids.should eq [1, 2, 3]
    members.quorum_size.should eq 2
    members.primary_id(0).should eq 1
    members.primary_id(1).should eq 2
    members.primary_id(3).should eq 1
  end

  it "derives node id from advertised uri" do
    members = LavinMQ::Clustering::StaticMembers.parse("1=tcp://a:5679,2=tcp://b:5679")
    members.derive_node_id("tcp://b:5679").should eq 2
  end

  it "derives node id with case-insensitive hostnames and default tcp port" do
    members = LavinMQ::Clustering::StaticMembers.parse("1=tcp://node-a,2=tcp://node-b:5679")
    members.derive_node_id("tcp://NODE-A:5679").should eq 1
  end

  it "does not match unknown schemes when both ports are omitted" do
    members = LavinMQ::Clustering::StaticMembers.parse("1=custom://a")
    expect_raises(LavinMQ::Clustering::StaticMembers::Error, /does not match/) do
      members.derive_node_id("custom://a")
    end
  end
end

describe LavinMQ::Clustering::PasswordStore do
  it "creates the clustering password with mode 0600" do
    with_datadir do |data_dir|
      store = LavinMQ::Clustering::PasswordStore.new(data_dir)
      store.password("seed").should eq "seed"
      path = File.join(data_dir, ".clustering_password")
      mode = File.info(path).permissions.value & 0o777
      mode.should eq 0o600
    end
  end

  it "fails fast when an existing password differs from the configured seed" do
    with_datadir do |data_dir|
      path = File.join(data_dir, ".clustering_password")
      File.write(path, "existing")
      File.chmod(path, 0o600)
      store = LavinMQ::Clustering::PasswordStore.new(data_dir)
      expect_raises(LavinMQ::Clustering::PasswordStore::Error, /does not match/) do
        store.password("different")
      end
    end
  end
end

private def vr_state_path(data_dir : String) : String
  File.join(data_dir, ".clustering_vr_state")
end

private def vr_counter_path(data_dir : String, name : String) : String
  File.join(data_dir, ".clustering_vr_state.#{name}")
end

private def read_vr_counter(path : String) : Int64
  File.open(path) do |file|
    file.seek(File.size(path) - 8)
    file.read_bytes(Int64, IO::ByteFormat::LittleEndian)
  end
end

describe LavinMQ::Clustering::VRState do
  it "stores cold state separately from append-only counter files" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      state.next_op!.should eq 1
      state.commit!(1)

      path = vr_state_path(data_dir)
      json = JSON.parse(File.read(path))
      json["role"].as_s.should eq "primary"
      json["op_number"]?.should be_nil
      json["commit_number"]?.should be_nil

      op_path = vr_counter_path(data_dir, "op_number")
      commit_path = vr_counter_path(data_dir, "commit_number")
      read_vr_counter(op_path).should eq 1
      read_vr_counter(commit_path).should eq 1
      File.size(op_path).should eq 8
      File.size(commit_path).should eq 8
    end
  end

  it "ignores leftover temp state files from interrupted durable writes" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      state.next_op!
      state.commit!(1)
      File.write(File.join(data_dir, ".clustering_vr_state.interrupted.tmp"), "{")

      restored = LavinMQ::Clustering::VRState.new(data_dir)
      restored.role.should eq "primary"
      restored.op_number.should eq 1
      restored.commit_number.should eq 1
    end
  end

  it "applies operation numbers monotonically and persists them" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.apply_op!(3)
      state.apply_op!(2)

      restored = LavinMQ::Clustering::VRState.new(data_dir)
      restored.op_number.should eq 3
    end
  end

  it "restores the last complete record and compacts interrupted trailing writes" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.apply_op!(3)

      path = vr_counter_path(data_dir, "op_number")
      File.open(path, "a", &.write_byte(1_u8))

      restored = LavinMQ::Clustering::VRState.new(data_dir)
      restored.op_number.should eq 3
      restored.apply_op!(4)

      File.size(path).should eq 8
      read_vr_counter(path).should eq 4
    end
  end

  it "compacts appended counter records when they reach the configured size" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir, 1_i64)
      state.apply_op!(1)
      state.apply_op!(2)
      state.commit!(1)
      state.commit!(2)

      op_path = vr_counter_path(data_dir, "op_number")
      commit_path = vr_counter_path(data_dir, "commit_number")
      File.size(op_path).should eq 8
      File.size(commit_path).should eq 8
      read_vr_counter(op_path).should eq 2
      read_vr_counter(commit_path).should eq 2
      File.exists?("#{op_path}.tmp").should be_false
      File.exists?("#{commit_path}.tmp").should be_false
    end
  end

  it "persists one vote per view" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.grant_vote?(1, 5, 0, 1).should be_true

      restored = LavinMQ::Clustering::VRState.new(data_dir)
      restored.voted_view.should eq 5
      restored.voted_for.should eq 1
      restored.grant_vote?(1, 5, 0, 1).should be_true
      restored.grant_vote?(2, 5, 0, 2).should be_false
    end
  end

  it "denies stale candidates behind the local operation number" do
    with_datadir do |data_dir|
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.apply_op!(7)

      state.grant_vote?(1, 0, 6, 1).should be_false
      state.grant_vote?(1, 0, 7, 1).should be_true
    end
  end
end

describe LavinMQ::Clustering do
  it "marks local clustering metadata as non-replicated" do
    LavinMQ::Clustering.metadata_file?(".lock").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_id").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_password").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_password.abcd.tmp").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_vr_state").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_vr_state.op_number").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_vr_state.commit_number.tmp").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_vr_state.abcd.tmp").should be_true
    LavinMQ::Clustering.metadata_file?("definitions.amqp").should be_false
  end
end

describe LavinMQ::Clustering::VRController do
  it "fires leader-elected after start and leader-lost when stopped" do
    with_datadir do |data_dir|
      events_path = File.join(data_dir, "events")
      config = LavinMQ::Config.instance.dup
      config.data_dir = data_dir
      config.clustering_members = "1=tcp://localhost:5679"
      config.clustering_node_id = 1
      config.clustering_on_leader_elected = "printf elected >> #{events_path}"
      config.clustering_on_leader_lost = "printf lost >> #{events_path}"
      controller = LavinMQ::Clustering::VRController.new(config)
      started = Channel(Nil).new(1)
      done = Channel(Nil).new(1)

      spawn(name: "vr controller hook spec") do
        controller.run do
          File.write(events_path, "start\n")
          started.send nil
        end
        done.send nil
      end

      started.receive
      wait_for { File.exists?(events_path) && File.read(events_path) == "start\nelected" }
      controller.stop
      wait_for { File.read(events_path) == "start\nelectedlost" }
      select
      when done.receive
      when timeout(2.seconds)
        fail "VR controller did not stop"
      end
    end
  end
end

describe LavinMQ::Clustering::Server do
  describe "VR quorum commit" do
    it "routes v2 replication connections to the stream path" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      members = LavinMQ::Clustering::StaticMembers.parse("0=tcp://localhost:1,1=tcp://localhost:2")
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, members, state)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "vr v2 route spec")

      follower = sync_vr_follower(server, tcp_server.local_address.port, 1)
      wait_for { server.followers.size == 1 }
      server.followers.first.v2?.should be_true
    ensure
      follower.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "routes v2 control RPCs to the vote handler" do
      with_datadir do |data_dir|
        members = LavinMQ::Clustering::StaticMembers.parse("1=tcp://localhost:1,2=tcp://localhost:2")
        config = vr_config(data_dir, 2, "1=tcp://localhost:1,2=tcp://localhost:2")
        state = LavinMQ::Clustering::VRState.new(data_dir)
        state.apply_op!(4)
        tcp_server = TCPServer.new("localhost", 0)
        server = start_vr_server(config, members, state, 2, tcp_server)

        response = vr_control_vote(server, tcp_server.local_address.port, 1, 0_i64, 4_i64)
        response[:granted].should be_true
        response[:view].should eq 0
        response[:op_number].should eq 4
        state.voted_for.should eq 1
      ensure
        server.try &.close
        tcp_server.try &.close
      end
    end

    it "excludes v1 replication peers from VR quorum" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      members = LavinMQ::Clustering::StaticMembers.parse("0=tcp://localhost:1,1=tcp://localhost:2,2=tcp://localhost:3")
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, members, state)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "vr v1 quorum exclusion spec")

      follower_io = sync_vr_follower(server, tcp_server.local_address.port, 1, v2: false)
      wait_for { server.followers.size == 1 }
      server.followers.first.v2?.should be_false

      server.append_bytes(File.join(data_dir, "vr-v1-excluded"), "x".to_slice, 0i64)
      follower = server.followers.first
      follower_io.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian

      committed = Channel(Nil).new(1)
      spawn(name: "vr v1 excluded wait spec") do
        server.wait_for_followers
        committed.send nil
      end

      select
      when committed.receive
        fail "v1 follower satisfied VR quorum"
      when timeout(300.milliseconds)
      end
      state.commit_number.should eq 0
    ensure
      follower_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "commits once a quorum including the primary has acked" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      members = LavinMQ::Clustering::StaticMembers.parse("0=tcp://localhost:1,1=tcp://localhost:2,2=tcp://localhost:3")
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, members, state)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "vr quorum server spec")

      follower_a = sync_vr_follower(server, tcp_server.local_address.port, 1)
      follower_b = sync_vr_follower(server, tcp_server.local_address.port, 2)
      wait_for { server.followers.size == 2 }

      server.append_bytes(File.join(data_dir, "vr-quorum"), "x".to_slice, 0i64)
      state.op_number.should eq 1

      committed = Channel(Nil).new(1)
      spawn(name: "vr quorum wait spec") do
        server.wait_for_followers
        committed.send nil
      end

      select
      when committed.receive
        fail "committed before any follower acked"
      when timeout(300.milliseconds)
      end

      follower = server.followers.find!(&.id.== 1)
      follower_a.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian

      select
      when committed.receive
      when timeout(5.seconds)
        fail "VR quorum commit did not complete after one follower ack"
      end
      state.commit_number.should eq 1
    ensure
      follower_a.try &.close
      follower_b.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "does not wait behind a slow follower before counting a healthy quorum follower" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      members = LavinMQ::Clustering::StaticMembers.parse("0=tcp://localhost:1,1=tcp://localhost:2,2=tcp://localhost:3")
      state = LavinMQ::Clustering::VRState.new(data_dir)
      state.role = "primary"
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, members, state)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "vr quorum nonsequential spec")

      slow_follower = sync_vr_follower(server, tcp_server.local_address.port, 1)
      fast_follower = sync_vr_follower(server, tcp_server.local_address.port, 2)
      wait_for { server.followers.size == 2 }

      server.append_bytes(File.join(data_dir, "vr-quorum-nonsequential"), "x".to_slice, 0i64)
      wait_for { server.followers.all? { |f| f.lag_in_bytes > 0 } }

      committed = Channel(Time::Span).new(1)
      started_at = Time.instant
      spawn(name: "vr quorum nonsequential wait spec") do
        server.wait_for_followers
        committed.send(Time.instant - started_at)
      end

      sleep 100.milliseconds
      follower = server.followers.find!(&.id.== 2)
      fast_follower.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian

      select
      when elapsed = committed.receive
        elapsed.should be < 1.second
      when timeout(1.second)
        fail "VR quorum waited behind the slow follower"
      end
      state.commit_number.should eq 1
    ensure
      slow_follower.try &.close
      fast_follower.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "exposes VR status fields" do
      with_datadir do |data_dir|
        members = LavinMQ::Clustering::StaticMembers.parse("1=tcp://a:5679,2=tcp://b:5679,3=tcp://c:5679")
        state = LavinMQ::Clustering::VRState.new(data_dir)
        state.role = "primary"
        state.next_op!
        state.commit!(1)
        server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 1, members, state)

        json = JSON.parse(server.status.to_json).as_h
        json["backend"].as_s.should eq "vr"
        json["node_id"].as_i.should eq 1
        json["role"].as_s.should eq "primary"
        json["view"].as_i.should eq 0
        json["primary_id"].as_i.should eq 1
        json["primary_uri"].as_s.should eq "tcp://a:5679"
        json["op_number"].as_i.should eq 1
        json["commit_number"].as_i.should eq 1
        json["quorum_size"].as_i.should eq 2
      end
    end
  end
end

describe "VR election safety" do
  it "prevents a stale deterministic candidate from winning without an up-to-date quorum" do
    with_datadir do |candidate_dir|
      with_datadir do |peer_dir|
        peer2_tcp = TCPServer.new("localhost", 0)
        peer3_port = unused_local_port
        members_raw = "1=tcp://localhost:#{unused_local_port},2=tcp://localhost:#{peer2_tcp.local_address.port},3=tcp://localhost:#{peer3_port}"
        members = LavinMQ::Clustering::StaticMembers.parse(members_raw)

        peer_config = vr_config(peer_dir, 2, members_raw)
        peer_state = LavinMQ::Clustering::VRState.new(peer_dir)
        peer_state.apply_op!(2)
        peer_server = start_vr_server(peer_config, members, peer_state, 2, peer2_tcp)

        candidate_config = vr_config(candidate_dir, 1, members_raw)
        candidate = LavinMQ::Clustering::VRController.new(candidate_config)
        candidate.state.apply_op!(1)

        candidate.win_election_for_spec.should be_false
      ensure
        peer_server.try &.close
        peer2_tcp.try &.close
      end
    end
  end

  it "allows the next up-to-date deterministic candidate to win" do
    with_datadir do |peer_dir|
      with_datadir do |candidate_dir|
        peer1_tcp = TCPServer.new("localhost", 0)
        peer3_port = unused_local_port
        members_raw = "1=tcp://localhost:#{peer1_tcp.local_address.port},2=tcp://localhost:#{unused_local_port},3=tcp://localhost:#{peer3_port}"
        members = LavinMQ::Clustering::StaticMembers.parse(members_raw)

        peer_config = vr_config(peer_dir, 1, members_raw)
        peer_state = LavinMQ::Clustering::VRState.new(peer_dir)
        peer_state.observe_view!(1)
        peer_state.apply_op!(1)
        peer_server = start_vr_server(peer_config, members, peer_state, 1, peer1_tcp)

        candidate_config = vr_config(candidate_dir, 2, members_raw)
        candidate = LavinMQ::Clustering::VRController.new(candidate_config)
        candidate.state.observe_view!(1)
        candidate.state.apply_op!(2)

        candidate.win_election_for_spec.should be_true
      ensure
        peer_server.try &.close
        peer1_tcp.try &.close
      end
    end
  end

  it "detects loss of authority after a peer votes in a higher view" do
    with_datadir do |primary_dir|
      with_datadir do |peer_dir|
        peer2_tcp = TCPServer.new("localhost", 0)
        peer3_port = unused_local_port
        members_raw = "1=tcp://localhost:#{unused_local_port},2=tcp://localhost:#{peer2_tcp.local_address.port},3=tcp://localhost:#{peer3_port}"
        members = LavinMQ::Clustering::StaticMembers.parse(members_raw)

        peer_config = vr_config(peer_dir, 2, members_raw)
        peer_state = LavinMQ::Clustering::VRState.new(peer_dir)
        peer_state.grant_vote?(2, 1, 0, 2).should be_true
        peer_server = start_vr_server(peer_config, members, peer_state, 2, peer2_tcp)

        primary_config = vr_config(primary_dir, 1, members_raw)
        primary = LavinMQ::Clustering::VRController.new(primary_config)

        authorized, highest_view = primary.authority_status_for_spec(0)
        authorized.should be_false
        highest_view.should eq 1
      ensure
        peer_server.try &.close
        peer2_tcp.try &.close
      end
    end
  end
end
