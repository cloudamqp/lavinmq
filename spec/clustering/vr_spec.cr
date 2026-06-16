require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/static_members"
require "../../src/lavinmq/clustering/password_store"
require "../../src/lavinmq/clustering/vr_state"
require "lz4"

private def sync_vr_follower(server, port, id : Int32) : TCPSocket
  io = TCPSocket.new("localhost", port)
  io.write LavinMQ::Clustering::Start
  io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
  io.write server.password.to_slice
  io.read_byte.should eq 0
  io.write_bytes id, IO::ByteFormat::LittleEndian
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
  io
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

describe LavinMQ::Clustering do
  it "marks local clustering metadata as non-replicated" do
    LavinMQ::Clustering.metadata_file?(".lock").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_id").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_password").should be_true
    LavinMQ::Clustering.metadata_file?(".clustering_vr_state").should be_true
    LavinMQ::Clustering.metadata_file?("definitions.amqp").should be_false
  end
end

describe LavinMQ::Clustering::Server do
  describe "VR quorum commit" do
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
