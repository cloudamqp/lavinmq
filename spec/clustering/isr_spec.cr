require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/client"
require "lz4"

# Records the ISR sets the Server writes, so specs can assert ISR membership
# changes without a real etcd.
class SpyCoordinator < LavinMQ::Clustering::Coordinator
  @lock = Mutex.new
  getter isr_updates = Array(Set(Int32)).new

  def update_isr(synced_node_ids : Set(Int32)) : Nil
    @lock.synchronize { @isr_updates << synced_node_ids.dup }
  end

  def password : String
    "isr-spec-secret"
  end

  def last_isr : Set(Int32)?
    @lock.synchronize { @isr_updates.last? }
  end
end

# Drives the clustering handshake + two full-sync passes (requesting no files)
# so the follower reaches the Synced state on the leader.
private def sync_follower(server, port, id : Int32) : TCPSocket
  io = TCPSocket.new("localhost", port)
  io.write LavinMQ::Clustering::Start
  io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
  io.write server.password.to_slice
  io.read_byte # password-accepted byte
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
    io.write_bytes 0i32 # request no files
    io.flush
  end
  io
end

describe LavinMQ::Clustering::Server do
  describe "ISR maintenance on follower disconnect" do
    it "removes a follower that was behind from the ISR immediately, before the next write" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr behind spec")

      follower_id = 7
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }
      coordinator.last_isr.try(&.includes?(follower_id)).should be_true # joined the ISR

      # Replicate data the raw follower never acks → it falls behind.
      server.append_bytes(File.join(data_dir, "lag"), "x".to_slice, 0i64)
      wait_for { server.followers.find(&.id.== follower_id).try { |f| f.lag_in_bytes > 0 } }

      client_io.close # drop while behind; no further replication write follows

      wait_for { coordinator.last_isr.try { |s| !s.includes?(follower_id) } }
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_false
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "keeps a caught-up follower in the ISR after it disconnects (valid failover candidate)" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr caught-up spec")

      follower_id = 9
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }
      server.followers.find!(&.id.== follower_id).lag_in_bytes.should eq 0

      client_io.close                      # drop while fully caught up; no writes in flight
      wait_for { server.followers.empty? } # leader noticed the disconnect

      # No eager flush: the ISR still lists the follower (it has everything
      # confirmed so far and is a valid candidate). It would only be removed on
      # the next replication write.
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end
end
