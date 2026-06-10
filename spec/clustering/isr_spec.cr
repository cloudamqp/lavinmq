require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/client"
require "lz4"

# Records the ISR sets the Server writes, so specs can assert ISR membership
# changes without a real etcd. Can be made to fail (coordinator unreachable)
# to assert that publish confirms stall until the ISR write succeeds.
class SpyCoordinator < LavinMQ::Clustering::Coordinator
  @lock = Mutex.new
  @failing = false
  getter isr_updates = Array(Set(Int32)).new

  def update_isr(synced_node_ids : Set(Int32)) : Nil
    @lock.synchronize do
      raise Error.new("coordinator unavailable (spec)") if @failing
      @isr_updates << synced_node_ids.dup
    end
  end

  def failing=(value : Bool)
    @lock.synchronize { @failing = value }
  end

  def password : String
    "isr-spec-secret"
  end

  def last_isr : Set(Int32)?
    @lock.synchronize { @isr_updates.last? }
  end

  class Error < Exception; end
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
      # confirmed so far and is a valid candidate). It is only removed before
      # the next publish confirm (see "flushes a dirty ISR" below).
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "publish confirms against the ISR" do
    it "flushes a dirty ISR before delivering a publish confirm" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr confirm flush spec")

      follower_id = 9
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      client_io.close # caught-up disconnect: stays in the ISR, marks it dirty
      wait_for { server.followers.empty? }
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true

      # The confirm itself must commit the shrunken ISR first: everything
      # confirmed from here on is missing on the disconnected follower, so it
      # may not be listed as a failover candidate anymore.
      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          q = ch.queue("isr_confirm_flush", durable: true)
          q.publish_confirm("m", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)).should be_true
        end
      end
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_false
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "stalls publish confirms until a dead follower's ISR removal is committed" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr confirm stall spec")

      follower_id = 7
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          ch.confirm_select
          q = ch.queue("isr_confirm_stall", durable: true)
          coordinator.failing = true # coordinator becomes unreachable
          q.publish "m", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
          # The raw follower never acks; once bytes are outstanding, drop it.
          wait_for { server.all_followers.first?.try { |f| f.lag_in_bytes > 0 } }
          client_io.close # dies while behind, with a pending confirm

          confirmed = Channel(Bool).new(1)
          spawn(name: "wait_for_confirms spec") do
            confirmed.send(ch.wait_for_confirms)
          rescue
            confirmed.close
          end

          # While the ISR can't be written the follower might still be listed
          # as a failover candidate, so the confirm must not be delivered.
          select
          when confirmed.receive
            fail "confirm delivered before the follower's ISR removal was committed"
          when timeout(1.second)
          end

          coordinator.failing = false # coordinator is back
          select
          when ok = confirmed.receive
            ok.should be_true
          when timeout(5.seconds)
            fail "confirm never delivered after the ISR write succeeded"
          end
          coordinator.last_isr.not_nil!.includes?(follower_id).should be_false
        end
      end
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end
end
