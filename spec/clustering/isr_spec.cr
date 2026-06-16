require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/client"
require "lz4"

# Records the ISR sets the Server writes, so specs can assert ISR membership
# changes without a real etcd. Can be made to fail (coordinator unreachable)
# to assert that publish confirms stall until the ISR write succeeds.
class SpyCoordinator
  include LavinMQ::Clustering::Coordinator
  @lock = Mutex.new
  @failing = false
  getter isr_updates = Array(Set(Int32)).new

  def update_isr(synced_node_ids : Enumerable(Int32)) : Bool
    @lock.synchronize do
      return false if @failing # coordinator unreachable: caller stalls + retries
      @isr_updates << synced_node_ids.to_set
      true
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
      # the next replicated durable operation or publish confirm (see the
      # specs below).
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "join failures" do
    # Regression: if the join's ISR commit raised after mark_synced!, the
    # follower used to stay in @followers as Synced with no ack_loop running —
    # a zombie that every wait_for_confirm (publish confirms, definition
    # fences) blocked on forever. It must be removed like any other
    # disconnect.
    it "drops a follower whose join failed at the ISR commit, instead of leaving a zombie" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr join failure spec")

      coordinator.failing = true # the join's ISR commit will raise after mark_synced!
      follower_id = 3
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)

      wait_for { server.all_followers.empty? }

      coordinator.failing = false
      # The next durable operation must commit an ISR without the follower
      # and must not hang waiting for an ack that can never come.
      server.append_bytes(File.join(data_dir, "file"), "x".to_slice, 0i64)
      done = Channel(Nil).new(1)
      spawn(name: "wait_for_followers join failure spec") do
        server.wait_for_followers
        done.send nil
      end
      select
      when done.receive
      when timeout(5.seconds)
        fail "wait_for_followers hung after the failed join"
      end
      coordinator.last_isr.try(&.includes?(follower_id)).should be_false
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "durable operations against the ISR" do
    it "commits a dirty ISR before a replicated durable operation returns" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr durable op spec")

      follower_id = 9
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      client_io.close # caught-up disconnect: stays in the ISR, marks it dirty
      wait_for { server.followers.empty? }
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true

      # Durable operations that don't go through the Persister (queue/exchange
      # declares appending to definitions.amqp, users.json replaces, segment
      # deletes) must commit the shrunken ISR before they return — and thus
      # before they are acknowledged to any client — otherwise a leader crash
      # could elect the disconnected follower without the acknowledged change.
      server.append_bytes(File.join(data_dir, "definitions.amqp"), "frame".to_slice, 0i64)
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_false
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "definition changes against follower acks" do
    it "holds a durable queue declare until in-sync followers ack it" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr declare fence spec")

      follower_id = 5
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          declared = Channel(Nil).new(1)
          spawn(name: "declare queue spec") do
            ch.queue("definition_fence", durable: true)
            declared.send nil
          rescue
            declared.close
          end

          # The declare's definition is replicated but the raw follower never
          # acks; the Declare-Ok must be held back, like a publish confirm.
          wait_for { server.followers.find(&.id.== follower_id).try { |f| f.lag_in_bytes > 0 } }
          select
          when declared.receive
            fail "Declare-Ok delivered before the follower acked the definition"
          when timeout(300.milliseconds)
          end

          # Ack everything outstanding; the declare must now complete.
          follower = server.followers.find!(&.id.== follower_id)
          client_io.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian
          select
          when declared.receive
          when timeout(5.seconds)
            fail "Declare-Ok never delivered after the follower acked"
          end
          coordinator.last_isr.not_nil!.includes?(follower_id).should be_true
        end
      end
    ensure
      client_io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "completes a declare by evicting a follower that never acks, after committing its ISR removal" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr declare evict spec")

      follower_id = 5
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          declared = Channel(Nil).new(1)
          spawn(name: "declare queue spec") do
            ch.queue("definition_fence_evict", durable: true)
            declared.send nil
          rescue
            declared.close
          end

          # The follower never acks: after the ack deadline it is dropped and
          # its ISR removal committed, and only then may the declare complete
          # — the remaining failover candidates all have the definition.
          select
          when declared.receive
          when timeout(10.seconds)
            fail "Declare-Ok never delivered after the silent follower was evicted"
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

    it "flushes a dirty ISR before delivering a publish confirm when sync is disabled" do
      LavinMQ::Config.instance.sync = false
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr no-sync confirm flush spec")

      follower_id = 9
      client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
      wait_for { server.followers.any? &.id.== follower_id }

      client_io.close # caught-up disconnect: stays in the ISR, marks it dirty
      wait_for { server.followers.empty? }
      coordinator.last_isr.not_nil!.includes?(follower_id).should be_true

      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          q = ch.queue("isr_no_sync_confirm_flush", durable: true)
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

    it "waits for in-sync follower acks before confirming when sync is disabled" do
      LavinMQ::Config.instance.sync = false
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      coordinator = SpyCoordinator.new
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, coordinator, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "isr no-sync confirm ack spec")

      follower_id = 8
      client_io = nil.as(TCPSocket?)
      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          ch.confirm_select
          q = ch.queue("isr_no_sync_confirm_ack", durable: true)
          client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
          wait_for { server.followers.any? &.id.== follower_id }

          q.publish "m", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
          wait_for { server.followers.find(&.id.== follower_id).try { |f| f.lag_in_bytes > 0 } }

          confirmed = Channel(Bool).new(1)
          spawn(name: "no-sync wait_for_confirms spec") do
            confirmed.send(ch.wait_for_confirms)
          rescue
            confirmed.close
          end

          select
          when confirmed.receive
            fail "confirm delivered before the in-sync follower acked"
          when timeout(300.milliseconds)
          end

          follower = server.followers.find!(&.id.== follower_id)
          client_io.not_nil!.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian
          select
          when ok = confirmed.receive
            ok.should be_true
          when timeout(5.seconds)
            fail "confirm never delivered after the in-sync follower acked"
          end
        end
      end
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
      client_io = nil.as(TCPSocket?)
      with_amqp_server(replicator: server) do |s|
        with_channel(s) do |ch|
          ch.confirm_select
          # Declare before the follower joins: a durable declare also waits
          # for follower acks, and this raw follower never acks anything.
          q = ch.queue("isr_confirm_stall", durable: true)
          client_io = sync_follower(server, tcp_server.local_address.port, follower_id)
          wait_for { server.followers.any? &.id.== follower_id }

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
