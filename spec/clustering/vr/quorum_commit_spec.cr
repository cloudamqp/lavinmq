require "../../spec_helper"
require "../../../src/lavinmq/clustering/server"
require "lz4"

# Drive the majority-quorum durability gate (Server#wait_for_quorum) directly,
# with real Follower objects over in-process socket pairs whose acks advance the
# commit point. No etcd: a roster quorum is passed to the Server explicitly.
module QuorumCommitSpec
  # An ack is a byte delta (Int64) + the absolute applied op (UInt64).
  private def self.write_ack(socket, op : UInt64)
    socket.write_bytes 1i64, IO::ByteFormat::LittleEndian
    socket.write_bytes op, IO::ByteFormat::LittleEndian
  end

  # Add a synced follower wired to the server's commit-notify, draining its side
  # so the follower's periodic flushes never block. Returns the client end.
  private def self.add_synced_follower(server, data_dir) : UNIXSocket
    sock, client = FakeSocket.pair
    follower = LavinMQ::Clustering::Follower.new(sock, data_dir, server, server.@commit_notify)
    follower.mark_synced!(0u64)
    server.@followers << follower
    spawn(name: "quorum-spec ack_loop") { follower.ack_loop }
    spawn(name: "quorum-spec drain") do
      buf = uninitialized UInt8[4096]
      # Stop at EOF: a UNIXSocket read returns 0 (no exception) once the peer
      # closes, and looping on that would busy-spin and starve the scheduler.
      until client.read(buf.to_slice).zero?
      end
    rescue IO::Error
    end
    client
  end

  describe LavinMQ::Clustering::Server do
    describe "#wait_for_quorum" do
      it "returns once a majority (leader + one of two followers) has acked" do
        with_datadir do |data_dir|
          server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, quorum: 2)
          # Advance the leader's log head to op 3 (no followers yet, so nothing is
          # sent; each dispatch just bumps @op). Followers then join behind it.
          3.times { server.append_bytes("#{data_dir}/seed", "x".to_slice, 0i64) }
          server.current_op.should eq 3
          client_a = add_synced_follower(server, data_dir)
          add_synced_follower(server, data_dir)

          # Only the leader is at op 3; the two followers are at 0. 1 < quorum(2),
          # so the gate must block.
          confirmed = Channel(Nil).new
          spawn { server.wait_for_quorum(3u64); confirmed.send(nil) }
          select
          when confirmed.receive
            fail "wait_for_quorum returned before a majority acked"
          when timeout(150.milliseconds)
          end

          # One follower acks up to op 3: now leader + that follower = 2 = quorum.
          write_ack(client_a, 3u64)
          select
          when confirmed.receive
          when timeout(2.seconds)
            fail "wait_for_quorum did not return after a majority acked"
          end

          server.committed_op.should eq 3
        ensure
          server.try &.close
          FileUtils.rm_rf data_dir
        end
      end

      it "returns immediately for an already-committed target" do
        with_datadir do |data_dir|
          server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, quorum: 2)
          5.times { server.append_bytes("#{data_dir}/seed", "x".to_slice, 0i64) }
          client = add_synced_follower(server, data_dir)
          write_ack(client, 5u64)
          wait_for { server.committed_op >= 5u64 }

          confirmed = Channel(Nil).new
          spawn { server.wait_for_quorum(4u64); confirmed.send(nil) }
          select
          when confirmed.receive
          when timeout(2.seconds)
            fail "wait_for_quorum blocked on an already-committed target"
          end
        ensure
          server.try &.close
          FileUtils.rm_rf data_dir
        end
      end

      it "stalls a minority that cannot form a quorum" do
        with_datadir do |data_dir|
          server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0, quorum: 2)
          2.times { server.append_bytes("#{data_dir}/seed", "x".to_slice, 0i64) }
          # No followers: the lone leader is a minority of a 3-node roster.
          server.committed_op.should eq 0 # 1 member < quorum(2): nothing commits

          confirmed = Channel(Nil).new
          spawn { server.wait_for_quorum(2u64); confirmed.send(nil) }
          select
          when confirmed.receive
            fail "wait_for_quorum returned on a minority"
          when timeout(200.milliseconds)
          end
        ensure
          server.try &.close
          FileUtils.rm_rf data_dir
        end
      end
    end
  end
end
