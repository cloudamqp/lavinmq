require "../spec_helper"
require "../../src/lavinmq/clustering_v2/raft_log"

describe LavinMQ::ClusteringV2::RaftLog do
  it "performs mutual HMAC-SHA256 challenge-response handshake and rejects wrong secrets" do
    server_socket, client_socket = UNIXSocket.pair
    secret = "super-secret-cluster-cookie"

    server_ch = Channel(Tuple(UInt64, String) | Exception).new(1)
    spawn do
      res = LavinMQ::ClusteringV2::Protocol.server_handshake!(server_socket, 101_u64, secret)
      server_ch.send(res)
    rescue ex
      server_ch.send(ex)
    end

    remote_server_id = LavinMQ::ClusteringV2::Protocol.client_handshake!(client_socket, 202_u64, secret, "tcp://127.0.0.1:5679")
    remote_server_id.should eq 101_u64

    server_res = server_ch.receive
    server_res.should be_a(Tuple(UInt64, String))
    client_id, peer_uri = server_res.as(Tuple(UInt64, String))
    client_id.should eq 202_u64
    peer_uri.should eq "tcp://127.0.0.1:5679"
    server_socket.close
    client_socket.close

    # Verify wrong secret is rejected
    s2, c2 = UNIXSocket.pair
    spawn do
      LavinMQ::ClusteringV2::Protocol.server_handshake!(s2, 101_u64, secret)
    rescue
    end
    expect_raises(LavinMQ::ClusteringV2::Protocol::AuthenticationError) do
      LavinMQ::ClusteringV2::Protocol.client_handshake!(c2, 202_u64, "wrong-secret", "tcp://127.0.0.1:5679")
    end
    s2.close
    c2.close
  end

  it "persists and recovers raft.meta and raft.idx across restarts" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_persist")
    members = [
      LavinMQ::ClusteringV2::MemberInfo.new(1_u64, "tcp://127.0.0.1:5681", "amqp://127.0.0.1:5672", "http://127.0.0.1:15672", LavinMQ::ClusteringV2::MemberRole::Voter),
      LavinMQ::ClusteringV2::MemberInfo.new(2_u64, "tcp://127.0.0.1:5682", "amqp://127.0.0.1:15673", "http://127.0.0.1:15673", LavinMQ::ClusteringV2::MemberRole::Learner),
    ]

    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 16_u64, node_id: 1_u64, cluster_id: 999_u64)
    log.set_term_and_vote(4_u64, 2_u64)
    log.set_members(members)
    e1 = log.append_command(4_u64, LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp)
    e2 = log.append_command(4_u64, LavinMQ::ClusteringV2::LogCommandType::QueueDeclare, "q1".to_slice, vhost_id: 1_u64, queue_id: 7_u64)
    log.advance_commit_index(e2.lsn).should be_true
    log.fsync
    log.close

    reopened = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 16_u64)
    reopened.node_id.should eq 1_u64
    reopened.cluster_id.should eq 999_u64
    reopened.current_term.should eq 4_u64
    reopened.voted_for.should eq 2_u64
    reopened.first_lsn.should eq 1_u64
    reopened.last_lsn.should eq 2_u64
    reopened.commit_index.should eq 2_u64
    reopened.members.size.should eq 2
    reopened.members[1].learner?.should be_true

    rec1 = reopened.read_entry(e1.lsn).not_nil!
    rec1.command.should eq LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp
    rec2 = reopened.read_entry(e2.lsn).not_nil!
    rec2.command.should eq LavinMQ::ClusteringV2::LogCommandType::QueueDeclare
    String.new(rec2.payload).should eq "q1"
    reopened.close
  end

  it "slices publish payloads zero-copy from registered MFile segments and elides deleted segments" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_zerocopy")
    FileUtils.mkdir_p(dir)
    seg_path = File.join(dir, "msgs.0000000001")
    seg_mfile = MFile.new(seg_path, 4096)
    seg_mfile.write_bytes(1_i32) # schema version header at offset 0..3

    msg1_bytes = "hello-zero-copy-segment-1".to_slice
    pos1 = seg_mfile.size.to_u32
    seg_mfile.write(msg1_bytes)

    msg2_bytes = "hello-zero-copy-segment-2".to_slice
    pos2 = seg_mfile.size.to_u32
    seg_mfile.write(msg2_bytes)

    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 32_u64, node_id: 1_u64)
    log.register_segment(1_u64, 10_u64, 1_u32, seg_mfile)

    pub1 = log.append_publish(1_u64, 1_u64, 10_u64, 1_u32, pos1, msg1_bytes.size.to_u32)
    log.append_publish(1_u64, 1_u64, 10_u64, 1_u32, pos2, msg2_bytes.size.to_u32)
    log.append_ack(1_u64, 1_u64, 10_u64, 1_u32, pos1)
    log.append_ack(1_u64, 1_u64, 10_u64, 1_u32, pos2)

    # Before segment deletion, read_entry_leased slices directly from seg_mfile under an MFile lease
    r1, leased_mfile = log.read_entry_leased(pub1.lsn).not_nil!
    begin
      leased_mfile.should eq seg_mfile
      r1.entry.has_ctrl_payload?.should be_false
      r1.payload.should eq msg1_bytes
      r1.payload.to_unsafe.should eq(seg_mfile.to_slice.to_unsafe + pos1)
    ensure
      leased_mfile.try(&.release_lease)
    end

    # Now delete segment 1 immediately (as MessageStore#delete does when all messages are acked)
    seg_del = log.append_segment_delete(1_u64, 1_u64, 10_u64, 1_u32)
    seg_mfile.close
    File.delete(seg_path)

    # Catching-up follower reading from LSN 1..5 receives Tombstone entries for the deleted segment's
    # Publish and Ack entries, followed by SegmentDelete, without needing the deleted segment file!
    entries = log.read_entries(1_u64).not_nil!
    entries.size.should eq 5
    entries[0].entry.tombstoned?.should be_true
    entries[0].command.should eq LavinMQ::ClusteringV2::LogCommandType::Tombstone
    entries[1].entry.tombstoned?.should be_true
    entries[2].entry.tombstoned?.should be_true
    entries[3].entry.tombstoned?.should be_true
    entries[4].lsn.should eq seg_del.lsn
    entries[4].command.should eq LavinMQ::ClusteringV2::LogCommandType::SegmentDelete

    log.close
  end

  it "handles ring buffer wrap-around and uncommitted tail truncation with rollback callbacks" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_ring")
    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 8_u64, node_id: 1_u64)

    truncated_lsns = Array(UInt64).new
    log.on_truncate do |entry|
      truncated_lsns << entry.lsn
    end

    # Append 5 entries in term 1, commit up to LSN 3
    5.times do |i|
      log.append_command(1_u64, LavinMQ::ClusteringV2::LogCommandType::QueueDeclare, "q#{i}".to_slice)
    end
    log.advance_commit_index(3_u64).should be_true

    # Attempting to truncate committed LSN 3 must raise
    expect_raises(ArgumentError) do
      log.truncate_from(3_u64)
    end

    # Leader with term 2 overwrites uncommitted LSN 4 and 5 via maybe_append
    new_rec = LavinMQ::ClusteringV2::LogEntryRecord.new(
      LavinMQ::ClusteringV2::LogIndexEntry.new(
        term: 2_u64,
        lsn: 4_u64,
        cmd_type: LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp.value
      )
    )
    ok, match_lsn, _, _ = log.maybe_append(3_u64, 1_u64, 4_u64, [new_rec])
    ok.should be_true
    match_lsn.should eq 4_u64
    log.last_lsn.should eq 4_u64
    log.commit_index.should eq 4_u64
    truncated_lsns.should eq [5_u64, 4_u64]

    # Now append 10 more entries, advancing commit_index and applied_index so the 8-slot ring buffer can wrap
    log.advance_applied_index(4_u64)
    10.times do
      e = log.append_command(2_u64, LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp)
      log.advance_commit_index(e.lsn)
      log.advance_applied_index(e.lsn)
    end
    log.last_lsn.should eq 14_u64
    log.first_lsn.should eq 7_u64
    log.read_entries(6_u64).should be_nil
    log.read_entries(7_u64).not_nil!.size.should eq 8

    log.close
  end

  it "does not inflate match_lsn or commit stale follower suffix entries in maybe_append" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_stale_suffix")
    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 32_u64, node_id: 2_u64)

    # Follower holds LSN 1..10 from term 1
    10.times do |i|
      log.append_command(1_u64, LavinMQ::ClusteringV2::LogCommandType::QueueDeclare, "q#{i + 1}".to_slice)
    end
    log.last_lsn.should eq 10_u64

    # Leader sends entries 6..7 (matching term 1) with leader_commit = 9
    entries = [6_u64, 7_u64].map do |lsn|
      LavinMQ::ClusteringV2::LogEntryRecord.new(
        LavinMQ::ClusteringV2::LogIndexEntry.new(
          term: 1_u64,
          lsn: lsn,
          cmd_type: LavinMQ::ClusteringV2::LogCommandType::QueueDeclare.value
        ),
        "q#{lsn}".to_slice
      )
    end

    ok, match_lsn, _, _ = log.maybe_append(5_u64, 1_u64, 9_u64, entries)
    ok.should be_true
    # match_lsn must be prev_log_lsn + entries.size (7), NOT @last_lsn (10)
    match_lsn.should eq 7_u64
    # commit_index must be min(leader_commit=9, last_new_lsn=7) = 7, NOT 9 or 10
    log.commit_index.should eq 7_u64
    log.close
  end

  it "discards local log suffix on install_snapshot_state when term at last_included_lsn mismatches" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_snap_term")
    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 32_u64, node_id: 2_u64)

    10.times do
      log.append_command(1_u64, LavinMQ::ClusteringV2::LogCommandType::LeaderNoOp)
    end
    log.last_lsn.should eq 10_u64

    # Snapshot at LSN 6 with term 1 (matches local term 1 at LSN 6) -> keeps suffix 7..10
    log.install_snapshot_state(6_u64, 1_u64, [] of LavinMQ::ClusteringV2::MemberInfo)
    log.last_lsn.should eq 10_u64
    log.first_lsn.should eq 7_u64

    # Snapshot at LSN 8 with term 2 (mismatches local term 1 at LSN 8) -> discards suffix 9..10
    log.install_snapshot_state(8_u64, 2_u64, [] of LavinMQ::ClusteringV2::MemberInfo)
    log.last_lsn.should eq 8_u64
    log.first_lsn.should eq 9_u64
    log.commit_index.should eq 8_u64
    log.applied_index.should eq 8_u64
    log.close
  end

  it "dynamically grows the index ring rather than overwriting uncommitted or unapplied entries" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_grow")
    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 4_u64, node_id: 1_u64)
    log.track_applied = true
    log.capacity.should eq 4_u64

    # Append 10 entries while applied_index remains 0
    10.times do |i|
      e = log.append_command(1_u64, LavinMQ::ClusteringV2::LogCommandType::QueueDeclare, "q#{i + 1}".to_slice)
      log.advance_commit_index(e.lsn)
    end

    log.capacity.should be >= 16_u64
    log.first_lsn.should eq 1_u64
    log.last_lsn.should eq 10_u64

    # Every single unapplied entry 1..10 is still readable
    (1_u64..10_u64).each do |lsn|
      rec = log.read_entry(lsn).not_nil!
      String.new(rec.payload).should eq "q#{lsn}"
    end

    log.advance_applied_index(10_u64).should be_true
    log.close

    reopened = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 4_u64)
    reopened.applied_index.should eq 10_u64
    reopened.close
  end

  it "stages active publish payloads into raft.ctrl on unregister_segment so closing the segment MFile is safe" do
    dir = File.join(LavinMQ::Config.instance.data_dir, "raft_log_unregister_seg")
    FileUtils.mkdir_p(dir)
    seg_path = File.join(dir, "msgs.0000000005")
    seg_mfile = MFile.new(seg_path, 4096)
    seg_mfile.write_bytes(1_i32)

    payload = "stream-segment-evicted-payload".to_slice
    pos = seg_mfile.size.to_u32
    seg_mfile.write(payload)

    log = LavinMQ::ClusteringV2::RaftLog.new(dir, capacity: 16_u64, node_id: 1_u64)
    log.register_segment(1_u64, 42_u64, 5_u32, seg_mfile)
    pub = log.append_publish(1_u64, 1_u64, 42_u64, 5_u32, pos, payload.size.to_u32)

    # Evict/unregister the segment without tombstoning the entry (e.g., queue close or stream retention)
    log.unregister_segment(1_u64, 42_u64, 5_u32)
    seg_mfile.close
    File.delete(seg_path)

    # Reading the entry still succeeds from staged raft.ctrl memory without touching the closed MFile
    rec = log.read_entry(pub.lsn).not_nil!
    rec.entry.position.should eq pos
    rec.entry.byte_size.should eq payload.size.to_u32
    rec.payload.should eq payload
    log.close
  end
end
