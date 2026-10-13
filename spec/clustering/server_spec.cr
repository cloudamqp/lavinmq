require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "lz4"

# A version 3 handshake up to the resume request; returns the socket and the
# lz4 reader
private def connect_v3(server, port, id : Int32) : {TCPSocket, Compress::LZ4::Reader}
  io = TCPSocket.new("localhost", port)
  clustering_handshake(io, server.password, LavinMQ::Clustering::StartV3, id)
  {io, Compress::LZ4::Reader.new(io)}
end

describe LavinMQ::Clustering::Server do
  describe "#files_with_hash" do
    describe "for MFile" do
      it "should hash only the real (data) size, not the sparse capacity" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
          0)
        file = MFile.new(File.join(data_dir, "mfile_hash_test"), 1024)
        file.print "foo"
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end
    end

    describe "for File" do
      it "should open and read file calculating hash", tags: "slow" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
          0)
        path = File.join(data_dir, "file_hash_test")
        file = File.open(path, "w")
        file.print "foo"
        file.close
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end
    end

    describe "with caps" do
      it "reuses the cached hash when it covers exactly the cap" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
          0)
        content = "hello world"
        path = File.join(data_dir, "capped_cache_test")
        File.write path, content
        server.register_file(path)
        server.files_with_hash { |_path_hash| }
        # Deleted from disk: the capped pass can only produce the hash from
        # the cache, proving it doesn't re-read the file.
        File.delete path

        caps = {"capped_cache_test" => content.bytesize.to_i64}
        hashes = [] of Bytes
        server.files_with_hash(caps) { |_path, hash| hashes << hash }
        hashes.should eq [Digest::SHA1.digest(content)]
      ensure
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end

      it "recomputes when the cap does not match the cached hash's size" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
          0)
        content = "hello world"
        path = File.join(data_dir, "capped_recompute_test")
        File.write path, content
        server.register_file(path)
        server.files_with_hash { |_path_hash| }

        caps = {"capped_recompute_test" => 5i64}
        hashes = [] of Bytes
        server.files_with_hash(caps) { |_path, hash| hashes << hash }
        hashes.should eq [Digest::SHA1.digest(content[0, 5])]
      ensure
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end

      # Regression: entries restored from an old-format checksums.sha1 have
      # no size. The uncapped pass must treat them as misses and recompute,
      # so the capped pass can reuse the healed entry instead of re-hashing
      # every upgraded file under the lock.
      it "heals restored sizeless entries in the uncapped pass" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        content = "hello world"
        path = File.join(data_dir, "sizeless_heal_test")
        File.write path, content
        checksums_path = File.join(data_dir, "checksums.sha1")
        stale_hash = Digest::SHA1.digest("stale")
        File.write checksums_path, "#{stale_hash.hexstring} *sizeless_heal_test\n"

        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
          0)
        server.register_file(path)
        tcp_server = TCPServer.new("localhost", 0)
        spawn { server.listen(tcp_server) }
        # restore (run by listen) truncates the file once it's loaded
        wait_for { File.size(checksums_path) == 0 }

        # The sizeless entry is a miss: recomputed, not served stale.
        uncapped = [] of Bytes
        server.files_with_hash { |_path, hash| uncapped << hash }
        uncapped.should eq [Digest::SHA1.digest(content)]

        # Deleted from disk: the capped pass can only reuse the healed entry.
        File.delete path
        caps = {"sizeless_heal_test" => content.bytesize.to_i64}
        capped = [] of Bytes
        server.files_with_hash(caps) { |_path, hash| capped << hash }
        capped.should eq [Digest::SHA1.digest(content)]
      ensure
        server.try &.close
        tcp_server.try &.close
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end
    end
  end

  describe "#followers" do
    it "returns only synced followers" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      fi = FakeFileIndex.new(data_dir)
      sock_a, client_a = FakeSocket.pair
      sock_b, client_b = FakeSocket.pair
      synced = LavinMQ::Clustering::Follower.new(sock_a, data_dir, fi)
      syncing = LavinMQ::Clustering::Follower.new(sock_b, data_dir, fi)
      synced.mark_synced!
      server.@followers << synced << syncing # syncing left in Syncing state

      server.followers.should eq [synced] # excludes the still-syncing follower
    ensure
      sock_a.try &.close
      client_a.try &.close
      sock_b.try &.close
      client_b.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "broken-follower cleanup" do
    it "drops a synced follower whose socket breaks while it is sent to" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "broken follower spec")

      io, _lz4 = sync_fake_follower(server, tcp_server.local_address.port, 4)
      wait_for { server.followers.any? &.id.== 4 }

      path = File.join(data_dir, "broken_follower_seg")
      File.write(path, "")
      server.register_file(path)

      io.close # break the follower's peer socket
      # Incompressible payloads large enough to force the sender's LZ4
      # flushes to the dead socket, raising IO::Error in the sender.
      payload = Random::Secure.random_bytes(256 * 1024)
      10.times { server.append_bytes(path, payload, 0i64) }

      wait_for { server.all_followers.empty? }
    ensure
      io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "#replace_file" do
    it "copies the file's content at call time so later writes do not bleed into the stream" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      path = File.join(data_dir, "replaced")
      File.write path, "foo"
      server.register_file(path)

      lsn = server.journal.tap(&.subscribe(FakeJournalSubscriber.new)).last_lsn + 1
      server.replace_file(path)
      File.write path, "appended-after-replace", mode: "a"

      entry = server.journal.read(lsn).not_nil!
      entry.op.replace?.should be_true
      entry.path.should eq "replaced"
      String.new(entry.bytes).should eq "foo"
    ensure
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "#append" do
    it "raises ArgumentError when no MFile is registered for the path" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      path = File.join(data_dir, "path_only_file")
      File.write(path, "data")
      server.register_file(path) # path-only, not MFile

      expect_raises(ArgumentError, /requires an MFile/) do
        server.append(path, 0, 4)
      end
    ensure
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "join" do
    # Regression: the second full-sync pass of a joining follower used to run
    # under the dispatch lock, pausing every replicated write (publishes,
    # acks, declares) for as long as the pass took. With the journal the pass
    # is capped at a log position instead, and writers go on.
    it "does not block writers while a follower is in its second sync pass" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "join stall spec")
      path = File.join(data_dir, "join_stall_file")
      File.write(path, "")
      server.register_file(path)

      io = TCPSocket.new("localhost", tcp_server.local_address.port)
      io.write LavinMQ::Clustering::Start
      io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
      io.write server.password.to_slice
      io.read_byte # password-accepted byte
      io.write_bytes 6, IO::ByteFormat::LittleEndian
      io.flush
      lz4 = Compress::LZ4::Reader.new(io)
      sha1_size = Digest::SHA1.new.digest_size
      read_file_list = -> do
        loop do
          len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if len.zero?
          lz4.skip len
          lz4.skip sha1_size
        end
      end
      read_file_list.call
      io.write_bytes 0i32 # first pass: request no files
      io.flush
      read_file_list.call # second pass: leave the leader waiting for our file requests

      written = Channel(Nil).new(1)
      spawn(name: "writer during join") do
        server.append_bytes(path, "x".to_slice, 0i64)
        written.send nil
      end
      select
      when written.receive
      when timeout(2.seconds)
        fail "a joining follower's second sync pass blocked a writer"
      end
    ensure
      io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "join baseline (full_sync cut)" do
    it "streams only the unsynced tail of a record that straddles the cut" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "repli server spec")

      path = File.join(data_dir, "baseline_seg")
      mfile = MFile.new(path, 1024)
      mfile.print "head" # a complete 4-byte record
      server.register_file(mfile)
      # A writer is mid-record: it has written the first half of the record at
      # [4, 12) but not the rest, and the dispatch only happens once the whole
      # record is written. The join takes its cut from the live size (8), so
      # the cut lands inside the record.
      mfile.print "part"

      io, lz4 = sync_fake_follower(server, tcp_server.local_address.port, 5)
      wait_for { server.followers.any? &.id.== 5 }

      # The writer finishes the record and dispatches it as a whole.
      mfile.print "more"
      server.append(path, 0, 4) # entirely within the baseline → skipped
      server.append(path, 4, 8) # straddles the cut → only the tail is sent

      received = Channel({String, Int64, String}).new(1)
      spawn do
        len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
        filename = lz4.read_string(len)
        size = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        body = lz4.read_string(size.abs.to_i32)
        received.send({filename, size, body})
      rescue IO::Error
      end

      select
      when msg = received.receive
        msg[0].should eq "baseline_seg"
        # Only the bytes full_sync didn't deliver: not the whole record (would
        # duplicate "part") and not nothing (would lose "more").
        msg[1].should eq(-4i64)
        msg[2].should eq "more"
      when timeout(3.seconds)
        fail "the straddling record's tail was never streamed to the joined follower"
      end
    ensure
      io.try &.close
      mfile.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "join baseline, protocol version 3" do
    it "streams every append with its offset, so the follower rewrites the head it already has" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "repli server spec v3")

      path = File.join(data_dir, "baseline_seg_v3")
      mfile = MFile.new(path, 1024)
      mfile.print "head"
      server.register_file(mfile)
      mfile.print "part" # mid-record at the cut, as in the v1 twin above

      io, lz4 = sync_fake_follower(server, tcp_server.local_address.port, 7, LavinMQ::Clustering::StartV3)
      wait_for { server.followers.any? &.id.== 7 }

      mfile.print "more"
      server.append(path, 0, 4) # within the baseline: still sent, the follower rewrites it in place
      server.append(path, 4, 8) # straddles the cut: sent whole, with its offset

      received = Channel({Int64, String}).new(2)
      spawn do
        2.times do
          header = LavinMQ::Clustering::WireV3.read_header(lz4)
          header.op.append?.should be_true
          header.path.should eq "baseline_seg_v3"
          offset = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          len = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          received.send({offset, lz4.read_string(len)})
        end
      rescue IO::Error
      end

      2.times do |i|
        select
        when msg = received.receive
          msg.should eq(i.zero? ? {0i64, "head"} : {4i64, "partmore"})
        when timeout(3.seconds)
          fail "append #{i} was never streamed to the joined follower"
        end
      end
    ensure
      io.try &.close
      mfile.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "resuming a version 3 follower" do
    it "streams from the entry after the one the follower applied, without a full sync" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "resume spec")
      path = File.join(data_dir, "resumed")
      File.write(path, "")
      server.register_file(path)
      # Entries the follower will claim to have, journaled while nobody listens
      server.append_bytes(path, "a".to_slice, 0i64)
      server.append_bytes(path, "b".to_slice, 1i64)
      applied = server.journal.last_lsn

      io, lz4 = connect_v3(server, tcp_server.local_address.port, 8)
      LavinMQ::Clustering::WireV3.write_resume_request(io, server.journal.id, applied)
      resume, journal_id = LavinMQ::Clustering::WireV3.read_resume_reply(io)
      resume.should be_true
      journal_id.should eq server.journal.id
      wait_for { server.followers.any? &.id.== 8 }
      server.followers.find!(&.id.== 8).applied_lsn.should eq applied

      server.append_bytes(path, "c".to_slice, 2i64)
      header = LavinMQ::Clustering::WireV3.read_header(lz4) # no file lists: the stream directly
      header.lsn.should eq applied + 1
      header.path.should eq "resumed"
      lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 2i64
      lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 1i64
      lz4.read_string(1).should eq "c"
    ensure
      io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "sends the bytes of an entry whose mmap lease is gone from the file" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "resume mfile spec")
      mfile = MFile.new(File.join(data_dir, "seg"), 64)
      mfile.print "head"
      server.register_file(mfile)
      server.append(mfile.path, 0, 4)
      mfile.print "tail"
      lsn = server.journal.last_lsn + 1
      server.append(mfile.path, 4, 4) # no subscriber: never leased
      mfile.close
      gone = MFile.new(File.join(data_dir, "gone"), 64)
      gone.print "x"
      server.register_file(gone)
      server.append(gone.path, 0, 1)
      gone.delete
      gone.close

      io, lz4 = connect_v3(server, tcp_server.local_address.port, 9)
      LavinMQ::Clustering::WireV3.write_resume_request(io, server.journal.id, lsn - 1)
      LavinMQ::Clustering::WireV3.read_resume_reply(io)[0].should be_true

      header = LavinMQ::Clustering::WireV3.read_header(lz4)
      header.lsn.should eq lsn
      lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64
      lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64
      lz4.read_string(4).should eq "tail"
      header = LavinMQ::Clustering::WireV3.read_header(lz4)
      header.lsn.should eq lsn + 1
      header.op.delete?.should be_true # the file is gone, a later entry deletes it anyway
      header.path.should eq "gone"
    ensure
      io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "asks for a full sync from another journal or an evicted entry, and sends the cut after it" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "resume full sync spec")
      server.append_bytes(File.join(data_dir, "f"), "a".to_slice, 0i64)

      io, lz4 = connect_v3(server, tcp_server.local_address.port, 10)
      LavinMQ::Clustering::WireV3.write_resume_request(io, server.journal.id + 1, 1u64)
      resume, journal_id = LavinMQ::Clustering::WireV3.read_resume_reply(io)
      resume.should be_false
      journal_id.should eq server.journal.id
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
      LavinMQ::Clustering::WireV3.read_cut(lz4).should eq server.journal.head
      wait_for { server.followers.any? &.id.== 10 }
    ensure
      io.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "#replace_file" do
    it "journals a delete for a file that is gone" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      path = File.join(data_dir, "vanished")
      server.register_file(path)
      lsn = server.journal.tap(&.subscribe(FakeJournalSubscriber.new)).last_lsn + 1
      server.replace_file(path)
      entry = server.journal.read(lsn).not_nil!
      entry.op.delete?.should be_true
      entry.path.should eq "vanished"
    ensure
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "thread safety" do
    it "concurrent files_with_hash and mutations don't crash", tags: "slow" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_test_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 200_000

      # Reader on a separate thread: iterate files_with_hash
      Fiber::ExecutionContext::Isolated.new("test-concurrent-hash") do
        iterations.times do
          server.files_with_hash { |_path, _hash| }
        end
        done.send nil
      end

      # Writer on main thread: mix of mutations
      iterations.times do |i|
        mfile = mfiles[i % mfiles.size]
        case i % 5
        when 0 then server.delete_file(mfile.path)
        when 1 then server.replace_file(mfile.path)
        when 2 then server.append_bytes(mfile.path, "data".to_slice, 0i64)
        else        server.register_file(mfile)
        end
      end

      select
      when done.receive
      when timeout(30.seconds)
        fail "Timed out waiting for concurrent operations"
      end

      server.nr_of_files.should be >= 0
    ensure
      mfiles.try &.each { |mf| mf.delete rescue nil }
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "concurrent with_file and mutations don't crash", tags: "slow" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_wf_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 200_000

      # Reader on a separate thread: call with_file repeatedly
      Fiber::ExecutionContext::Isolated.new("test-concurrent-wf") do
        iterations.times do |i|
          key = "concurrent_wf_#{i % mfiles.size}"
          server.with_file(key) { |_f, _size| }
        end
        done.send nil
      end

      # Writer on main thread: register and delete files
      iterations.times do |i|
        mfile = mfiles[i % mfiles.size]
        if i % 3 == 0
          server.delete_file(mfile.path)
        else
          server.register_file(mfile)
        end
      end

      select
      when done.receive
      when timeout(30.seconds)
        fail "Timed out waiting for concurrent operations"
      end
    ensure
      mfiles.try &.each { |mf| mf.delete rescue nil }
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end
end
