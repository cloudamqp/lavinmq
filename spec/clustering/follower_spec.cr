require "../spec_helper"
require "lz4"

private def read_filename(io) : String
  size = io.read_bytes Int32, IO::ByteFormat::LittleEndian
  io.read_string(size)
end

private def read_data_size(io) : Int64
  io.read_bytes Int64, IO::ByteFormat::LittleEndian
end

# A journal with the follower tailing it from the head, so entries appended to
# it come out on the follower's socket (the sender flushes whenever it has
# drained the journal).
private def start_streaming(follower) : LavinMQ::Clustering::Journal
  journal = LavinMQ::Clustering::Journal.new(1024)
  journal.subscribe(follower)
  follower.start_sender(journal)
  journal
end

# Drain the client side so the sender never blocks on a full socket. Stops at
# EOF: a read at the peer's close returns 0 instead of raising, and looping on
# it would spin the fiber.
private def drain(client_socket) : Nil
  spawn do
    buf = uninitialized UInt8[4096]
    until client_socket.read(buf.to_slice).zero?
    end
  rescue IO::Error
    # socket closed at spec end
  end
end

private def record_size(path : String, payload : Int) : Int64
  (sizeof(Int32) + path.bytesize + sizeof(Int64) + payload).to_i64
end

private def negotiate(follower, client_socket, header) : Nil
  password = "foo"
  if LavinMQ::Clustering.header_version(header) == 3
    spawn(name: "handshake") { clustering_handshake(client_socket, password, header, 1) }
    follower.negotiate!(password)
  else
    client_socket.write header
    client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
    client_socket.write password.to_slice
    client_socket.write_bytes 1, IO::ByteFormat::LittleEndian # id
    follower.negotiate!(password)
    client_socket.read_byte.should eq 0u8
  end
end

private alias WireV3 = LavinMQ::Clustering::WireV3

private def v3_follower(data_dir, follower_socket, client_socket)
  follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
  negotiate(follower, client_socket, LavinMQ::Clustering::StartV3)
  follower
end

private def ack(client_socket, lsn : UInt64, bytes : Int64)
  WireV3.write_ack(client_socket, lsn, bytes)
end

module FollowerSpec
  # FakeFileIndex and FakeSocket live in spec/support/fake_follower.cr so the
  # clustering server spec can reuse them.

  describe LavinMQ::Clustering::Follower do
    describe "#negotiate!" do
      it "should raise InvalidStartHeaderError on invalid start header" do
        with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

          invalid_start = Bytes[0, 1, 2, 3, 4, 5, 6, 7]
          client_socket.write invalid_start

          expect_raises(LavinMQ::Clustering::InvalidStartHeaderError) do
            follower.negotiate!("foo")
          end
        ensure
          follower_socket.try &.close
          client_socket.try &.close
        end
      end

      it "should raise AuthenticationError and send 1 on wrong password" do
        with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

          password = "foo"
          client_socket.write LavinMQ::Clustering::Start
          client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          client_socket.write password.to_slice

          expect_raises(LavinMQ::Clustering::AuthenticationError) do
            follower.negotiate!("bar")
          end

          response = client_socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
          response.should eq 1u8
        ensure
          follower_socket.try &.close
          client_socket.try &.close
        end
      end

      it "should send 0 on succesful negotiation" do
        with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

          password = "foo"
          client_socket.write LavinMQ::Clustering::Start
          client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          client_socket.write password.to_slice
          client_socket.write_bytes 1, IO::ByteFormat::LittleEndian # id

          follower.negotiate!("foo")

          response = client_socket.read_byte
          response.should eq 0u8
        ensure
          follower_socket.try &.close
          client_socket.try &.close
        end
      end
    end
  end

  describe "#full_sync" do
    it "should send file list" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.full_sync }

        file_list = Hash(String, Bytes).new
        done = Channel(Nil).new
        spawn do
          loop do
            len = client_lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
            break if len == 0
            hash = Bytes.new(20)
            path = client_lz4.read_string len
            client_lz4.read_fully hash
            file_list[path] = hash
          end
          client_socket.write_bytes 0, IO::ByteFormat::LittleEndian # don't request any files
          Fiber.yield
          done.send nil
        end

        select
        when done.receive
        when timeout(1.second)
          fail "timeout reading file list"
        end

        file_list.should eq file_index.@files_with_hash
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # A syncing follower isn't in the ISR, so the aggressive streaming-phase
    # write timeout must not apply during the bulk transfer — otherwise a
    # follower that's merely slow at hashing or persisting files gets dropped
    # mid-sync. full_sync relaxes it; ack_loop tightens it again.
    it "relaxes the write timeout during full_sync and restores it for ack_loop" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        follower_socket.write_timeout.should eq 3.seconds # initialize default

        done = Channel(Nil).new
        spawn do
          follower.full_sync
          done.send nil
        end

        # Drain the file list and request no files so full_sync completes.
        loop do
          len = client_lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if len == 0
          hash = Bytes.new(20)
          client_lz4.read_string len
          client_lz4.read_fully hash
        end
        client_socket.write_bytes 0, IO::ByteFormat::LittleEndian # request no files

        select
        when done.receive
        when timeout(2.seconds)
          fail "full_sync did not complete"
        end
        follower_socket.write_timeout.should eq LavinMQ::Clustering::Follower::SYNC_WRITE_TIMEOUT

        spawn { follower.ack_loop }
        # ack_loop tightens the write timeout to ACK_TIMEOUT when it starts.
        10.times do
          break if follower_socket.write_timeout == LavinMQ::Clustering::Follower::ACK_TIMEOUT
          sleep 20.milliseconds
        end
        follower_socket.write_timeout.should eq LavinMQ::Clustering::Follower::ACK_TIMEOUT
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#stream changes" do
    it "delivers and acks outstanding data, then shuts down cleanly" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        10.times { journal.append_bytes("file", "hello world".to_slice, 0i64) }
        target = record_size("file", "hello world".bytesize) * 10
        wait_for { follower.lag_in_bytes == target }

        # Ack the outstanding bytes and wait until the follower has registered
        # them, so the lag-0 assertion is deterministic.
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(journal.last_lsn) }
        select
        when confirmed.receive
        when timeout(2.seconds)
          fail "follower never acked outstanding data"
        end
        follower.lag_in_bytes.should eq 0

        # A clean shutdown closes the socket and marks the follower dead.
        follower.close
        follower.dead?.should be_true
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "disconnects a follower that falls behind the journal" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)
        spawn { follower.ack_loop }

        journal = LavinMQ::Clustering::Journal.new(4)
        journal.subscribe(follower)
        8.times { journal.delete("f") } # evicts the first four before the sender starts
        follower.start_sender(journal)

        wait_for { follower.dead? }
        follower_socket.closed?.should be_true
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "replace entries" do
    it "write filename, file size, and file contents to the LZ4 stream" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.replace("file1", "foo".to_slice)

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        data_size = read_data_size(client_lz4)
        data_size.should eq 3i64
        buf = Bytes.new(data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        follower.sent_bytes.should eq record_size("file1", 3)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "append entries" do
    it "write filename and Bytes payload with a negative size header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.append_bytes("bar", "foo".to_slice, 0i64)

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "bar"
        data_size = read_data_size(client_lz4)
        data_size.should eq(-3i64)
        buf = Bytes.new(-data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        follower.sent_bytes.should eq record_size("bar", 3)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "write a value little-endian with a -4 size header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.append_value("file1", 123u32, 0i64)

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_bytes(UInt32, IO::ByteFormat::LittleEndian).should eq 123u32

        follower.sent_bytes.should eq record_size("file1", sizeof(UInt32))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "read the payload of an mfile entry from its mmap" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        mfile = MFile.new(File.join(data_dir, "seg"), 64)
        mfile.print "head"
        mfile.print "tail"

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.append_mfile("seg", mfile, 4i64, 4i64)
        mfile.close # rolled over before it was sent: the lease keeps the mmap

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "seg"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_string(4).should eq "tail"
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "skip what the follower's join cut already covers" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        follower.capture_synced_baseline({"seg" => 6i64})

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.append_bytes("seg", "head".to_slice, 0i64)  # entirely in the snapshot
        journal.append_bytes("seg", "part".to_slice, 4i64)  # straddles the cut
        journal.append_bytes("seg", "more".to_slice, 8i64)  # past the cut
        journal.append_bytes("other", "all".to_slice, 0i64) # not in the baseline

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "seg"
        read_data_size(client_lz4).should eq(-2i64)
        client_lz4.read_string(2).should eq "rt"
        read_filename(client_lz4).should eq "seg"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_string(4).should eq "more"
        read_filename(client_lz4).should eq "other"
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#wait_for" do
    it "blocks until the follower has acked the entry" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        target = record_size("file", "hello world".bytesize)
        wait_for { follower.lag_in_bytes == target }

        confirmed = Channel(Nil).new
        spawn do
          follower.wait_for(lsn)
          confirmed.send nil
        end

        # Should not return before the follower has acked the target bytes
        select
        when confirmed.receive
          fail "wait_for returned before follower acked"
        when timeout(50.milliseconds)
        end

        # Ack the bytes; wait_for should now return
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        select
        when confirmed.receive
        when timeout(2.seconds)
          fail "wait_for did not return after ack"
        end

        follower.lag_in_bytes.should eq 0
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "waits for an entry the sender hasn't written yet" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)
        spawn { follower.ack_loop }

        # Journaled before the sender starts: nothing sent yet, so what has
        # been acked (nothing) must not satisfy the wait.
        journal = LavinMQ::Clustering::Journal.new(1024)
        journal.subscribe(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(lsn) }
        select
        when confirmed.receive
          fail "wait_for returned before the entry was even sent"
        when timeout(50.milliseconds)
        end

        follower.start_sender(journal)
        wait_for { follower.lag_in_bytes > 0 }
        client_socket.write_bytes follower.lag_in_bytes, IO::ByteFormat::LittleEndian
        select
        when result = confirmed.receive
          result.should be_true
        when timeout(2.seconds)
          fail "wait_for did not return after ack"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "unblocks all concurrent waiters when the follower acks" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        target = record_size("file", "hello world".bytesize)
        wait_for { follower.lag_in_bytes == target }

        # The publish confirm loop and definition fences can wait
        # concurrently; a single ack must unblock every waiter whose target
        # it reaches, not just one.
        confirmed = Channel(Bool).new
        3.times { spawn { confirmed.send follower.wait_for(lsn) } }
        sleep 100.milliseconds # let all waiters block on the ack notification

        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        3.times do
          select
          when result = confirmed.receive
            result.should be_true
          when timeout(2.seconds)
            fail "a concurrent wait_for waiter never unblocked"
          end
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "disconnects a connected follower that stops acking, unblocking the waiter" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket) # but never ack

        # Short ack deadline: the follower stays connected but never acks, so
        # ack_loop should give up and disconnect, closing @ack_notify.
        spawn { follower.ack_loop(50.milliseconds) }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)

        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(lsn) }

        select
        when result = confirmed.receive
          result.should be_false # follower was disconnected before acking
        when timeout(2.seconds)
          fail "wait_for did not return after follower was dropped"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "does not disconnect a follower that was idle longer than the ack deadline" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        # Stay idle (no outstanding data) for well over the ack deadline: the
        # deadline must not start ticking until data is actually outstanding.
        spawn { follower.ack_loop(50.milliseconds) }
        journal = start_streaming(follower)
        sleep 200.milliseconds

        # Now publish; a healthy follower acking promptly must NOT be dropped.
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        target = record_size("file", "hello world".bytesize)
        wait_for { follower.lag_in_bytes == target }
        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(lsn) }
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian

        select
        when result = confirmed.receive
          result.should be_true # follower stayed connected and acked
        when timeout(2.seconds)
          fail "wait_for did not return"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Regression: the publish-confirm loop runs on an isolated execution
    # context, but the follower socket's fd belongs to the default context's
    # event loop (ack_loop keeps a read pending on it). The old
    # wait_for_confirm flushed the socket from the calling fiber; when the
    # flush blocked, the cross-context fd handover raised RuntimeError,
    # killing the confirm loop and hanging every publish confirm forever.
    # Socket writes belong to the follower's sender fiber on the default
    # context; a waiter only touches channels and counters.
    it "never writes the socket from the calling fiber, so an isolated execution context can wait safely" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # ack_loop on the default context keeps a read pending on the fd.
        spawn { follower.ack_loop }
        journal = start_streaming(follower)

        # Fill the socket buffers (the client side never reads), then stop at
        # the first blocked write, so the sender's flush of the entry below
        # must block on the event loop.
        filled = Channel(Nil).new(1)
        spawn(name: "socket filler") do
          junk = Bytes.new(65536)
          loop { follower_socket.write junk }
        rescue IO::TimeoutError
          filled.send nil
        rescue IO::Error
          # socket closed at spec end
        end
        select
        when filled.receive
        when timeout(10.seconds)
          fail "socket buffers never filled"
        end

        lsn = journal.append_bytes("file", Bytes.new(1024), 0i64)

        result = Channel(Bool | Exception).new(1)
        Fiber::ExecutionContext::Isolated.new("confirm from isolated EC") do
          result.send follower.wait_for(lsn)
        rescue ex
          result.send ex
        end

        # The follower never acks and never reads; the sender's blocked flush
        # times out and drops the follower, which unblocks the waiter with
        # false. No cross-context IO error may be raised here.
        select
        when r = result.receive
          r.should be_false
        when timeout(10.seconds)
          fail "wait_for never returned from the isolated execution context"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "returns when the follower disconnects before acking" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)

        confirmed = Channel(Nil).new
        spawn do
          follower.wait_for(lsn) # never acked
          confirmed.send nil
        end

        # Closing the socket ends ack_loop, which must unblock the waiter
        client_socket.close
        select
        when confirmed.receive
        when timeout(2.seconds)
          fail "wait_for did not return after follower disconnected"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "the sender" do
    it "flushes an entry to the follower as soon as the journal is drained" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)

        received = Channel(String).new(1)
        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          read_filename(client_lz4)
          size = read_data_size(client_lz4)
          buf = Bytes.new(size.abs)
          client_lz4.read_fully(buf)
          received.send String.new(buf)
        rescue IO::Error
          # socket closed at spec end
        end

        # A small append stays in the LZ4 writer's buffer (auto_flush is
        # off); the sender flushes it since nothing else is queued.
        journal.append_bytes("file", "hello world".to_slice, 0i64)
        select
        when payload = received.receive
          payload.should eq "hello world"
        when timeout(50.milliseconds)
          fail "the sender did not flush the entry to the follower"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#close" do
    # Regression: a follower whose join failed after mark_synced! (e.g. the
    # ISR commit raised) never runs ack_loop, so ack_loop's ensure never
    # closes @ack_notify. close() must mark it dead and unblock waiters
    # itself, or a publish confirm waiting on it would hang forever.
    it "marks a follower whose ack_loop never ran as dead and unblocks waiters" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        drain(client_socket)

        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)

        confirmed = Channel(Bool).new(1)
        spawn { confirmed.send follower.wait_for(lsn) }
        sleep 50.milliseconds # let the waiter block on the ack notification

        follower.close
        select
        when result = confirmed.receive
          result.should be_false
        when timeout(2.seconds)
          fail "wait_for was not unblocked by close"
        end
        follower.dead?.should be_true
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Regression: when a follower stopped reading (e.g. slow hashing during
    # full_sync) the send buffer fills. close() used to call @lz4.close first,
    # whose flush re-blocked on the full buffer until the write timeout and
    # raised — skipping @socket.close, so the socket stayed open. The follower
    # then never saw a disconnect and never reconnected. close must always
    # close the socket.
    it "closes the socket even when the send buffer is full" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        # Fail a blocked write fast instead of after the production timeout.
        follower_socket.write_timeout = 200.milliseconds

        # Fill the socket send buffer; the client never reads, so any further
        # write blocks until the write timeout.
        junk = Bytes.new(65536)
        begin
          loop { follower_socket.write junk }
        rescue IO::TimeoutError
        end
        # Journaled data the sender will try (and fail) to flush
        journal = start_streaming(follower)
        journal.append_bytes("file", "hello world".to_slice, 0i64)

        follower.close
        follower_socket.closed?.should be_true
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#already_synced" do
    it "counts appends below the captured baseline as fully synced" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        follower.capture_synced_baseline({"file1" => 10i64})
        follower.already_synced("file1", 0i64, 5i64).should eq 5
        follower.already_synced("file1", 9i64, 1i64).should eq 1
        # A path absent from the baseline is always delivered in full
        follower.already_synced("other", 0i64, 4i64).should eq 0
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "returns the synced head size for an append straddling the baseline" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # The cut landed mid-record: full_sync delivered bytes [0, 10), the
        # record spans [6, 14) — the follower already has its first 4 bytes.
        follower.capture_synced_baseline({"file1" => 10i64})
        follower.already_synced("file1", 6i64, 8i64).should eq 4
        # A straddle doesn't drop the entry; the next append (at the record's
        # end) does.
        follower.@synced_baseline.has_key?("file1").should be_true
        follower.already_synced("file1", 14i64, 4i64).should eq 0
        follower.@synced_baseline.has_key?("file1").should be_false
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "drops a file's entry once an append reaches its baseline" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        follower.capture_synced_baseline({"file1" => 10i64, "file2" => 20i64})
        # Caught up with file1: nothing already synced, and the entry is dropped
        follower.already_synced("file1", 10i64, 4i64).should eq 0
        follower.@synced_baseline.has_key?("file1").should be_false
        follower.@synced_baseline.has_key?("file2").should be_true
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "resets to a fresh empty hash once the last file catches up" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        baseline = {"file1" => 10i64}
        follower.capture_synced_baseline(baseline)
        follower.already_synced("file1", 10i64, 4i64).should eq 0
        follower.@synced_baseline.empty?.should be_true
        # A fresh hash, not the captured one emptied in place
        follower.@synced_baseline.should_not be(baseline)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "protocol version" do
    it "negotiates version 3 from a version 3 header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
        negotiate(follower, client_socket, LavinMQ::Clustering::StartV3)
        follower.protocol_version.should eq 3
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "rejects a challenge response made with another password" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
        spawn(name: "handshake") { clustering_handshake(client_socket, "bar", LavinMQ::Clustering::StartV3, 1, accepted: false) }
        expect_raises(LavinMQ::Clustering::AuthenticationError) do
          follower.negotiate!("foo")
        end
        client_socket.read_byte.should eq 1u8
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "accepts a version 1 header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
        negotiate(follower, client_socket, LavinMQ::Clustering::Start)
        follower.protocol_version.should eq 1
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "replies with its own header to an unknown one" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
        client_socket.write Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 4, 0, 0]
        expect_raises(LavinMQ::Clustering::InvalidStartHeaderError) do
          follower.negotiate!("foo")
        end
        header = Bytes.new(8)
        client_socket.read_fully(header)
        header.should eq LavinMQ::Clustering::StartV3
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "fsync entries" do
    it "send nothing to a version 1 follower, which syncs before every ack" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, FakeFileIndex.new(data_dir))
        negotiate(follower, client_socket, LavinMQ::Clustering::Start)
        spawn { follower.ack_loop }
        journal = start_streaming(follower)

        journal.fsync("file")
        journal.fsync("")
        journal.delete("other")

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "other"
        follower.sent_bytes.should eq record_size("other", 0)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "delete entries" do
    it "write filename and a zero size marker" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        journal.delete("file1")

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq 0i64

        follower.sent_bytes.should eq record_size("file1", 0)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end
  # Version 3 records carry the journal lsn and an append's offset, and acks
  # carry the applied lsn (see WireV3)
  describe "protocol version 3" do
    it "sends appends with their lsn and offset, ignoring the join cut baseline" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = v3_follower(data_dir, follower_socket, client_socket)
        follower.capture_synced_baseline({"seg" => 6i64}) # v2 would skip "head" and send "rt"
        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        first = journal.append_bytes("seg", "head".to_slice, 0i64)
        journal.append_bytes("seg", "part".to_slice, 4i64)
        journal.append_value("seg", 7u32, 8i64)

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        header = WireV3.read_header(client_lz4)
        header.lsn.should eq first
        header.op.append?.should be_true
        header.path.should eq "seg"
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 0i64 # offset
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64 # length
        client_lz4.read_string(4).should eq "head"

        header = WireV3.read_header(client_lz4)
        header.lsn.should eq first + 1
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64
        client_lz4.read_string(4).should eq "part"

        header = WireV3.read_header(client_lz4)
        header.lsn.should eq first + 2
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 8i64
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 4i64
        client_lz4.read_bytes(UInt32, IO::ByteFormat::LittleEndian).should eq 7u32

        follower.sent_bytes.should eq(
          WireV3.record_size(WireV3::Op::Append, "seg", 4) * 3)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "sends replace, delete and fsync records" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = v3_follower(data_dir, follower_socket, client_socket)
        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        first = journal.replace("file1", "foo".to_slice)
        journal.delete("file2")
        journal.fsync("vhost/queue")
        journal.fsync("")

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        header = WireV3.read_header(client_lz4)
        {header.lsn, header.op, header.path}.should eq({first, WireV3::Op::Replace, "file1"})
        client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 3i64
        client_lz4.read_string(3).should eq "foo"
        header = WireV3.read_header(client_lz4)
        {header.lsn, header.op, header.path}.should eq({first + 1, WireV3::Op::Delete, "file2"})
        header = WireV3.read_header(client_lz4)
        {header.lsn, header.op, header.path}.should eq({first + 2, WireV3::Op::Fsync, "vhost/queue"})
        header = WireV3.read_header(client_lz4)
        {header.lsn, header.op, header.path}.should eq({first + 3, WireV3::Op::Fsync, ""})

        follower.sent_bytes.should eq(
          WireV3.record_size(WireV3::Op::Replace, "file1", 3) +
          WireV3.record_size(WireV3::Op::Delete, "file2") +
          WireV3.record_size(WireV3::Op::Fsync, "vhost/queue") +
          WireV3.record_size(WireV3::Op::Fsync, ""))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "is satisfied by the applied lsn, not by received bytes" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = v3_follower(data_dir, follower_socket, client_socket)
        drain(client_socket)
        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        size = WireV3.record_size(WireV3::Op::Append, "file", 11)
        wait_for { follower.lag_in_bytes == size }

        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(lsn) }

        # Everything received, but not applied yet: a progress ack
        ack(client_socket, lsn - 1, size)
        wait_for { follower.acked_bytes == size }
        follower.lag_in_bytes.should eq 0
        select
        when confirmed.receive
          fail "wait_for returned on received bytes without the entry being applied"
        when timeout(100.milliseconds)
        end

        ack(client_socket, lsn, size)
        select
        when result = confirmed.receive
          result.should be_true
        when timeout(2.seconds)
          fail "wait_for did not return after the applied ack"
        end
        follower.applied_lsn.should eq lsn
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "returns at once from a wait for an entry before the join cut" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = v3_follower(data_dir, follower_socket, client_socket)
        spawn { follower.ack_loop }
        journal = LavinMQ::Clustering::Journal.new(1024)
        before = journal.delete("old") # delivered by the full sync, never streamed
        journal.subscribe(follower)
        follower.start_sender(journal)

        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for(before) }
        select
        when result = confirmed.receive
          result.should be_true
        when timeout(1.second)
          fail "wait_for an entry below the cut blocked on an ack that can't come"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "reads an ack that arrives in two pieces" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        follower = v3_follower(data_dir, follower_socket, client_socket)
        drain(client_socket)
        spawn { follower.ack_loop }
        journal = start_streaming(follower)
        lsn = journal.append_bytes("file", "hello world".to_slice, 0i64)
        size = WireV3.record_size(WireV3::Op::Append, "file", 11)
        wait_for { follower.lag_in_bytes == size }

        buf = Bytes.new(WireV3::ACK_SIZE)
        IO::ByteFormat::LittleEndian.encode(lsn.to_i64, buf[0, 8])
        IO::ByteFormat::LittleEndian.encode(size, buf[8, 8])
        client_socket.write buf[0, 5]
        sleep 250.milliseconds # longer than the ack loop's read timeout
        client_socket.write buf[5..]

        wait_for { follower.applied_lsn == lsn }
        follower.lag_in_bytes.should eq 0
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end
end
