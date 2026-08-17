require "../spec_helper"
require "lz4"

private def read_filename(io) : String
  size = io.read_bytes Int32, IO::ByteFormat::LittleEndian
  io.read_string(size)
end

private def read_data_size(io) : Int64
  io.read_bytes Int64, IO::ByteFormat::LittleEndian
end

# What Clustering::Server#request_sync queues: the record, then the flush that
# puts it on the socket and releases `wg`.
private def request_sync(follower, wg : WaitGroup)
  follower.control LavinMQ::Clustering::SyncControlPacket.new
  follower.control LavinMQ::Clustering::FlushPacket.new(wg)
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

        # Fiber to drain client socket so follower doesn't block on write/flush
        spawn do
          buf = uninitialized UInt8[4096]
          loop do
            client_socket.read(buf.to_slice)
          end
        rescue IO::Error
          # socket closed
        end

        spawn { follower.ack_loop }
        10.times do
          follower.append("#{data_dir}/file", "hello world".to_slice)
        end
        target = follower.lag_in_bytes

        # Ack the outstanding bytes and wait until the follower has registered
        # them, so the lag-0 assertion is deterministic (close no longer flushes
        # or waits for acks itself).
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for_confirm }
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
  end

  describe "#replace" do
    it "writes filename, file size, and file contents to the LZ4 stream" do
      with_datadir do |data_dir|
        File.write File.join(data_dir, "file1"), "foo"

        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # close no longer flushes buffered data, so drive the flush via ack_loop
        # + request_flush (see the #request_flush test) to read it off the wire.
        spawn { follower.ack_loop }
        lag = follower.replace("file1")
        follower.request_flush

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        data_size = read_data_size(client_lz4)
        data_size.should eq 3i64
        buf = Bytes.new(data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        lag.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + 3)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "captures the file size at call time so later appends do not bleed into the stream" do
      with_datadir do |data_dir|
        File.write File.join(data_dir, "file1"), "foo"

        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # Drain the client side so the synchronous replace doesn't block on LZ4 writes
        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        lag = follower.replace("file1")
        File.write File.join(data_dir, "file1"), "appended-after-replace", mode: "a"
        lag.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + 3)
        follower.close
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#append" do
    it "writes filename and Bytes payload with a negative size header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        lag = follower.append("bar", "foo".to_slice)
        follower.request_flush

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "bar"
        data_size = read_data_size(client_lz4)
        data_size.should eq(-3i64)
        buf = Bytes.new(-data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        lag.should eq(sizeof(Int32) + "bar".bytesize + sizeof(Int64) + 3)
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "writes Int32 value little-endian with a -4 size header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        lag = follower.append("file1", 123i32)
        follower.request_flush

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian).should eq 123i32

        lag.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + sizeof(Int32))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "writes UInt32 value little-endian with a -4 size header" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        lag = follower.append("file1", 123u32)
        follower.request_flush

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_bytes(UInt32, IO::ByteFormat::LittleEndian).should eq 123u32

        lag.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + sizeof(UInt32))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#wait_for_confirm" do
    it "blocks until the follower has acked the bytes sent so far" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # Drain the client side so synchronous appends don't block on LZ4 writes
        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_lz4.read(buf.to_slice) }
        rescue IO::Error
        end

        follower.append("#{data_dir}/file", "hello world".to_slice)
        target = follower.lag_in_bytes
        spawn { follower.ack_loop }

        confirmed = Channel(Nil).new
        spawn do
          follower.wait_for_confirm
          confirmed.send nil
        end

        # Should not return before the follower has acked the target bytes
        select
        when confirmed.receive
          fail "wait_for_confirm returned before follower acked"
        when timeout(50.milliseconds)
        end

        # Ack the bytes; wait_for_confirm should now return
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        select
        when confirmed.receive
        when timeout(2.seconds)
          fail "wait_for_confirm did not return after ack"
        end

        follower.lag_in_bytes.should eq 0
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

        # Drain the client side so synchronous appends don't block on LZ4 writes
        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        follower.append("#{data_dir}/file", "hello world".to_slice)
        target = follower.lag_in_bytes
        spawn { follower.ack_loop }

        # The publish confirm loop and definition fences can wait
        # concurrently; a single ack must unblock every waiter whose target
        # it reaches, not just one.
        confirmed = Channel(Bool).new
        3.times { spawn { confirmed.send follower.wait_for_confirm } }
        sleep 100.milliseconds # let all waiters block on the ack notification

        client_socket.write_bytes target, IO::ByteFormat::LittleEndian
        3.times do
          select
          when result = confirmed.receive
            result.should be_true
          when timeout(2.seconds)
            fail "a concurrent wait_for_confirm waiter never unblocked"
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

        # Drain the client side so the flush doesn't block, but never send an ack
        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        follower.append("#{data_dir}/file", "hello world".to_slice)
        # Short ack deadline: the follower stays connected but never acks, so
        # ack_loop should give up and disconnect, closing @ack_notify.
        spawn { follower.ack_loop(50.milliseconds) }

        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for_confirm }

        select
        when result = confirmed.receive
          result.should be_false # follower was disconnected before acking
        when timeout(2.seconds)
          fail "wait_for_confirm did not return after follower was dropped"
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

        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        # Stay idle (no outstanding data) for well over the ack deadline: the
        # deadline must not start ticking until data is actually outstanding.
        spawn { follower.ack_loop(50.milliseconds) }
        sleep 200.milliseconds

        # Now publish; a healthy follower acking promptly must NOT be dropped.
        follower.append("#{data_dir}/file", "hello world".to_slice)
        target = follower.lag_in_bytes
        confirmed = Channel(Bool).new
        spawn { confirmed.send follower.wait_for_confirm }
        client_socket.write_bytes target, IO::ByteFormat::LittleEndian

        select
        when result = confirmed.receive
          result.should be_true # follower stayed connected and acked
        when timeout(2.seconds)
          fail "wait_for_confirm did not return"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Regression: the publish-confirm loop runs on an isolated execution
    # context, but the follower socket's fd belongs to the default context's
    # event loop (ack_loop keeps a read pending on it). wait_for_confirm used
    # to flush the socket from the calling fiber; when the flush blocked, the
    # cross-context fd handover raised RuntimeError, killing the confirm loop
    # and hanging every publish confirm forever. The flush must instead be
    # delegated to a follower-owned fiber on the default context.
    it "never writes the socket from the calling fiber, so an isolated execution context can wait safely" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # Outstanding data pending inside the LZ4 writer (well below its
        # block size, so the append itself doesn't touch the socket); any
        # flush must now write to the socket.
        follower.append("#{data_dir}/file", Bytes.new(1024))

        # Fill the socket buffers (the client side never reads), then stop at
        # the first blocked write, so a later flush of the pending LZ4 data
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

        # ack_loop on the default context keeps a read pending on the fd.
        spawn { follower.ack_loop }
        sleep 20.milliseconds

        result = Channel(Bool | Exception).new(1)
        Fiber::ExecutionContext::Isolated.new("confirm from isolated EC") do
          result.send follower.wait_for_confirm
        rescue ex
          result.send ex
        end

        # The follower never acks; eventually ack_loop gives up (its own
        # blocked flush times out) and unblocks the waiter with false. The
        # old direct flush instead raised RuntimeError here: the blocked
        # write tried to move the fd to the isolated context's event loop
        # while ack_loop's read was pending on the default one.
        select
        when r = result.receive
          r.should be_false # never acked — and no cross-context IO error raised
        when timeout(10.seconds)
          fail "wait_for_confirm never returned from the isolated execution context"
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

        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        follower.append("#{data_dir}/file", "hello world".to_slice)
        spawn { follower.ack_loop }

        confirmed = Channel(Nil).new
        spawn do
          follower.wait_for_confirm # never acked
          confirmed.send nil
        end

        # Closing the socket ends ack_loop, which must unblock the waiter
        client_socket.close
        select
        when confirmed.receive
        when timeout(2.seconds)
          fail "wait_for_confirm did not return after follower disconnected"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#request_flush" do
    # Regression: the control queue was a Channel(Nil), and receive? returns
    # nil both for a delivered message and for a closed channel, so the loop
    # exited on the first request without ever flushing — every confirm then
    # waited for ack_loop's 100ms fallback flush instead.
    it "pushes buffered bytes to the follower without waiting for the ack-loop fallback flush" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }

        # A small append stays in the LZ4 writer's buffer (auto_flush is
        # off); only a flush moves it to the socket.
        follower.append("#{data_dir}/file", "hello world".to_slice)

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

        follower.request_flush
        # Must arrive via control_loop, well before ack_loop's 100ms fallback
        select
        when payload = received.receive
          payload.should eq "hello world"
        when timeout(50.milliseconds)
          fail "request_flush did not flush buffered bytes to the follower"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#control" do
    packet_size = LavinMQ::Clustering::SyncControlPacket::RECORD.bytesize.to_i64

    it "sends a $ctrl/sync record with an empty body, without waiting for the ack-loop fallback flush" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }

        received = Channel({String, Int64}).new(1)
        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          filename = read_filename(client_lz4)
          received.send({filename, read_data_size(client_lz4)})
        rescue IO::Error
          # socket closed at spec end
        end

        request_sync follower, WaitGroup.new

        # Must arrive via control_loop, well before ack_loop's 100ms fallback
        select
        when record = received.receive
          record.should eq({LavinMQ::Clustering::SyncControlPacket::PATH, 0i64})
        when timeout(50.milliseconds)
          fail "the packet's record was not flushed to the follower"
        end

        # The control path is not a file; nothing may be created for it
        Dir.exists?(File.join(data_dir, "$ctrl")).should be_false
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # What makes the flush packet worth waiting for: nothing else pushes the
    # record out, so a waiter it releases knows the record is on the socket.
    it "leaves the record in the compressor until a flush packet follows it" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }

        received = Channel(String).new(1)
        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          received.send read_filename(client_lz4)
        rescue IO::Error
          # socket closed at spec end
        end

        follower.control LavinMQ::Clustering::SyncControlPacket.new
        # Written and counted, but not pushed: well inside ack_loop's 100ms
        # fallback flush, the follower must have seen nothing.
        select
        when filename = received.receive
          fail "the record reached the wire without a flush packet: #{filename}"
        when timeout(50.milliseconds)
        end
        follower.lag_in_bytes.should eq packet_size

        wg = WaitGroup.new
        follower.control LavinMQ::Clustering::FlushPacket.new(wg)
        wg.wait # released once the flush has run, so the record is out

        select
        when filename = received.receive
          filename.should eq LavinMQ::Clustering::SyncControlPacket::PATH
        when timeout(50.milliseconds)
          fail "the flush packet did not push the record to the follower"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # The invariant the fence rests on: a byte count is only a position in the
    # stream if every record is counted where it is written. Counting one when
    # it's *requested* puts it in the total ahead of appends that reach the wire
    # first, and those appends alone can then satisfy wait_for_confirm's target.
    it "counts the record only once written, not when requested" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          loop do
            read_filename(client_lz4)
            read_data_size(client_lz4)
          end
        rescue IO::Error
          # socket closed at spec end
        end

        # No ack_loop yet, so there is no control_loop to write the packet.
        wg = WaitGroup.new
        request_sync follower, wg
        follower.lag_in_bytes.should eq 0

        written = Channel(Nil).new(1)
        spawn do
          wg.wait
          written.send nil
        end

        spawn { follower.ack_loop }

        select
        when written.receive
        when timeout(500.milliseconds)
          fail "the packet's waiter was not released after it was written"
        end
        follower.lag_in_bytes.should eq packet_size
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Consequence of counting at the write: the record's bytes sit after
    # everything written before it, so acking those can't satisfy a target
    # taken once the record is out.
    it "is not satisfied by acks for bytes written before the record" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          loop do
            read_filename(client_lz4)
            size = read_data_size(client_lz4)
            client_lz4.skip(size.abs)
          end
        rescue IO::Error
          # socket closed at spec end
        end

        # Written (and counted) before the packet, which is still queued.
        append_size = follower.append("#{data_dir}/file", Bytes.new(1024))
        append_size.should be > packet_size # or the ack below couldn't overshoot

        wg = WaitGroup.new
        request_sync follower, wg
        spawn { follower.ack_loop }
        wg.wait

        confirmed = Channel(Bool).new(1)
        spawn { confirmed.send follower.wait_for_confirm }

        client_socket.write_bytes append_size, IO::ByteFormat::LittleEndian
        select
        when confirmed.receive
          fail "wait_for_confirm was satisfied by the append written before the record"
        when timeout(100.milliseconds)
        end

        client_socket.write_bytes packet_size, IO::ByteFormat::LittleEndian
        select
        when ok = confirmed.receive
          ok.should be_true
        when timeout(1.second)
          fail "wait_for_confirm did not return after the record was acked"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # A full queue may drop a plain flush request — the packets in it flush
    # those bytes anyway. It may not drop a packet: nothing would tell the
    # follower to sync, yet the confirm would go out.
    it "does not drop the packet when the control queue is full" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # No ack_loop yet, so nothing drains the queue: these take every slot.
        LavinMQ::Clustering::Follower::CONTROL_QUEUE_CAPACITY.times { follower.request_flush }

        wg = WaitGroup.new
        spawn { request_sync follower, wg }

        received = Channel(String).new(1)
        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          filename = read_filename(client_lz4)
          read_data_size(client_lz4)
          received.send filename
        rescue IO::Error
          # socket closed at spec end
        end

        spawn { follower.ack_loop }

        select
        when filename = received.receive
          filename.should eq LavinMQ::Clustering::SyncControlPacket::PATH
        when timeout(500.milliseconds)
          fail "the sync record never reached the wire"
        end
        follower.lag_in_bytes.should eq packet_size
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Every packet gets its own record, so a second request can't be satisfied
    # by a record already on the wire.
    it "writes a record per packet, counting each one" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        records = Channel(String).new(2)
        spawn do
          client_lz4 = Compress::LZ4::Reader.new(client_socket)
          loop do
            filename = read_filename(client_lz4)
            read_data_size(client_lz4)
            records.send filename
          end
        rescue IO::Error
          # socket closed at spec end
        end

        spawn { follower.ack_loop }

        wg = WaitGroup.new
        2.times { request_sync follower, wg }

        written = Channel(Nil).new(1)
        spawn do
          wg.wait
          written.send nil
        end

        select
        when written.receive
        when timeout(500.milliseconds)
          fail "not every packet released its waiter"
        end

        2.times do
          select
          when filename = records.receive
            filename.should eq LavinMQ::Clustering::SyncControlPacket::PATH
          when timeout(500.milliseconds)
            fail "a packet did not get a record of its own"
          end
        end
        follower.lag_in_bytes.should eq packet_size * 2
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # A waiter is released per follower, so one that will never write the packet
    # has to say so — or the publish confirm loop blocks on it forever.
    it "releases the waiter of a packet queued for a dead follower" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        follower.close
        follower.dead?.should be_true

        wg = WaitGroup.new
        request_sync follower, wg

        released = Channel(Nil).new(1)
        spawn do
          wg.wait
          released.send nil
        end
        select
        when released.receive
        when timeout(500.milliseconds)
          fail "a packet queued for a dead follower never released its waiter"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "releases the waiters of packets still queued when the follower is closed" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # Queued while there is no control_loop to carry it out, then released
        # by close's drain rather than by a write.
        wg = WaitGroup.new
        request_sync follower, wg

        released = Channel(Nil).new(1)
        spawn do
          wg.wait
          released.send nil
        end

        follower.close

        select
        when released.receive
        when timeout(500.milliseconds)
          fail "closing the follower stranded a queued packet's waiter"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    # Once the follower is closed no control_loop will free a slot, so a packet
    # waiting for one has to fail — and fail released, not stranded.
    it "releases the waiter of a packet still waiting for a slot when the follower is closed" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        # No ack_loop, so nothing drains the queue: these take every slot and
        # the packet below has to wait for one.
        LavinMQ::Clustering::Follower::CONTROL_QUEUE_CAPACITY.times { follower.request_flush }

        wg = WaitGroup.new
        released = Channel(Nil).new(1)
        spawn do
          request_sync follower, wg
          wg.wait
          released.send nil
        end
        sleep 50.milliseconds # let the sender block on the full queue

        follower.close

        select
        when released.receive
        when timeout(500.milliseconds)
          fail "closing the follower stranded a packet blocked on the channel"
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

        # Drain the client side so close's final flush doesn't block
        spawn do
          buf = uninitialized UInt8[4096]
          loop { client_socket.read(buf.to_slice) }
        rescue IO::Error
        end

        follower.append("#{data_dir}/file", "hello world".to_slice)

        confirmed = Channel(Bool).new(1)
        spawn { confirmed.send follower.wait_for_confirm }
        sleep 50.milliseconds # let the waiter block on the ack notification

        follower.close
        select
        when result = confirmed.receive
          result.should be_false
        when timeout(2.seconds)
          fail "wait_for_confirm was not unblocked by close"
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

        # Buffered data that a (now removed) flush-on-close would try to send.
        follower.append("#{data_dir}/file", "hello world".to_slice)

        # Fill the socket send buffer; the client never reads, so any further
        # write blocks until the write timeout.
        junk = Bytes.new(65536)
        begin
          loop { follower_socket.write junk }
        rescue IO::TimeoutError
        end

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

  describe "#delete" do
    it "writes filename and a zero size marker" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)

        spawn { follower.ack_loop }
        lag = follower.delete("file1")
        follower.request_flush

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq 0i64

        lag.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end
end
