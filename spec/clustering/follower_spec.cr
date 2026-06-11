require "../spec_helper"
require "lz4"

private def read_filename(io) : String
  size = io.read_bytes Int32, IO::ByteFormat::LittleEndian
  io.read_string(size)
end

private def read_data_size(io) : Int64
  io.read_bytes Int64, IO::ByteFormat::LittleEndian
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
  end

  describe "#stream changes" do
    it "should fully sync on graceful shutdown" do
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

        10.times do
          follower.append("#{data_dir}/file", "hello world".to_slice)
        end
        spawn do
          follower.ack_loop
        end

        closed = false
        wg = WaitGroup.new
        wg.add(1)
        spawn do
          follower.close
          closed = true
          wg.done
        end

        # Send an ack back to satisfy lag check if needed,
        # though close doesn't strictly depend on it now.
        # But let's verify lag reaches 0.
        client_socket.write_bytes follower.lag_in_bytes.to_i64, IO::ByteFormat::LittleEndian

        # Wait for closing fiber to finish
        wg.wait
        closed.should be_true
        follower.lag_in_bytes.should eq 0
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

        lag_ch = Channel(Int64).new(1)
        spawn do
          lag_ch.send follower.replace("file1")
          follower.close
        end

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        data_size = read_data_size(client_lz4)
        data_size.should eq 3i64
        buf = Bytes.new(data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        lag_ch.receive.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + 3)
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

        lag_ch = Channel(Int64).new(1)
        spawn do
          lag_ch.send follower.append("bar", "foo".to_slice)
          follower.close
        end

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "bar"
        data_size = read_data_size(client_lz4)
        data_size.should eq(-3i64)
        buf = Bytes.new(-data_size)
        client_lz4.read_fully(buf)
        String.new(buf).should eq "foo"

        lag_ch.receive.should eq(sizeof(Int32) + "bar".bytesize + sizeof(Int64) + 3)
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

        lag_ch = Channel(Int64).new(1)
        spawn do
          lag_ch.send follower.append("file1", 123i32)
          follower.close
        end

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian).should eq 123i32

        lag_ch.receive.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + sizeof(Int32))
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

        lag_ch = Channel(Int64).new(1)
        spawn do
          lag_ch.send follower.append("file1", 123u32)
          follower.close
        end

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq(-4i64)
        client_lz4.read_bytes(UInt32, IO::ByteFormat::LittleEndian).should eq 123u32

        lag_ch.receive.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64) + sizeof(UInt32))
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

        lag_ch = Channel(Int64).new(1)
        spawn do
          lag_ch.send follower.delete("file1")
          follower.close
        end

        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        read_filename(client_lz4).should eq "file1"
        read_data_size(client_lz4).should eq 0i64

        lag_ch.receive.should eq(sizeof(Int32) + "file1".bytesize + sizeof(Int64))
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end
end
