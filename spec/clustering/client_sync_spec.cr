require "../spec_helper"
require "lz4"

module ClientSyncSpec
  extend ClusteringSpecHelper
  # `extend` only copies methods, not the module's nested types.
  alias TestClient = ClusteringSpecHelper::TestClient

  # Runs one file-list comparison pass against a leader offering `leader_files`,
  # with `resync` picking the reconnect variant (see TestClient).
  def self.sync_with_leader(client : TestClient, leader_files : Hash(String, String), resync = false)
    server_io, client_io = UNIXSocket.pair
    lz4_reader = Compress::LZ4::Reader.new(client_io)
    done = Channel(Nil).new
    spawn do
      simulate_leader(server_io, leader_files)
      done.send nil
    end
    if resync
      client.resync_files_public(client_io, lz4_reader)
    else
      client.sync_files_public(client_io, lz4_reader)
    end
    select
    when done.receive
    when timeout(1.second)
      raise "leader fiber timed out"
    end
  ensure
    server_io.try &.close
    client_io.try &.close
  end

  # One replication record: a negative length appends, a positive one replaces.
  def self.write_record(lz4 : Compress::LZ4::Writer, filename : String, len : Int64, bytes : Bytes)
    lz4.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
    lz4.write filename.to_slice
    lz4.write_bytes len, IO::ByteFormat::LittleEndian
    lz4.write bytes
    lz4.flush
  end

  # Bytes the follower acks for a whole record: framing plus payload.
  def self.record_size(filename : String, payload_size : Int) : Int64
    (sizeof(Int32) + filename.bytesize + sizeof(Int64) + payload_size).to_i64
  end

  def self.read_acks(io : IO, target : Int64)
    io.read_timeout = 2.seconds
    acked = 0i64
    while acked < target
      acked += io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
    end
    acked.should eq target
  end

  # follow() was never called in this harness, so satisfy close's follower-done
  # handshake ourselves.
  def self.close_client(client : TestClient)
    spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
    client.close
  end

  def self.persisted_checksums(data_dir : String) : Hash(String, {String, String})
    path = File.join(data_dir, "checksums.sha1")
    return Hash(String, {String, String}).new unless File.exists?(path)
    File.read_lines(path).to_h do |line|
      hash, _, rest = line.partition(" ")
      size, _, filename = rest.partition(" *")
      {filename, {hash, size}} # a later line wins, as in Checksums#restore
    end
  end

  # Every line in checksums.sha1 must be the hash and size of what's on disk
  # right now: one that isn't makes the next sync throw the file away and
  # re-fetch it from the leader. Returns the hashes for further assertions.
  def self.checksums_matching_disk(data_dir : String) : Hash(String, String)
    persisted_checksums(data_dir).to_h do |filename, (hash, size)|
      path = File.join(data_dir, filename)
      File.exists?(path).should be_true, "checksum for missing file #{filename}"
      content = File.read(path)
      hash.should eq Digest::SHA1.digest(content).hexstring
      size.should eq content.bytesize.to_s
      {filename, hash}
    end
  end

  describe LavinMQ::Clustering::Client do
    describe "stream_changes" do
      # Regression: a single large action must be acked incrementally as its
      # payload is written, not just once when the whole action completes.
      # Otherwise a big message/file streamed over a slow link goes un-acked
      # for seconds and the leader evicts the healthy follower on its ack
      # deadline.
      it "acks a large action incrementally instead of only when it completes" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          filename = "stream_file"
          buffer_size = LavinMQ::Clustering::Client::BUFFER_SIZE
          chunk = Bytes.new(buffer_size, 0xAB_u8)
          rest = Bytes.new(buffer_size * 2, 0xCD_u8) # full payload spans 3 chunks
          payload_size = (chunk.size + rest.size).to_i64
          framing = (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
            # socket closed to end the loop
          end

          # Announce an append of the whole payload but only send the first
          # chunk, withholding the rest.
          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes -payload_size, IO::ByteFormat::LittleEndian
          lz4_writer.write chunk
          lz4_writer.flush

          # The follower must ack the framing + first chunk without having
          # received the rest. Before incremental acks it blocked in
          # read_fully until the whole action arrived and acked nothing here.
          leader_io.read_timeout = 2.seconds
          acked = 0i64
          while acked < framing + chunk.size
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          acked.should eq(framing + chunk.size)

          # Send the remainder; the follower acks the rest and persists the file.
          lz4_writer.write rest
          lz4_writer.flush
          while acked < framing + payload_size
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          acked.should eq(framing + payload_size)

          File.size(File.join(data_dir, filename)).should eq payload_size
          client_socket.close
        end
      end

      it "skips local sync before acking streamed bytes when sync is disabled" do
        with_datadir do |data_dir|
          client = make_client(data_dir, sync: false)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          filename = "no_sync_stream_file"
          payload = "replicated bytes"
          framing = (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64

          spawn(name: "client no-sync stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes -payload.bytesize.to_i64, IO::ByteFormat::LittleEndian
          lz4_writer.write payload.to_slice
          lz4_writer.flush

          leader_io.read_timeout = 2.seconds
          acked = 0i64
          while acked < framing + payload.bytesize
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end

          acked.should eq framing + payload.bytesize
          client.syncs_started.should eq 0
          File.read(File.join(data_dir, filename)).should eq payload
          client_socket.close
        end
      end

      # A delete record's framing bytes are its only bytes, so their ack tells
      # the leader the deletion is durable; it may only be sent once the file
      # is actually gone, or a failover could resurrect deleted data.
      it "acks a delete only after the file has been deleted" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          filename = "doomed_file"
          File.write File.join(data_dir, filename), "data"
          framing = (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes 0i64, IO::ByteFormat::LittleEndian # delete marker
          lz4_writer.flush

          # Receiving the full ack implies the unlink has been applied.
          leader_io.read_timeout = 2.seconds
          acked = 0i64
          while acked < framing
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          acked.should eq framing
          File.exists?(File.join(data_dir, filename)).should be_false
          client_socket.close
        end
      end

      # Regression: the streamed-bytes logging fiber looped forever, so every
      # reconnect (each of which spawns one) leaked another fiber logging the
      # same counter.
      it "stops the streamed bytes logging fiber when the stream ends" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
            # leader disconnected below to end the loop
          end
          wait_for { client.log_loops_running == 1 }

          leader_io.close # disconnect, so stream_changes raises and returns
          wait_for { client.log_loops_running.zero? }
          client_socket.close
        end
      end

      it "does not ack a delete that could not be applied" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          # A directory can't be unlinked, so the delete raises before it is
          # applied; the record must not be acked (the follower disconnects
          # and re-syncs instead of overstating its progress).
          filename = "undeletable"
          Dir.mkdir_p File.join(data_dir, filename)

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes 0i64, IO::ByteFormat::LittleEndian # delete marker
          lz4_writer.flush

          leader_io.read_timeout = 500.milliseconds
          expect_raises(IO::TimeoutError) do
            leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          client_socket.close
        end
      end

      # The final ack of a replace marks the whole record as durable, so it
      # may only be sent once the .tmp file has been renamed into place;
      # otherwise the leader can treat the follower as caught up while it
      # still exposes the old file.
      it "acks the end of a replace only after the file is renamed into place" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          filename = "replaced_file"
          File.write File.join(data_dir, filename), "old content"
          content = "new content"
          framing = (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes content.bytesize.to_i64, IO::ByteFormat::LittleEndian
          lz4_writer.write content.to_slice
          lz4_writer.flush

          # Receiving the full ack implies the rename has been applied.
          leader_io.read_timeout = 2.seconds
          acked = 0i64
          while acked < framing + content.bytesize
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          acked.should eq framing + content.bytesize
          File.read(File.join(data_dir, filename)).should eq content
          File.exists?(File.join(data_dir, "#{filename}.tmp")).should be_false
          client_socket.close
        end
      end

      it "does not send a replace's final ack if the rename could not be applied" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          # A file can't be renamed over a directory, so the replace raises
          # after writing the .tmp file but before it is installed; the
          # payload's final ack must never be sent.
          filename = "unreplaceable"
          Dir.mkdir_p File.join(data_dir, filename)
          content = "new content"
          framing = (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes content.bytesize.to_i64, IO::ByteFormat::LittleEndian
          lz4_writer.write content.to_slice
          lz4_writer.flush

          # The framing bytes are acked up front, but nothing further.
          leader_io.read_timeout = 2.seconds
          acked = 0i64
          while acked < framing
            acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          acked.should eq framing
          leader_io.read_timeout = 500.milliseconds
          expect_raises(IO::TimeoutError) do
            leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
          end
          client_socket.close
        end
      end
    end

    # Hashing is CPU bound and the leader waits, holding its sync lock, for our
    # file requests. So it all happens before the follower connects.
    describe "hash_local_files" do
      it "hashes every local file, including files the leader doesn't have" do
        with_datadir do |data_dir|
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), "a"
          File.write File.join(data_dir, "definitions.amqp"), "b"

          client = make_client(data_dir)
          client.hash_local_files_public

          client.@checksums["queue1/messages.dat"]?.should eq Digest::SHA1.digest("a")
          client.@checksums["definitions.amqp"]?.should eq Digest::SHA1.digest("b")
          # Persisted too, so a crash before the sync doesn't waste the work.
          checksums_file = File.read(File.join(data_dir, "checksums.sha1"))
          checksums_file.should contain "#{Digest::SHA1.digest("a").hexstring} *queue1/messages.dat"
          checksums_file.should contain "#{Digest::SHA1.digest("b").hexstring} *definitions.amqp"
        end
      end

      it "skips local-only files the leader never sends" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, ".clustering_id"), "id"

          client = make_client(data_dir)
          client.hash_local_files_public

          client.@checksums[".clustering_id"]?.should be_nil
          client.@checksums[".lock"]?.should be_nil
          client.@checksums["checksums.sha1"]?.should be_nil
        end
      end

      it "skips files already in the checksum cache" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, "cached.dat"), "original"
          client = make_client(data_dir)
          cached = Digest::SHA1.digest("cached")
          client.@checksums.append("cached.dat", cached)

          client.hash_local_files_public

          client.files_hashed.should eq 0
          client.@checksums["cached.dat"]?.should eq cached
        end
      end

      # This pass hashes files the leader doesn't have, so an unreadable file
      # must not abort it and wedge the follower before it even dials.
      it "keeps hashing after a file it can't read" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, "unreadable.dat"), "nope"
          File.write File.join(data_dir, "readable.dat"), "yep"

          client = make_client(data_dir)
          client.fail_hash_for = "unreadable.dat"
          client.hash_local_files_public

          client.@checksums["unreadable.dat"]?.should be_nil
          client.@checksums["readable.dat"]?.should eq Digest::SHA1.digest("yep")
        end
      end

      # Regression: the compare loop used to hash while the leader waited for
      # our file requests, stalling it for as long as hashing took.
      it "leaves no hashing to do while the leader is connected" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), content

          client = make_client(data_dir)
          client.hash_local_files_public
          client.files_hashed.should eq 1

          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)
          requested = Channel(Array(String)).new
          spawn do
            requested.send simulate_leader(server_io, {"queue1/messages.dat" => content})
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when files = requested.receive
            files.should be_empty # matching hash, nothing to re-fetch
          when timeout(1.second)
            fail "leader fiber timed out"
          end
          client.files_hashed.should eq 1 # nothing hashed while connected
        end
      end

      # Files the leader lacks are hashed but then deleted, so their checksums
      # must go too or the map grows dead paths on every sync.
      it "drops checksums of files deleted because the leader lacks them" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, "orphan.dat"), "gone tomorrow"
          client = make_client(data_dir)
          client.hash_local_files_public
          client.@checksums["orphan.dat"]?.should_not be_nil

          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)
          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {} of String => String)
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          File.exists?(File.join(data_dir, "orphan.dat")).should be_false
          client.@checksums["orphan.dat"]?.should be_nil
        end
      end
    end

    describe "sync_files directory cleanup" do
      it "deletes directory not on leader" do
        with_datadir do |data_dir|
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), "data"

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {} of String => String)
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_false
        end
      end

      it "deletes nested directory tree absent from leader" do
        with_datadir do |data_dir|
          Dir.mkdir_p File.join(data_dir, "a", "b", "c")
          File.write File.join(data_dir, "a", "b", "c", "file.dat"), "data"

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {} of String => String)
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "a", "b", "c")).should be_false
          Dir.exists?(File.join(data_dir, "a", "b")).should be_false
          Dir.exists?(File.join(data_dir, "a")).should be_false
        end
      end

      it "keeps directories containing files present on the leader" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), content

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_true
          File.exists?(File.join(data_dir, "queue1", "messages.dat")).should be_true
        end
      end

      it "deletes only directories absent from leader" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          Dir.mkdir_p File.join(data_dir, "queue2")
          File.write File.join(data_dir, "queue1", "messages.dat"), content
          File.write File.join(data_dir, "queue2", "messages.dat"), content

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_true
          Dir.exists?(File.join(data_dir, "queue2")).should be_false
        end
      end
    end

    describe "checksum persistence during sync" do
      # Regression for #1834: a hash computed while comparing a pre-existing
      # local file must be persisted to checksums.sha1 immediately (appended),
      # so a crash mid-sync doesn't lose the work and force a full re-hash.
      it "persists compare-loop hashes incrementally, before close" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), content

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          # Persisted during sync, without any close/store.
          checksums_file = File.join(data_dir, "checksums.sha1")
          File.exists?(checksums_file).should be_true
          expected = Digest::SHA1.digest(content).hexstring
          File.read(checksums_file).should contain "#{expected} #{content.bytesize} *queue1/messages.dat"
        end
      end

      # A file the follower lacks is requested and received via file_from_socket;
      # its hash must also be persisted so a crash mid-sync doesn't re-hash it.
      it "persists hashes of received files, before close" do
        with_datadir do |data_dir|
          content = "received payload"
          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          File.read(File.join(data_dir, "queue1", "messages.dat")).should eq content
          checksums_file = File.join(data_dir, "checksums.sha1")
          File.exists?(checksums_file).should be_true
          expected = Digest::SHA1.digest(content).hexstring
          File.read(checksums_file).should contain "#{expected} #{content.bytesize} *queue1/messages.dat"
        end
      end

      # Regression: checksums.sha1 is local-only and the leader never sends it,
      # so the "delete files not on leader" sweep must not wipe it — otherwise
      # the second sync pass (sync runs sync_files twice) deletes hashes the
      # first pass persisted.
      it "keeps checksums.sha1 across repeated sync passes" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), content
          client = make_client(data_dir)

          2.times do
            server_io, client_io = UNIXSocket.pair
            lz4_reader = Compress::LZ4::Reader.new(client_io)
            done = Channel(Nil).new
            spawn do
              simulate_leader(server_io, {"queue1/messages.dat" => content})
              done.send nil
            end
            client.sync_files_public(client_io, lz4_reader)
            select
            when done.receive
            when timeout(1.second)
              fail "leader fiber timed out"
            end
          end

          checksums_file = File.join(data_dir, "checksums.sha1")
          File.exists?(checksums_file).should be_true
          File.read(checksums_file).should contain "queue1/messages.dat"
        end
      end
    end

    # A digest that didn't start at byte 0 of the file covers only the bytes it
    # saw, so persisting it as that file's checksum makes the next sync mismatch
    # and re-fetch a file the follower already has.
    describe "checksums persisted on close" do
      it "persists no checksum for a file appended to after sync" do
        with_datadir do |data_dir|
          filename = "queue1/msgs.0000000001"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, filename), "hello"

          client = make_client(data_dir)
          # Sync first, so the file's pre-append hash is known and persisted.
          sync_with_leader(client, {filename => "hello"})

          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          payload = " world"
          write_record(lz4_writer, filename, -payload.bytesize.to_i64, payload.to_slice)
          read_acks(leader_io, record_size(filename, payload.bytesize))

          client_socket.close
          close_client(client)

          File.read(File.join(data_dir, filename)).should eq "hello world"
          # Dropped when the append arrived, so the next sync re-hashes the file
          # locally instead of trusting the hash of just the appended bytes.
          checksums_matching_disk(data_dir).has_key?(filename).should be_false
        end
      end

      it "persists the checksum of a file it created by appending" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          # The file doesn't exist locally, so the digest sees all of it — over
          # both records.
          filename = "queue1/msgs.0000000001"
          {"abc", "def"}.each do |payload|
            write_record(lz4_writer, filename, -payload.bytesize.to_i64, payload.to_slice)
            read_acks(leader_io, record_size(filename, payload.bytesize))
          end

          client_socket.close
          close_client(client)

          File.read(File.join(data_dir, filename)).should eq "abcdef"
          checksums_matching_disk(data_dir)[filename].should eq Digest::SHA1.digest("abcdef").hexstring
        end
      end

      it "persists the checksum of a replaced file" do
        with_datadir do |data_dir|
          filename = "definitions.amqp"
          File.write File.join(data_dir, filename), "old content"

          client = make_client(data_dir)
          sync_with_leader(client, {filename => "old content"})

          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          content = "brand new content"
          write_record(lz4_writer, filename, content.bytesize.to_i64, content.to_slice)
          read_acks(leader_io, record_size(filename, content.bytesize))

          client_socket.close
          close_client(client)

          checksums_matching_disk(data_dir)[filename].should eq Digest::SHA1.digest(content).hexstring
        end
      end

      # The digest of an aborted replace covers content that was never installed,
      # so it must not become the file's checksum; the old file is still on disk
      # and keeps its own (matching) hash.
      it "keeps the old checksum when a replace never completes" do
        with_datadir do |data_dir|
          filename = "definitions.amqp"
          File.write File.join(data_dir, filename), "old content"

          client = make_client(data_dir)
          sync_with_leader(client, {filename => "old content"})

          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          stream_done = Channel(Nil).new(1)
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          ensure
            stream_done.send nil
          end

          # Announce twice the bytes we send, then hang up: the replace writes a
          # partial .tmp file and raises before renaming it into place.
          content = "new content"
          write_record(lz4_writer, filename, (content.bytesize * 2).to_i64, content.to_slice)
          leader_io.close

          select
          when stream_done.receive
          when timeout(2.seconds)
            fail "stream fiber did not exit"
          end

          close_client(client)

          File.read(File.join(data_dir, filename)).should eq "old content"
          checksums_matching_disk(data_dir)[filename].should eq Digest::SHA1.digest("old content").hexstring
        end
      end

      # checksums.sha1 is carried across restarts wholesale (restore, then store
      # at shutdown), so a line for a file that's been deleted would never leave
      # it again.
      it "drops the checksum of a file the sync deleted" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          filename = "gone_from_leader"
          payload = "data"
          write_record(lz4_writer, filename, -payload.bytesize.to_i64, payload.to_slice)
          read_acks(leader_io, record_size(filename, payload.bytesize))
          client_socket.close

          # Reconnect to a leader that no longer has the file: the sync sweeps it.
          sync_with_leader(client, {} of String => String, resync: true)
          File.exists?(File.join(data_dir, filename)).should be_false

          close_client(client)
          checksums_matching_disk(data_dir).has_key?(filename).should be_false
        end
      end
    end

    # The open append handles and running digests belong to one connection: a
    # sync deletes and re-fetches local files that don't match the leader, so
    # anything held over from the previous connection can describe an inode
    # that's no longer at that path.
    describe "state carried across a re-sync" do
      it "appends to the file the re-sync installed, not the replaced inode" do
        with_datadir do |data_dir|
          filename = "queue1/msgs.0000000001"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, filename), "old"

          client = make_client(data_dir)
          sync_with_leader(client, {filename => "old"})

          # Stream an append, which leaves an open handle on the current inode.
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end
          write_record(lz4_writer, filename, -1i64, "A".to_slice)
          read_acks(leader_io, record_size(filename, 1))
          File.read(File.join(data_dir, filename)).should eq "oldA"
          client_socket.close

          # Reconnect to a leader whose copy differs: the sync unlinks our file
          # and re-fetches it, so the path now points at a new inode.
          sync_with_leader(client, {filename => "new"}, resync: true)
          File.read(File.join(data_dir, filename)).should eq "new"

          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "client stream_changes after resync") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end
          write_record(lz4_writer, filename, -1i64, "B".to_slice)
          read_acks(leader_io, record_size(filename, 1))

          # A stale handle would have sent this into the unlinked inode, where
          # it's invisible — and acked it as durable.
          File.read(File.join(data_dir, filename)).should eq "newB"

          client_socket.close
          close_client(client)
          checksums_matching_disk(data_dir)
        end
      end
    end

    describe "#close" do
      # Regression: close used to wait only for the follow loop, then close
      # the data dir fd while the ack-sending fiber could still be draining
      # buffered acks — each preceded by a syncfs on that fd. The resulting
      # EBADF made the follower Log.fatal and exit 1 in the middle of a
      # graceful shutdown or a promotion to leader.
      it "waits for the ack loop's pending syncs before closing the data dir fd" do
        with_datadir do |data_dir|
          client = make_client(data_dir)
          client.sync_delay = 50.milliseconds
          client_socket, leader_io = FakeSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4_writer = Compress::LZ4::Writer.new(leader_io,
            Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))

          spawn(name: "client stream_changes") do
            client.stream_changes_public(client_socket, lz4_reader)
          rescue IO::Error
          end

          # Stream a small append so acks start flowing and the ack loop
          # enters its (slowed) sync.
          filename = "ack_file"
          payload = "data"
          lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
          lz4_writer.write filename.to_slice
          lz4_writer.write_bytes -payload.bytesize.to_i64, IO::ByteFormat::LittleEndian
          lz4_writer.write payload.to_slice
          lz4_writer.flush
          wait_for { client.syncs_started > 0 }

          # Keep acks arriving while close runs, so a sync is in flight or
          # pending throughout the shutdown.
          spawn(name: "ack feeder") do
            20.times do
              client.@acks.send(1i64)
              sleep 10.milliseconds
            end
          rescue Channel::ClosedError
            # close drained and closed the channel
          end

          # follow() was never called in this harness, so satisfy close's
          # follower-done handshake ourselves.
          spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
          client.close

          sleep 200.milliseconds # let any straggler sync run after close returned
          client.synced_on_closed_fd?.should be_false
          client_socket.close
          leader_io.close
        end
      end
    end
  end
end
