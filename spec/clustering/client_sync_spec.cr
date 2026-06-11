require "../spec_helper"
require "lz4"

module ClientSyncSpec
  class TestClient < LavinMQ::Clustering::Client
    def sync_files_public(socket, lz4)
      sync_files(socket, lz4)
    end

    def stream_changes_public(socket, lz4)
      stream_changes(socket, lz4)
    end
  end

  def self.make_client(data_dir : String) : TestClient
    config = LavinMQ::Config.instance.dup
    config.data_dir = data_dir
    config.metrics_http_port = -1
    TestClient.new(config, 1, "password", proxy: false)
  end

  def self.simulate_leader(io : IO, leader_files : Hash(String, String))
    lz4 = Compress::LZ4::Writer.new(io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
    leader_files.each do |filename, content|
      hash = Digest::SHA1.digest(content)
      lz4.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
      lz4.write filename.to_slice
      lz4.write hash
    end
    lz4.write_bytes 0i32, IO::ByteFormat::LittleEndian
    lz4.flush

    requested = Array(String).new
    loop do
      len = io.read_bytes Int32, IO::ByteFormat::LittleEndian
      break if len == 0
      filename = io.read_string(len)
      requested << filename
    end

    requested.each do |filename|
      content = leader_files[filename]? || ""
      lz4.write_bytes content.bytesize.to_i64, IO::ByteFormat::LittleEndian
      lz4.write content.to_slice
      lz4.flush
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
  end
end
