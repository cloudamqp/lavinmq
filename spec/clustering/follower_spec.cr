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
  class FakeFileIndex
    include LavinMQ::Clustering::FileIndex

    alias FileType = MFile | File

    def initialize(@data_dir : String)
      @files_with_hash = {
        "file1" => Digest::SHA1.digest("hash1"),
        "file2" => Digest::SHA1.digest("hash2"),
        "file3" => Digest::SHA1.digest("hash3"),
      }
    end

    def files_with_hash(& : Tuple(String, Bytes) -> _)
      @files_with_hash.each do |values|
        yield values
      end
    end

    def with_file(filename : String, &)
      yield nil, 0i64
    end

    def nr_of_files
      @files_with_hash.size
    end
  end

  class FakeSocket < TCPSocket
    def self.pair
      left, right = UNIXSocket.pair
      {FakeSocket.new(left), right}
    end

    def initialize(@io : UNIXSocket)
      super(Family::INET, Type::STREAM, Protocol::TCP)
    end

    delegate read, write, to: @io
    delegate close, closed?, to: @io

    def remote_address : Socket::IPAddress
      IPAddress.parse("tcp://127.0.0.1:1234")
    end
  end

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
