require "../spec_helper"
require "lz4"

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
      yield nil
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

    delegate read, write, flush, to: @io
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
          client_socket.flush

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
          client_socket.flush

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
          client_socket.flush

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
          buf = uninitialized UInt8[1024]
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
        client_socket.flush

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
end
