require "../spec_helper"
require "lz4"

module FollowerSpec
  def self.checksum(str : String)
    algo = Digest::CRC32.new
    hash = Bytes.new(algo.digest_size)
    algo.update str.to_slice
    algo.final hash
    algo.reset
    hash
  end

  class FakeFileIndex
    include LavinMQ::Clustering::FileIndex

    alias FileType = MFile | File

    DEFAULT_WITH_FILE = {} of String => FileType

    @files_with_hash : Hash(String, Bytes)

    def initialize(data_dir : String, files_with_hash : Hash(String, Bytes)? = nil,
                   @with_file : Hash(String, FileType) = DEFAULT_WITH_FILE)
      @files_with_hash = files_with_hash || Hash(String, Bytes){
        File.join(data_dir, "file1") => FollowerSpec.checksum("hash1"),
        File.join(data_dir, "file2") => FollowerSpec.checksum("hash2"),
        File.join(data_dir, "file3") => FollowerSpec.checksum("hash3"),
      }
    end

    def files_with_hash(algo : Digest, & : Tuple(String, Bytes) -> Nil)
      @files_with_hash.each do |values|
        yield values
      end
    end

    def with_file(filename : String, &) : Nil
      yield @with_file[filename]?
    end
  end

  class FakeSocket < TCPSocket
    def self.pair
      left, right = UNIXSocket.pair
      {FakeSocket.new(left), right}
    end

    def initialize(@io : UNIXSocket)
      super(Family::INET, Type::STREAM, Protocol::TCP, false)
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
        end
      end

      it "should raise AuthenticationError and send 1 on wrong password" do
        with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Clustering::Follower.new(follower_socket, "/tmp", file_index)

          password = "foo"
          client_socket.write LavinMQ::Clustering::Start
          client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          client_socket.write password.to_slice

          expect_raises(LavinMQ::Clustering::AuthenticationError) do
            follower.negotiate!("bar")
          end

          response = client_socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
          response.should eq 1u8
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

          response = client_socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
          response.should eq 0u8
        end
      end

      it "can negotiate older protocol version" do
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
            hash = Bytes.new(4)
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

        file_list = file_list.transform_keys do |k|
          File.join data_dir, k
        end

        file_list.should eq file_index.@files_with_hash
      end
    end
  end

  describe "#stream changes" do
    it "should fully sync on graceful shutdown" do
      with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        lz4_writer = Compress::LZ4::Writer.new(follower_socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Clustering::Follower.new(follower_socket, data_dir, file_index)
        wg = WaitGroup.new(1)
        10.times do
          follower.append("#{data_dir}/file", "hello world".to_slice)
        end
        spawn do
          follower.action_loop lz4_writer
        end

        closed = false
        spawn do
          wg.done
          follower.close
          closed = true
          wg.done
        end

        wg.wait
        closed.should be_false
        wg.add(1)
        client_socket.write_bytes follower.lag_in_bytes.to_i64, IO::ByteFormat::LittleEndian
        client_socket.flush
        wg.wait
        follower.lag_in_bytes.should eq 0
        closed.should be_true
      end
    end
  end
end
