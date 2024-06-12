require "../spec_helper"
require "lz4"

module FollowerSpec
  def self.with_datadir(&)
    data_dir = File.tempname("lavinmq", "spec")
    Dir.mkdir_p data_dir
    yield data_dir
  ensure
    FileUtils.rm_rf data_dir if data_dir
  end

  def self.sha1(str : String)
    sha1 = Digest::SHA1.new
    hash = Bytes.new(sha1.digest_size)
    sha1.update str.to_slice
    sha1.final hash
    sha1.reset
    hash
  end

  class FakeFileIndex
    include LavinMQ::Replication::FileIndex

    alias FileType = MFile | File

    DEFAULT_WITH_FILE = {} of String => FileType

    @files_with_hash : Hash(String, Bytes)

    def initialize(data_dir : String, files_with_hash : Hash(String, Bytes)? = nil,
                   @with_file : Hash(String, FileType) = DEFAULT_WITH_FILE)
      @files_with_hash = files_with_hash || Hash(String, Bytes){
        File.join(data_dir, "file1") => FollowerSpec.sha1("hash1"),
        File.join(data_dir, "file2") => FollowerSpec.sha1("hash2"),
        File.join(data_dir, "file3") => FollowerSpec.sha1("hash3"),
      }
    end

    def files_with_hash(& : Tuple(String, Bytes) -> Nil)
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

    def remote_address
      Address.parse("tcp://127.0.0.1:1234")
    end
  end

  describe LavinMQ::Replication::Follower do
    describe "#negotiate!" do
      it "should raise InvalidStartHeaderError on invalid start header" do
        FollowerSpec.with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)

          invalid_start = Bytes[0, 1, 2, 3, 4, 5, 6, 7]
          client_socket.write invalid_start

          expect_raises(LavinMQ::Replication::InvalidStartHeaderError) do
            follower.negotiate!("foo")
          end
        end
      end

      it "should raise AuthenticationError and send 1 on wrong password" do
        FollowerSpec.with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Replication::Follower.new(follower_socket, "/tmp", file_index)

          password = "foo"
          client_socket.write LavinMQ::Replication::Start
          client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          client_socket.write password.to_slice

          expect_raises(LavinMQ::Replication::AuthenticationError) do
            follower.negotiate!("bar")
          end

          response = client_socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
          response.should eq 1u8
        end
      end

      it "should send 0 on succesful negotiation" do
        FollowerSpec.with_datadir do |data_dir|
          follower_socket, client_socket = FakeSocket.pair
          file_index = FakeFileIndex.new(data_dir)
          follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)

          password = "foo"
          client_socket.write LavinMQ::Replication::Start
          client_socket.write_bytes password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          client_socket.write password.to_slice

          follower.negotiate!("foo")

          response = client_socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
          response.should eq 0u8
        end
      end
    end
  end

  describe "#full_sync" do
    it "should send file list" do
      FollowerSpec.with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        client_lz4 = Compress::LZ4::Reader.new(client_socket)
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)

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

        file_list = file_list.transform_keys do |k|
          File.join data_dir, k
        end

        file_list.should eq file_index.@files_with_hash
      end
    end
  end

  describe "#close" do
    it "should let followers sync" do
      FollowerSpec.with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)
        client_lz4 = Compress::LZ4::Reader.new(client_socket)

        spawn { follower.read_acks }

        file = File.join data_dir, "file1"
        File.write file, "foo"
        follower.add file

        ack_size = 0i64
        filename_size = client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        ack_size += filename_size + sizeof(Int32)
        client_lz4.skip filename_size
        data_size = client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        client_lz4.skip data_size
        ack_size += data_size + sizeof(Int64)
        let_sync = Channel({LavinMQ::Replication::Follower, Bool}).new

        spawn { follower.close(let_sync) }
        spawn { client_socket.write_bytes ack_size, IO::ByteFormat::LittleEndian }

        select
        when res = let_sync.receive
          follower, in_sync = res
          in_sync.should eq true
        when timeout(1.second)
          fail "timeout close"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end

    it "should close even when sync fails" do
      FollowerSpec.with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)
        client_lz4 = Compress::LZ4::Reader.new(client_socket)

        spawn { follower.read_acks }

        # Add a file to the follower to create some lag
        file = File.join data_dir, "file1"
        File.write file, "foo"
        follower.add file

        # Read and ack the to make follower in sync
        filename_size = client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        client_lz4.skip filename_size
        data_size = client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        client_lz4.skip data_size
        let_sync = Channel({LavinMQ::Replication::Follower, Bool}).new

        spawn do
          follower.close(let_sync)
        end
        spawn { follower_socket.close }

        select
        when res = let_sync.receive
          follower, in_sync = res
          in_sync.should eq false
        when timeout(1.second)
          fail "timeout close"
        end
      ensure
        follower_socket.try &.close
        client_socket.try &.close
      end
    end
  end

  describe "#lag" do
    it "should count bytes added to action queue" do
      FollowerSpec.with_datadir do |data_dir|
        follower_socket, _client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)
        filename = File.join data_dir, "file1"
        size = follower.append filename, Bytes.new(10)
        follower.lag.should eq size
      end
    end

    it "should subtract acked bytes from lag" do
      FollowerSpec.with_datadir do |data_dir|
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new(data_dir)
        follower = LavinMQ::Replication::Follower.new(follower_socket, data_dir, file_index)
        filename = File.join data_dir, "file1"
        size = follower.append filename, Bytes.new(10)
        size2 = follower.append filename, Bytes.new(20)
        # send ack for first message
        client_socket.write_bytes size.to_i64, IO::ByteFormat::LittleEndian
        follower.read_ack
        follower.lag.should eq size2
      end
    end
  end
end
