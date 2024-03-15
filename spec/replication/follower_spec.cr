require "../spec_helper"
require "lz4"

module FollowerSpec
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

    DEFAULT_FILES_WITH_HASH = {
      "#{LavinMQ::Config.instance.data_dir}/file1" => FollowerSpec.sha1("hash1"),
      "#{LavinMQ::Config.instance.data_dir}/file2" => FollowerSpec.sha1("hash2"),
      "#{LavinMQ::Config.instance.data_dir}/file3" => FollowerSpec.sha1("hash3"),
    } of String => Bytes

    alias FileType = MFile | File

    DEFAULT_WITH_FILE = {} of String => FileType

    def initialize(@files_with_hash : Hash(String, Bytes) = DEFAULT_FILES_WITH_HASH,
                   @with_file : Hash(String, FileType) = DEFAULT_WITH_FILE)
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
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new
        follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)

        invalid_start = Bytes[0, 1, 2, 3, 4, 5, 6, 7]
        client_socket.write invalid_start

        expect_raises(LavinMQ::Replication::InvalidStartHeaderError) do
          follower.negotiate!("foo")
        end
      end

      it "should raise AuthenticationError and send 1 on wrong password" do
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new
        follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)

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

      it "should send 0 on succesful negotiation" do
        follower_socket, client_socket = FakeSocket.pair
        file_index = FakeFileIndex.new
        follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)

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

  describe "#full_sync" do
    it "should send file list" do
      follower_socket, client_socket = FakeSocket.pair
      client_lz4 = Compress::LZ4::Reader.new(client_socket)
      file_index = FakeFileIndex.new
      follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)

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
        "#{LavinMQ::Config.instance.data_dir}/#{k}"
      end

      file_list.should eq file_index.@files_with_hash
    end
  end
end
