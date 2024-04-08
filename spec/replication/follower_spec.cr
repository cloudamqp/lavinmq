require "../spec_helper"
require "lz4"

module FollowerSpec
  def self.with_datadir_tempfile(filename, &)
    relative_path = Path.new filename
    absolute_path = Path.new(LavinMQ::Config.instance.data_dir).join relative_path
    yield relative_path.to_s, absolute_path.to_s
  ensure
    if absolute_path && File.exists? absolute_path
      FileUtils.rm absolute_path
    end
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

  describe "#close" do
    it "should let followers sync" do
      follower_socket, client_socket = FakeSocket.pair
      file_index = FakeFileIndex.new
      follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)
      client_lz4 = Compress::LZ4::Reader.new(client_socket)

      spawn { follower.read_acks }
      data_size = 0i64
      FollowerSpec.with_datadir_tempfile("file1") do |_rel_path, abs_path|
        File.write abs_path, "foo"
        follower.add(abs_path)
        filename_size = client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        client_lz4.skip filename_size
        data_size = client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        client_lz4.skip data_size
      end
      let_sync = Channel({LavinMQ::Replication::Follower, Bool}).new
      spawn { follower.close(let_sync) }
      spawn { client_socket.write_bytes data_size, IO::ByteFormat::LittleEndian }

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

    it "should close even when sync fails" do
      follower_socket, client_socket = FakeSocket.pair
      file_index = FakeFileIndex.new
      follower = LavinMQ::Replication::Follower.new(follower_socket, file_index)
      client_lz4 = Compress::LZ4::Reader.new(client_socket)

      spawn { follower.read_acks }
      data_size = 0i64
      FollowerSpec.with_datadir_tempfile("file1") do |_rel_path, abs_path|
        File.write abs_path, "foo"
        follower.add(abs_path)
        filename_size = client_lz4.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        client_lz4.skip filename_size
        data_size = client_lz4.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        client_lz4.skip data_size
      end
      let_sync = Channel({LavinMQ::Replication::Follower, Bool}).new
      spawn { follower.close(let_sync) }
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
