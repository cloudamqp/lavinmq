require "../spec_helper"

module FollowerSpec
  #  class FakeSocket < TCPSocket
  #  end

  class FakeFileIndex
    include LavinMQ::Replication::FileIndex

    DEFAULT_FILES_WITH_HASH = {
      "file1" => "hash1".to_slice,
      "file2" => "hash2".to_slice,
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

    def read(buffer : Bytes)
      @io.read(buffer)
    end

    def write(buffer : Bytes)
      @io.write(buffer)
    end

    def remote_address
      Address.parse("tcp://127.0.0.1:1234")
    end

    {% for method in ["read_timeout", "keepalive",
                      "tcp_keepalive_idle", "tcp_keepalive_interval",
                      "tcp_keepalive_count"] %}
    def {{method.id}}=(value) end
    {% end %}
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
end
