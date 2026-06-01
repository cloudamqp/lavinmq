require "socket"
require "digest/sha1"
require "../../src/lavinmq/clustering/file_index"

# Shared test doubles for clustering follower specs.

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

# A TCPSocket whose IO is backed by a UNIXSocket pair, so specs can drive both
# ends in-process without real networking.
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
    Socket::IPAddress.parse("tcp://127.0.0.1:1234")
  end
end
