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

  def files_with_hash(caps : Hash(String, Int64)? = nil, & : Tuple(String, Bytes) -> _)
    @files_with_hash.each do |values|
      yield values
    end
  end

  def with_file(filename : String, cap : Int64? = nil, &)
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

  # ack_loop sets read_timeout to drive its flush/ack-deadline cadence; route it
  # to the backing UNIXSocket so reads actually time out in specs.
  def read_timeout=(timeout)
    @io.read_timeout = timeout
  end

  # Follower sets write_timeout so flushes to a blocked follower give up;
  # route it to the backing UNIXSocket so blocked writes time out in specs.
  def write_timeout=(timeout)
    @io.write_timeout = timeout
  end

  # Expose the backing socket's write_timeout so specs can assert the follower
  # relaxes it during full_sync and tightens it for the streaming phase.
  def write_timeout
    @io.write_timeout
  end

  def remote_address : Socket::IPAddress
    Socket::IPAddress.parse("tcp://127.0.0.1:1234")
  end
end
