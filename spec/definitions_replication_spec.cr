require "./spec_helper"

# Verifies, at the moment a definition frame is dispatched to followers, that
# the bytes are already readable from disk through a separate fd at the
# dispatched offset. A joining follower's cut and full_sync read the file that
# way (Clustering::Server#snapshot_sizes / #files_with_hash), while the change
# stream skips everything below the cut — so a frame sitting in a user-space
# write buffer at dispatch time would be permanently missing on a follower
# that joins in that window, even though it gets marked synced.
class DiskVisibilitySpyReplicator
  include LavinMQ::Clustering::Replicator

  getter violations = Array(String).new
  getter dispatched_definitions = 0

  def append_bytes(path : String, bytes : Bytes, offset : Int64)
    @dispatched_definitions += 1 if path.ends_with?("definitions.amqp")
    File.open(path) do |f|
      if f.size < offset + bytes.bytesize
        @violations << "#{path}: dispatched [#{offset}, #{offset + bytes.bytesize}) but only #{f.size} bytes are on disk"
        return
      end
      f.pos = offset
      buf = Bytes.new(bytes.bytesize)
      f.read_fully(buf)
      @violations << "#{path}: on-disk bytes at #{offset} differ from the dispatched frame" unless buf == bytes
    end
  rescue File::NotFoundError
    @violations << "#{path}: dispatched but the file doesn't exist on disk"
  end

  # The rest of the interface is irrelevant to this spec.
  def register_file(path : String)
  end

  def register_file(file : File)
  end

  def register_file(mfile : MFile)
  end

  def replace_file(path : String)
  end

  def append(path : String, pos : Int, length : Int)
  end

  def append_value(path : String, value : UInt32 | Int32, offset : Int64)
  end

  def delete_file(path : String)
  end

  def followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def syncing_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def all_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def isr_dirty? : Bool
    false
  end

  def flush_isr : Nil
  end

  def wait_for_followers : Nil
  end

  def close
  end

  def listen(server : TCPServer)
  end

  def clear
  end

  def password : String
    ""
  end
end

describe LavinMQ::DefinitionsStore do
  # Regression: the definitions file was write-buffered, so dispatched frames
  # (and the fstat-based offsets they carried) could be invisible on disk
  # until the next flush/fsync — invisible to a concurrent join cut too.
  it "has definition frames on disk before they are dispatched to followers" do
    spy = DiskVisibilitySpyReplicator.new
    with_amqp_server(replicator: spy) do |s|
      with_channel(s) do |ch|
        q = ch.queue("disk_visibility_q", durable: true)
        x = ch.exchange("disk_visibility_x", "topic", durable: true)
        q.bind(x.name, "rk")
      end
    end
    spy.dispatched_definitions.should be >= 3 # queue + exchange + binding
    spy.violations.should be_empty
  end
end
