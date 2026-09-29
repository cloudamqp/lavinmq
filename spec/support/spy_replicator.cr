class SpyReplicator
  include LavinMQ::Clustering::Replicator

  getter registered_files = Hash(String, Symbol).new
  getter deleted_files = Set(String).new
  getter replaced_files = Array(String).new

  def register_file(path : String)
    @registered_files[path] = :path
  end

  def register_file(file : File)
    @registered_files[file.path] = :file
  end

  def register_file(mfile : MFile)
    @registered_files[mfile.path] = :mfile
  end

  def replace_file(path : String)
    @replaced_files << path
  end

  def replace_file(mfile : MFile)
    @replaced_files << mfile.path
  end

  def append(path : String, pos : Int, length : Int)
  end

  def append_value(path : String, value : UInt32 | Int32, offset : Int64)
  end

  def append_bytes(path : String, bytes : Bytes, offset : Int64)
  end

  def delete_file(path : String)
    @deleted_files << path
  end

  def fsync_files(paths : Array(String))
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
