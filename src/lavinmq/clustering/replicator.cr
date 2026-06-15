require "../mfile"

module LavinMQ
  module Clustering
    module Replicator
      abstract def register_file(path : String)
      abstract def register_file(file : File)
      abstract def register_file(mfile : MFile)
      abstract def replace_file(path : String) # only non mfiles are ever replaced
      abstract def append(path : String, pos : Int, length : Int)
      # `offset` is the absolute byte position the value/bytes are written at on
      # the leader; used to skip appends a just-joined follower already received
      # via full_sync (see Server#append). Distinct names avoid colliding with
      # the positional append(path, pos, length) overload.
      abstract def append_value(path : String, value : UInt32 | Int32, offset : Int64)
      abstract def append_bytes(path : String, bytes : Bytes, offset : Int64)
      abstract def delete_file(path : String)
      abstract def followers : Array(Follower)
      abstract def syncing_followers : Array(Follower)
      # Block until a quorum (this leader + a majority of the roster) has acked
      # everything replicated so far. Called after a durable operation has been
      # dispatched and locally fsynced, before it is acknowledged to a client
      # (publish confirms via the Persister, definition changes via the
      # DefinitionsStore). A minority that cannot form a quorum stalls here.
      abstract def wait_for_followers : Nil
      abstract def all_followers : Array(Follower)
      abstract def close
      abstract def listen(server : TCPServer)
      abstract def clear
      abstract def password : String
    end
  end
end
