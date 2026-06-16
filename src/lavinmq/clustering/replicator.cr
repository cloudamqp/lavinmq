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
      # Compute, cache and persist the checksum of a segment that has just been
      # finalized (rolled away from) and is now immutable — see
      # MessageStore#open_new_segment. Lets a joining follower find the hash
      # precomputed and an unclean restart skip re-hashing it.
      abstract def checksum_file(mfile : MFile)
      abstract def followers : Array(Follower)
      abstract def syncing_followers : Array(Follower)
      # ISR bookkeeping for the publish-confirm path: a confirm may only be
      # sent against an ISR that is committed to the coordinator (see
      # Persister#wait_for_followers).
      abstract def isr_dirty? : Bool
      abstract def flush_isr : Nil
      # Block until every in-sync follower has acked everything replicated so
      # far, then commit any pending ISR change. Called after a durable
      # operation has been dispatched and locally fsynced, before it is
      # acknowledged to a client (publish confirms via the Persister,
      # definition changes via the DefinitionsStore).
      abstract def wait_for_followers : Nil
      abstract def all_followers : Array(Follower)
      abstract def close
      abstract def listen(server : TCPServer)
      abstract def clear
      abstract def password : String
    end
  end
end
