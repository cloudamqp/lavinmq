require "../mfile"
require "./control_packet"
require "wait_group"

module LavinMQ
  module Clustering
    module Replicator
      abstract def register_file(path : String)
      abstract def register_file(file : File)
      abstract def register_file(mfile : MFile)
      abstract def replace_file(path : String) # regular files, re-read from disk
      abstract def replace_file(mfile : MFile) # mmap-backed files, read from the mmap (capped at mfile.size)
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
      # ISR bookkeeping for the publish-confirm path: a confirm may only be
      # sent against an ISR that is committed to the coordinator (see
      # Persister#wait_for_followers).
      abstract def isr_dirty? : Bool
      abstract def flush_isr : Nil
      # Push the bytes replicated so far to every in-sync follower. Never blocks.
      abstract def request_flush : Nil
      # Ask every in-sync follower to make everything replicated so far durable.
      # Paired with #wait_for_followers: a follower acks received bytes, so only
      # the ack of this request means they're persisted. Called once the
      # operation is dispatched and locally fsynced, before the wait, so the
      # followers persist while the leader does.
      #
      # `wg` is released per follower once the record is on its socket. Wait for
      # it before #wait_for_followers: unless the record is counted in the
      # follower's sent-byte total first, an append written ahead of it can
      # carry that follower's acks past the target on its own.
      abstract def request_sync(wg : WaitGroup) : Nil
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
