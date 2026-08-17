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
      # `offset` is where the value/bytes are written on the leader, so a
      # just-joined follower can skip what full_sync already gave it.
      abstract def append_value(path : String, value : UInt32 | Int32, offset : Int64)
      abstract def append_bytes(path : String, bytes : Bytes, offset : Int64)
      abstract def delete_file(path : String)
      abstract def followers : Array(Follower)
      abstract def syncing_followers : Array(Follower)
      # True when the ISR last committed to the coordinator may be stale.
      abstract def isr_dirty? : Bool
      # Commit the current ISR to the coordinator.
      abstract def flush_isr : Nil
      # Push the bytes replicated so far to every in-sync follower.
      abstract def request_flush : Nil
      # Ask every in-sync follower to make everything replicated so far durable.
      # `wg` is released per follower once the request is on its socket; wait for
      # it before #wait_for_followers.
      abstract def request_sync(wg : WaitGroup) : Nil
      # Block until every in-sync follower has acked everything replicated so
      # far, then commit any pending ISR change.
      abstract def wait_for_followers : Nil
      abstract def all_followers : Array(Follower)
      abstract def close
      abstract def listen(server : TCPServer)
      abstract def clear
      abstract def password : String
    end
  end
end
