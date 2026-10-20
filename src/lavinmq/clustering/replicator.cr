require "../mfile"

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
      abstract def delete_dir(path : String)
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

      # Waits up to *timeout* for the in-sync followers to ack what has been
      # replicated, without changing the ISR. For when nothing more is
      # acknowledged, like a closing broker, which mustn't wait on the
      # coordinator. False when they didn't in time.
      def wait_for_acks(timeout : Time::Span) : Bool
        true
      end

      # Ask every in-sync follower to fsync these (absolute) paths, or to sync
      # its whole data dir, before acking past what has been replicated so far.
      # Callers then wait_for_followers.
      abstract def request_fsync(paths : Enumerable(String)) : Nil
      abstract def request_syncfs : Nil
      abstract def all_followers : Array(Follower)
      abstract def close

      # Called as the broker starts to close. What it writes is still
      # replicated, e.g. to a leadership transfer target, but from then on a
      # write that finds leadership lost completes instead of raising
      # Coordinator::StaleLeadership halfway through the close. Only
      # #wait_for_followers acknowledges anything then, and it still raises.
      def broker_closing : Nil
      end

      # Set for good once nothing can be acknowledged as replicated anymore:
      # replication was closed, or the coordinator said this node isn't the
      # leader. Writes still complete locally (the broker may be closing), but
      # #wait_for_followers raises Coordinator::StaleLeadership from then on.
      def fenced? : Bool
        false
      end

      abstract def listen(server : TCPServer)
      abstract def clear
      abstract def password : String
    end
  end
end
