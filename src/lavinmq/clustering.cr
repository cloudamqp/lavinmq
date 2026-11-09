require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    # Version 2 followers don't sync to disk before every ack; they fsync the
    # files named by fsync request records (a `$` prefixed path and a zero
    # length), and syncfs on a bare `$`.
    StartV2      = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]
    FSYNC_PREFIX = '$'

    # This node's own files in the data dir, never replicated nor restored:
    # the data dir lock and the clustering identity
    NODE_LOCAL_FILES = {".lock", ".clustering_id"}

    # Entries of the data dir that aren't the broker's: the schema migration
    # backups, this node's own files (NODE_LOCAL_FILES), and sockets, like the
    # lavinmqctl one when it's configured in the data dir (a crashed process
    # leaves it behind, copying it fails and a live one mustn't be deleted).
    # Also an entry that's gone. Schema.migrate neither backs these up nor
    # restores them.
    def self.node_local?(data_dir : String, child : String) : Bool
      return true if child == "backups" || child.in?(NODE_LOCAL_FILES)
      type = File.info?(File.join(data_dir, child), follow_symlinks: false).try(&.type)
      type.nil? || type.socket? || type.pipe?
    end

    class Error < Exception; end

    class InvalidStartHeaderError < Error
      def initialize(bytes)
        super("Invalid start header: #{bytes} #{String.new(bytes)} ")
      end
    end

    class AuthenticationError < Error
      def initialize
        super("Authentication error")
      end
    end
  end
end
