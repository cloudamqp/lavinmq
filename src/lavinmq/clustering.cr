require "log"
require "socket"
require "digest/sha1"
require "openssl/hmac"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    # Version 2 followers don't sync to disk before every ack; they fsync the
    # files named by fsync request records (a `$` prefixed path and a zero
    # length), and syncfs on a bare `$`. Like version 1 they send the
    # password, as 2.11.0-rc.1 does.
    StartV2 = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]
    # Version 3 is version 2, but instead of sending the password the
    # follower answers a challenge from the leader, see `challenge_response`.
    StartV3        = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 3, 0, 0]
    FSYNC_PREFIX   = '$'
    CHALLENGE_SIZE = 32

    # This node's own files in the data dir, never replicated nor restored:
    # the data dir lock, the clustering identity and the raft state. Losing
    # or rolling back .raft_state could let the node vote twice in a term.
    NODE_LOCAL_FILES = {".lock", ".clustering_id", ".raft_state", ".raft_state.tmp"}

    # Whether the data dir has broker data, i.e. anything but this node's own
    # files and the replication checksums
    def self.broker_data?(data_dir : String) : Bool
      return false unless Dir.exists?(data_dir)
      Dir.each_child(data_dir) do |child|
        next if child.in?(NODE_LOCAL_FILES) || child.starts_with?("checksums.sha1")
        return true
      end
      false
    end

    # The header is part of the MAC so a response can't be replayed against
    # the raft port, which uses the same password but MACs its challenge bare.
    def self.challenge_response(password : String, challenge : Bytes) : Bytes
      OpenSSL::HMAC.digest(:sha256, password, StartV3 + challenge)
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
