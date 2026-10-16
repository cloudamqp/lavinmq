require "log"
require "socket"
require "digest/sha1"
require "openssl/hmac"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    # Version 2 followers don't sync to disk before every ack; they fsync the
    # files named by fsync request records (a `$` prefixed path and a zero
    # length), and syncfs on a bare `$`. Instead of sending the password they
    # answer a challenge from the leader, see `challenge_response`.
    StartV2        = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]
    FSYNC_PREFIX   = '$'
    CHALLENGE_SIZE = 32

    # This node's own files in the data dir, never replicated nor restored:
    # the data dir lock, the clustering identity and the raft state. Losing
    # or rolling back .raft_state could let the node vote twice in a term.
    NODE_LOCAL_FILES = {".lock", ".clustering_id", ".raft_state", ".raft_state.tmp"}

    # The header is part of the MAC so a response can't be replayed against
    # the raft port, which uses the same password but MACs its challenge bare.
    def self.challenge_response(password : String, challenge : Bytes) : Bytes
      OpenSSL::HMAC.digest(:sha256, password, StartV2 + challenge)
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
