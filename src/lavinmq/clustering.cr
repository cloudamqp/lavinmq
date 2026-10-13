require "log"
require "socket"
require "digest/sha1"
require "openssl/hmac"

module LavinMQ
  module Clustering
    # Version 1 followers send the password in clear text, get appends
    # without offsets, ack cumulative byte counts and sync to disk before
    # every ack.
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    # Version 3 followers answer a challenge from the leader instead of
    # sending the password (see `challenge_response`), get records that carry
    # a log sequence number and the offset of every append, so they apply
    # them idempotently, ack the applied lsn and fsync only what the leader
    # asks for (see WireV3).
    StartV3          = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 3, 0, 0]
    PROTOCOL_VERSION =  3
    CHALLENGE_SIZE   = 32

    # This node's own files in the data dir, never replicated nor restored:
    # the data dir lock, the clustering identity, the raft state and the
    # replication position. Losing or rolling back .raft_state could let the
    # node vote twice in a term.
    NODE_LOCAL_FILES = {".lock", ".clustering_id", ".raft_state", ".raft_state.tmp",
                        ".replication_position", ".replication_position.tmp"}

    # Byte 5 of a start header, 0 if it isn't one
    def self.header_version(header : Bytes) : Int32
      return 0 unless header.size == 8 && header[0, 5] == Start[0, 5] && header[6] == 0 && header[7] == 0
      header[5].to_i32
    end

    # The start header the connection was opened with is part of the MAC so
    # a response can't be replayed against the raft port, which uses the
    # same password but MACs its challenge bare, nor across protocol versions.
    def self.challenge_response(password : String, challenge : Bytes, header : Bytes = StartV3) : Bytes
      OpenSSL::HMAC.digest(:sha256, password, header + challenge)
    end

    class Error < Exception; end

    # The peer broke the replication protocol. An IO::Error so a follower's
    # follow loop reconnects (and resyncs) instead of giving up.
    class ProtocolError < IO::Error; end

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
