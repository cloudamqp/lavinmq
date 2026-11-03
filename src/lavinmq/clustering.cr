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
    # Version 3 is version 2, but instead of sending the password both sides
    # prove they know it, see `challenge_response`: the leader sends a nonce,
    # the follower its own nonce and its MAC over both, and the leader
    # accepts with its MAC over both, which the follower checks before it
    # sends its id.
    StartV3        = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 3, 0, 0]
    FSYNC_PREFIX   = '$'
    CHALLENGE_SIZE = 32
    RESPONSE_SIZE  = 32 # HMAC-SHA256

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

    enum Role
      Leader
      Follower
    end

    # What *role* answers in the version 3 handshake, a MAC over both sides'
    # nonces. The role is part of it so one side's answer can't be replayed
    # as the other's, and the header so neither can be replayed against the
    # raft port, which uses the same password.
    def self.challenge_response(password : String, role : Role, leader_nonce : Bytes,
                                follower_nonce : Bytes) : Bytes
      signed = IO::Memory.new
      signed.write StartV3
      signed.write role.to_s.downcase.to_slice
      signed.write leader_nonce
      signed.write follower_nonce
      OpenSSL::HMAC.digest(:sha256, password, signed.to_slice)
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
