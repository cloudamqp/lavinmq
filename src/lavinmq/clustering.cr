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

    enum Role
      Leader
      Follower
    end

    # What *role* answers in the version 3 handshake, a MAC over both sides'
    # nonces. The role is part of it so one side's answer can't be replayed
    # as the other's.
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
