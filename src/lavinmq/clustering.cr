require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    # Version 2 followers don't sync to disk before every ack; they fsync the
    # files named by fsync request records (a `$` prefixed path and a zero
    # length), and syncfs on a bare `$`.
    StartV2 = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]
    # Version 3 records carry a log sequence number and the offset of every
    # append, so the follower applies them idempotently, and acks carry the
    # applied lsn (see WireV3).
    StartV3          = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 3, 0, 0]
    PROTOCOL_VERSION = 3
    FSYNC_PREFIX     = '$'

    # Byte 5 of a start header, 0 if it isn't one
    def self.header_version(header : Bytes) : Int32
      return 0 unless header.size == 8 && header[0, 5] == Start[0, 5]
      header[5].to_i32
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
