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
