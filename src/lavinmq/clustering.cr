require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    # Version byte bumped to 2: each replicated record is now prefixed with its
    # op-number (UInt64) and acks carry that op alongside the byte delta, so a v2
    # follower/leader cannot interoperate with a v1 one.
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]

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
