require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]

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

    class FollowerTooSlowError < Error
      def initialize(follower)
        super("Follower too slow, action channel full address=#{follower.remote_address} id=#{follower.id.to_s(36)}")
      end
    end
  end
end
