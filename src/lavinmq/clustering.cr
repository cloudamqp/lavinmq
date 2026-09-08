require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    # Version 2 requires explicit durability fences. Version 1 peers treat
    # ordinary byte acknowledgments as durable and must not join this protocol.
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
