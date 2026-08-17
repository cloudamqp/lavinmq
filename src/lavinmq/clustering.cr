require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]

    # Records with this prefix carry an instruction, not file data. Never a real
    # path, so nothing under it is created on disk or tracked.
    CONTROL_PREFIX = "$ctrl/"

    # Asks the follower to make everything replicated so far durable. Empty
    # body, hence routed by prefix before the length is interpreted.
    SYNC_CONTROL_PATH = "#{CONTROL_PREFIX}sync"

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
