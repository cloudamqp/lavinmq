require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Clustering
    Start   = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
    StartV2 = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]

    enum ConnectionKind : UInt8
      Replication = 0
      Control     = 1
    end

    enum ControlRequest : UInt8
      Vote      = 1
      Authority = 2
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
