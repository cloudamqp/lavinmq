require "log"
require "socket"
require "digest/sha1"

module LavinMQ
  module Replication
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 1, 0, 0]
  end
end
