require "amq-protocol"
require "./amqp/table_ext"

module LavinMQ
  module AMQP
    include AMQ::Protocol

    class Error < Exception; end
  end
end
