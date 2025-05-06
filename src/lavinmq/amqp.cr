require "amq-protocol"

module LavinMQ
  module AMQP
    include AMQ::Protocol

    class Error < Exception; end
  end
end
