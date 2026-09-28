require "amqp10-protocol"

module LavinMQ
  # AMQP 1.0 support. The wire protocol (types, framing, performatives) lives
  # in the amqp10-protocol shard and is included here, the way LavinMQ::AMQP
  # includes AMQ::Protocol; this namespace adds the broker side on top.
  module AMQP10
    include ::AMQP10::Protocol
  end
end
