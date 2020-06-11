require "amq-protocol"

module AvalancheMQ
  alias AMQP = AMQ::Protocol
  alias ArgumentNumber = UInt16 | Int32 | Int64
end
