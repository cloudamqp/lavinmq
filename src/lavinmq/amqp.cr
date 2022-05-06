require "amq-protocol"

module LavinMQ
  alias AMQP = AMQ::Protocol
  alias ArgumentNumber = Int8 | Int16 | UInt16 | Int32 | Int64
end
