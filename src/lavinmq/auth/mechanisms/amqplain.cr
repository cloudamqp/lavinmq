require "amq-protocol"
require "./mechanism"

module LavinMQ
  module Auth
    module Mechanisms
      # SASL AMQPLAIN: the response is an AMQP field table carrying LOGIN and
      # PASSWORD entries.
      class AMQPlain < Mechanism
        def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
          io = ::IO::Memory.new(response)
          tbl = ::AMQ::Protocol::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.bytesize.to_u32)
          user = tbl["LOGIN"]?.as(String?) || ""
          password = tbl["PASSWORD"]?.as(String?) || ""
          {user, password}
        end
      end
    end
  end
end
