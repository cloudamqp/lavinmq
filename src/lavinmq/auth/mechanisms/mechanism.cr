require "../../connection_info"

module LavinMQ
  module Auth
    # SASL authentication mechanisms. Each mechanism derives a
    # `{username, password}` pair from the client's SASL response and/or the
    # details of the (TLS) connection.
    module Mechanisms
      abstract class Mechanism
        abstract def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
      end
    end
  end
end
