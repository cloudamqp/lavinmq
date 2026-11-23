require "../../connection_info"

module LavinMQ
  module Auth
    # SASL authentication mechanisms. Each mechanism derives a
    # `{username, password}` pair from the client's SASL response and/or the
    # details of the (TLS) connection.
    module Mechanisms
      # Raised when the credentials can't be derived, e.g. a malformed response
      # or no verified client certificate for EXTERNAL. The client is refused.
      class Error < Exception; end

      abstract class Mechanism
        abstract def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
      end
    end
  end
end
