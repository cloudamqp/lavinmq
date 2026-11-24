require "../../connection_info"

module LavinMQ
  module Auth
    module Mechanisms
      class Error < Exception; end

      abstract class Mechanism
        abstract def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
      end
    end
  end
end
