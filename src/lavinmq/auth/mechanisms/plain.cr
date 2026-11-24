require "./mechanism"

module LavinMQ
  module Auth
    module Mechanisms
      class Plain < Mechanism
        NUL = '\u{0}'

        def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
          if i = response.index(NUL, 1)
            {response[1...i], response[(i + 1)..-1]}
          else
            raise Error.new("Invalid authentication response")
          end
        end
      end
    end
  end
end
