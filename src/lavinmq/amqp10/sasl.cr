require "./performatives"

module LavinMQ
  module AMQP10
    # SASL frames (amqp-core-security-v1.0 §5.3). LavinMQ offers PLAIN and
    # ANONYMOUS; the SASL layer precedes the AMQP layer on the connection.
    module Sasl
      # outcome codes (§5.3.3.1)
      OK         = 0_u8
      AUTH       = 1_u8 # authentication failed
      SYS        = 2_u8
      SYS_PERM   = 3_u8
      SYS_TEMP   = 4_u8
      MECHANISMS = ["PLAIN", "ANONYMOUS"]

      # sasl-mechanisms (server -> client)
      struct Mechanisms
        getter mechanisms : Array(String)

        def initialize(@mechanisms : Array(String) = MECHANISMS)
        end

        def to_io(io : IO) : Nil
          fl = FieldList.new
          # sasl-server-mechanisms is a symbol array; encode as a generic array.
          arr = @mechanisms.map { |m| Symbol.new(m).as(Codec::AnyValue) }
          fl.value arr
          Codec.write_described_list(io, Descriptor::SASL_MECHANISMS, fl.fields)
        end
      end

      # sasl-init (client -> server)
      struct Init
        getter mechanism : String
        getter initial_response : Bytes?
        getter hostname : String?

        def initialize(@mechanism : String, @initial_response : Bytes?, @hostname : String?)
        end

        def self.decode(d : Described) : Init
          f = FieldReader.from(d)
          Init.new(f.symbol? || "", f.bytes?, f.string?)
        end
      end

      # sasl-outcome (server -> client)
      struct Outcome
        getter code : UInt8

        def initialize(@code : UInt8)
        end

        def to_io(io : IO) : Nil
          fl = FieldList.new
          fl.ubyte @code
          Codec.write_described_list(io, Descriptor::SASL_OUTCOME, fl.fields)
        end
      end

      # Decode the credentials carried by a PLAIN initial-response:
      # [authzid] NUL authcid NUL passwd  (RFC 4616).
      def self.plain_credentials(response : Bytes) : {String, String}
        parts = String.new(response).split('\0')
        # parts: ["", user, pass] or [authzid, user, pass]
        raise Error.new("Malformed PLAIN response") if parts.size < 3
        {parts[1], parts[2]}
      end
    end
  end
end
