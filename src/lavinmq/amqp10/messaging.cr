require "amq-protocol"
require "./performatives"
require "./frames"

module LavinMQ
  module AMQP10
    # Helpers for building and classifying delivery states / outcomes
    # (amqp-core-messaging-v1.0 §3.4).
    module DeliveryState
      enum Outcome
        Accepted
        Rejected
        Released
        Modified
        Unknown
      end

      def self.accepted : Described
        Described.new(Descriptor::ACCEPTED, [] of Codec::AnyValue)
      end

      def self.released : Described
        Described.new(Descriptor::RELEASED, [] of Codec::AnyValue)
      end

      def self.rejected(condition : String? = nil, description : String? = nil) : Described
        error = if condition
                  Described.new(Descriptor::ERROR,
                    [Symbol.new(condition).as(Codec::AnyValue), description.as(Codec::AnyValue), nil] of Codec::AnyValue)
                end
        Described.new(Descriptor::REJECTED, [error.as(Codec::AnyValue)] of Codec::AnyValue)
      end

      def self.classify(state : Described?) : Outcome
        return Outcome::Unknown unless state
        case state.descriptor.as?(UInt64)
        when Descriptor::ACCEPTED then Outcome::Accepted
        when Descriptor::REJECTED then Outcome::Rejected
        when Descriptor::RELEASED then Outcome::Released
        when Descriptor::MODIFIED then Outcome::Modified
        else                           Outcome::Unknown
        end
      end
    end

    # Parses an AMQP 1.0 bare/annotated message (the `transfer` payload) into the
    # body bytes plus a best-effort mapping to AMQP 0-9-1 properties, and encodes
    # the reverse for delivery. This is the bridge that lets AMQP 1.0 and 0-9-1
    # clients share a queue (the same approach RabbitMQ takes).
    module MessageCodec
      record Parsed, properties : AMQ::Protocol::Properties, body : Bytes

      # Decode the message sections from a transfer payload, reading straight
      # from the byte slice with a cursor — no intermediate `IO::Memory` and no
      # `AnyValue` tree. The returned body is a slice *into* `payload` (no copy);
      # callers must consume it before the underlying buffer is reused.
      def self.parse(payload : Bytes) : Parsed
        props = AMQ::Protocol::Properties.new
        body = Bytes.empty
        pos = 0
        while pos < payload.size
          raise Error::Decode.new("Expected described section") unless payload[pos] == Codec::DESCRIBED
          pos += 1
          descriptor, pos = read_ulong(payload, pos)
          case descriptor
          when Descriptor::HEADER                 then props, pos = parse_header(payload, pos, props)
          when Descriptor::PROPERTIES             then props, pos = parse_properties(payload, pos, props)
          when Descriptor::APPLICATION_PROPERTIES then props, pos = parse_application_properties(payload, pos, props)
          when Descriptor::DATA                   then body, pos = read_binary(payload, pos)
          when Descriptor::AMQP_VALUE             then body, pos = read_amqp_value_body(payload, pos)
          else                                         pos = Codec.skip(payload, pos)
          end
        end
        Parsed.new(props, body)
      end

      # `Properties` is a value-type struct, so each helper takes it and returns
      # the mutated copy (stack-only, no heap allocation) along with the cursor.
      private def self.parse_header(p : Bytes, pos : Int32, props : AMQ::Protocol::Properties) : {AMQ::Protocol::Properties, Int32}
        count, pos = read_list_header(p, pos)
        count.times do |i|
          case i
          when 0 # durable
            durable, pos = read_bool(p, pos)
            props.delivery_mode = 2_u8 if durable
          when 1 # priority
            prio, pos = read_ubyte(p, pos)
            props.priority = prio if prio
          else
            pos = Codec.skip(p, pos)
          end
        end
        {props, pos}
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def self.parse_properties(p : Bytes, pos : Int32, props : AMQ::Protocol::Properties) : {AMQ::Protocol::Properties, Int32}
        count, pos = read_list_header(p, pos)
        count.times do |i|
          case i
          when 0 then v, pos = read_string(p, pos); props.message_id = v if v       # message-id
          when 4 then v, pos = read_string(p, pos); props.reply_to = v if v         # reply-to
          when 5 then v, pos = read_string(p, pos); props.correlation_id = v if v   # correlation-id
          when 6 then v, pos = read_string(p, pos); props.content_type = v if v     # content-type
          when 7 then v, pos = read_string(p, pos); props.content_encoding = v if v # content-encoding
          else        pos = Codec.skip(p, pos)                                      # user-id/to/subject/...
          end
        end
        {props, pos}
      end

      private def self.parse_application_properties(p : Bytes, pos : Int32, props : AMQ::Protocol::Properties) : {AMQ::Protocol::Properties, Int32}
        count, pos = read_map_header(p, pos) # count = number of values (pairs*2)
        return {props, pos} if count.zero?
        headers = AMQ::Protocol::Table.new
        (count // 2).times do
          key, pos = read_string(p, pos)
          val, pos = read_header_value(p, pos)
          headers[key || ""] = val if key
        end
        props.headers = headers
        {props, pos}
      end

      # Encode a body + AMQP 0-9-1 properties into AMQP 1.0 message sections,
      # written directly into `io` (a reusable buffer) — no allocations.
      def self.encode(io : IO::Memory, props : AMQ::Protocol::Properties, body : Bytes) : Nil
        write_properties(io, props)
        if (headers = props.headers) && !headers.empty?
          write_application_properties(io, headers)
        end
        io.write_byte Codec::DESCRIBED
        Codec.write_ulong(io, Descriptor::DATA)
        Codec.write_binary(io, body)
      end

      # Convenience wrapper that allocates (used by specs / non-hot callers).
      def self.encode(props : AMQ::Protocol::Properties, body : Bytes) : Bytes
        io = IO::Memory.new
        encode(io, props, body)
        io.to_slice
      end

      private def self.write_properties(io : IO::Memory, props : AMQ::Protocol::Properties) : Nil
        return unless props.message_id || props.correlation_id || props.content_type ||
                      props.content_encoding || props.reply_to
        Codec.write_described_list(io, Descriptor::PROPERTIES, 8) do |b|
          write_string_or_null(b, props.message_id) # message-id
          b.write_byte Codec::NULL                  # user-id
          b.write_byte Codec::NULL                  # to
          b.write_byte Codec::NULL                  # subject
          write_string_or_null(b, props.reply_to)
          write_string_or_null(b, props.correlation_id)
          write_symbol_or_null(b, props.content_type)
          write_symbol_or_null(b, props.content_encoding)
        end
      end

      private def self.write_application_properties(io : IO::Memory, headers : AMQ::Protocol::Table) : Nil
        io.write_byte Codec::DESCRIBED
        Codec.write_ulong(io, Descriptor::APPLICATION_PROPERTIES)
        Codec.write_map(io, headers.size) do |b|
          headers.each do |k, v|
            Codec.write_string(b, k)
            write_header_value(b, v)
          end
        end
      end

      private def self.write_string_or_null(io : IO::Memory, v : String?) : Nil
        v.nil? ? io.write_byte(Codec::NULL) : Codec.write_string(io, v)
      end

      private def self.write_symbol_or_null(io : IO::Memory, v : String?) : Nil
        v.nil? ? io.write_byte(Codec::NULL) : Codec.write_symbol(io, v)
      end

      private def self.write_header_value(io : IO::Memory, v) : Nil
        case v
        when String then Codec.write_string(io, v)
        when Bool   then Codec.write_bool(io, v)
        when Int    then Codec.write_long(io, v.to_i64)
        when Float  then io.write_byte(Codec::DOUBLE); v.to_f64.to_io(io, Codec::BE)
        when Nil    then io.write_byte(Codec::NULL)
        else             Codec.write_string(io, v.to_s)
        end
      end

      # ---- byte-cursor readers (return {value, new_pos}) ----

      private def self.read_ulong(p : Bytes, pos : Int32) : {UInt64, Int32}
        code = p[pos]
        pos += 1
        case code
        when Codec::SMALLULONG then {p[pos].to_u64, pos + 1}
        when Codec::ULONG0     then {0_u64, pos}
        when Codec::ULONG      then {Codec::BE.decode(UInt64, p[pos, 8]), pos + 8}
        else                        raise Error::Decode.new("Expected ulong descriptor, got 0x#{code.to_s(16)}")
        end
      end

      private def self.read_list_header(p : Bytes, pos : Int32) : {Int32, Int32}
        code = p[pos]
        pos += 1
        case code
        when Codec::LIST0  then {0, pos}
        when Codec::LIST8  then {p[pos + 1].to_i32, pos + 2}          # size(1), count(1)
        when Codec::LIST32 then {Codec.read_u32(p, pos + 4), pos + 8} # size(4), count(4)
        else                    raise Error::Decode.new("Expected list, got 0x#{code.to_s(16)}")
        end
      end

      private def self.read_map_header(p : Bytes, pos : Int32) : {Int32, Int32}
        code = p[pos]
        pos += 1
        case code
        when Codec::MAP8  then {p[pos + 1].to_i32, pos + 2}
        when Codec::MAP32 then {Codec.read_u32(p, pos + 4), pos + 8}
        else                   raise Error::Decode.new("Expected map, got 0x#{code.to_s(16)}")
        end
      end

      # Read a string field; returns nil (skipping the value) for non-strings.
      private def self.read_string(p : Bytes, pos : Int32) : {String?, Int32}
        code = p[pos]
        case code
        when Codec::STR8, Codec::SYM8
          len = p[pos + 1].to_i32
          {String.new(p[pos + 2, len]), pos + 2 + len}
        when Codec::STR32, Codec::SYM32
          len = Codec.read_u32(p, pos + 1)
          {String.new(p[pos + 5, len]), pos + 5 + len}
        else
          {nil, Codec.skip(p, pos)}
        end
      end

      # Read a binary field as a slice into `p` (no copy).
      private def self.read_binary(p : Bytes, pos : Int32) : {Bytes, Int32}
        code = p[pos]
        case code
        when Codec::VBIN8
          len = p[pos + 1].to_i32
          {p[pos + 2, len], pos + 2 + len}
        when Codec::VBIN32
          len = Codec.read_u32(p, pos + 1)
          {p[pos + 5, len], pos + 5 + len}
        else
          {Bytes.empty, Codec.skip(p, pos)}
        end
      end

      # An amqp-value body holding a string or binary, as a slice where possible.
      private def self.read_amqp_value_body(p : Bytes, pos : Int32) : {Bytes, Int32}
        code = p[pos]
        case code
        when Codec::STR8, Codec::VBIN8, Codec::SYM8
          len = p[pos + 1].to_i32
          {p[pos + 2, len], pos + 2 + len}
        when Codec::STR32, Codec::VBIN32, Codec::SYM32
          len = Codec.read_u32(p, pos + 1)
          {p[pos + 5, len], pos + 5 + len}
        else
          {Bytes.empty, Codec.skip(p, pos)}
        end
      end

      private def self.read_bool(p : Bytes, pos : Int32) : {Bool, Int32}
        case p[pos]
        when Codec::BOOL_TRUE  then {true, pos + 1}
        when Codec::BOOL_FALSE then {false, pos + 1}
        when Codec::BOOL       then {p[pos + 1] != 0_u8, pos + 2}
        else                        {false, Codec.skip(p, pos)}
        end
      end

      private def self.read_ubyte(p : Bytes, pos : Int32) : {UInt8?, Int32}
        case p[pos]
        when Codec::UBYTE, Codec::SMALLUINT, Codec::SMALLULONG then {p[pos + 1], pos + 2}
        when Codec::UINT0, Codec::ULONG0                       then {0_u8, pos + 1}
        else                                                        {nil, Codec.skip(p, pos)}
        end
      end

      # Read an application-property value into an AMQ table field.
      private def self.read_header_value(p : Bytes, pos : Int32) : {AMQ::Protocol::Field, Int32}
        code = p[pos]
        case code
        when Codec::STR8, Codec::STR32, Codec::SYM8, Codec::SYM32
          read_string(p, pos)
        when Codec::BOOL_TRUE                  then {true, pos + 1}
        when Codec::BOOL_FALSE                 then {false, pos + 1}
        when Codec::NULL                       then {nil, pos + 1}
        when Codec::SMALLLONG, Codec::SMALLINT then {p[pos + 1].to_i8!.to_i64, pos + 2}
        when Codec::LONG                       then {Codec::BE.decode(Int64, p[pos + 1, 8]), pos + 9}
        when Codec::INT                        then {Codec::BE.decode(Int32, p[pos + 1, 4]).to_i64, pos + 5}
        when Codec::DOUBLE                     then {Codec::BE.decode(Float64, p[pos + 1, 8]), pos + 9}
        else
          {nil, Codec.skip(p, pos)}
        end
      end
    end
  end
end
