require "uuid"
require "../amqp"
require "../message"
require "./codec"
require "./types"

module LavinMQ::AMQP10
  module MessageCodec
    extend self

    EMPTY_BODY = Bytes.empty

    record Incoming, properties : LavinMQ::AMQP::Properties, body : Bytes, to : String?

    def decode(reader : IO::Memory) : Incoming
      props = LavinMQ::AMQP::Properties.new
      to = nil
      body = EMPTY_BODY
      body_io : IO::Memory? = nil

      until Codec.exhausted?(reader)
        descriptor = read_descriptor_code(reader)
        case descriptor
        when Descriptor::HEADER
          props = read_header(reader, props)
        when Descriptor::DELIVERY_ANNOTATIONS, Descriptor::MESSAGE_ANNOTATIONS, Descriptor::FOOTER
          skip_value(reader)
        when Descriptor::PROPERTIES
          props, to = read_properties(reader, props)
        when Descriptor::APPLICATION_PROPERTIES
          props = read_application_properties(reader, props)
        when Descriptor::DATA
          body, body_io = append_data_section(body, body_io, read_binary_value(reader))
        when Descriptor::AMQP_VALUE
          body_io = nil
          body = read_amqp_value_body(reader)
        else
          skip_value(reader)
        end
      end

      if chunks = body_io
        body = chunks.to_slice
      end
      Incoming.new(props, body, to)
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 message", cause: ex)
    end

    private def append_data_section(body : Bytes, body_io : IO::Memory?, section : Bytes) : Tuple(Bytes, IO::Memory?)
      if chunks = body_io
        chunks.write section
        {body, chunks}
      elsif body.empty?
        {section, nil}
      else
        chunks = IO::Memory.new
        chunks.write body
        chunks.write section
        {EMPTY_BODY, chunks}
      end
    end

    # Descriptors may be encoded either numerically (ulong code) or symbolically
    # (§1.6). Map the symbolic names of every section and outcome we understand
    # back to their numeric code so peers that describe by name interoperate.
    DESCRIPTOR_SYMBOLS = {
      "amqp:header:list"                => Descriptor::HEADER,
      "amqp:delivery-annotations:map"   => Descriptor::DELIVERY_ANNOTATIONS,
      "amqp:message-annotations:map"    => Descriptor::MESSAGE_ANNOTATIONS,
      "amqp:properties:list"            => Descriptor::PROPERTIES,
      "amqp:application-properties:map" => Descriptor::APPLICATION_PROPERTIES,
      "amqp:data:binary"                => Descriptor::DATA,
      "amqp:amqp-value:*"               => Descriptor::AMQP_VALUE,
      "amqp:footer:map"                 => Descriptor::FOOTER,
      "amqp:accepted:list"              => Descriptor::ACCEPTED,
      "amqp:rejected:list"              => Descriptor::REJECTED,
      "amqp:released:list"              => Descriptor::RELEASED,
      "amqp:modified:list"              => Descriptor::MODIFIED,
    }

    def read_descriptor_code(reader : IO::Memory) : UInt64
      raise DecodeError.new("expected described type") unless Codec.read_byte(reader) == 0x00
      case code = Codec.read_byte(reader)
      when 0xa3 then symbolic_descriptor(reader.read_string(Codec.read_byte(reader).to_i))
      when 0xb3 then symbolic_descriptor(reader.read_string(Codec.read_size32(reader, "symbol32")))
      else           uint_value(reader, code)
      end
    end

    private def symbolic_descriptor(name : String) : UInt64
      DESCRIPTOR_SYMBOLS[name]? || raise DecodeError.new("unknown descriptor #{name.inspect}")
    end

    def read_uint_value(reader : IO::Memory) : UInt64
      uint_value(reader, Codec.read_byte(reader))
    end

    private def uint_value(reader : IO::Memory, code : UInt8) : UInt64
      case code
      when 0x43 then 0_u64
      when 0x44 then 0_u64
      when 0x50 then Codec.read_byte(reader).to_u64
      when 0x52 then Codec.read_byte(reader).to_u64
      when 0x53 then Codec.read_byte(reader).to_u64
      when 0x60 then Codec.read_u16(reader).to_u64
      when 0x70 then Codec.read_u32(reader).to_u64
      when 0x80 then Codec.read_u64(reader)
      else
        raise DecodeError.new("expected uint-like value, got 0x#{code.to_s(16)}")
      end
    end

    def read_bool_value(reader : IO::Memory) : Bool
      case code = Codec.read_byte(reader)
      when 0x41 then true
      when 0x42 then false
      when 0x56 then !Codec.read_byte(reader).zero?
      else
        raise DecodeError.new("expected boolean, got 0x#{code.to_s(16)}")
      end
    end

    def read_binary_value(reader : IO::Memory) : Bytes
      case code = Codec.read_byte(reader)
      when 0xa0
        Codec.read_slice(reader, Codec.read_byte(reader).to_i)
      when 0xb0
        Codec.read_slice(reader, Codec.read_size32(reader, "binary32"))
      when 0x40
        EMPTY_BODY
      else
        raise DecodeError.new("expected binary, got 0x#{code.to_s(16)}")
      end
    end

    def read_string_value(reader : IO::Memory) : String?
      case code = Codec.read_byte(reader)
      when 0x40
        nil
      when 0xa1, 0xa3
        reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb1, 0xb3
        reader.read_string(Codec.read_size32(reader, "string32"))
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_amqp_value_body(reader : IO::Memory) : Bytes
      start = reader.pos
      case code = Codec.read_byte(reader)
      when 0x40
        EMPTY_BODY
      when 0xa0, 0xa1, 0xa3
        Codec.read_slice(reader, Codec.read_byte(reader).to_i)
      when 0xb0, 0xb1, 0xb3
        Codec.read_slice(reader, Codec.read_size32(reader, "value32"))
      else
        # Structured amqp-value bodies (lists, maps, numbers) have no 0-9-1
        # equivalent; preserve the raw encoded value verbatim instead of
        # silently dropping it.
        skip_value_payload(reader, code)
        Codec.slice_from(reader, start)
      end
    end

    private def read_header(reader, props) : LavinMQ::AMQP::Properties
      count, end_pos = read_list_header(reader)
      index = 0
      while index < count
        case index
        when 0
          props.delivery_mode = 2_u8 if read_optional_bool_value(reader)
        when 1
          if priority = read_optional_ubyte_value(reader)
            props.priority = priority
          end
        when 2
          # header ttl (milliseconds) maps to the 0-9-1 expiration.
          if ttl = read_optional_uint_value(reader)
            props.expiration = ttl.to_s
          end
        else
          skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      props
    end

    private def read_application_properties(reader, props) : LavinMQ::AMQP::Properties
      count, end_pos = read_map_header(reader)
      if count > 0
        headers = LavinMQ::AMQP::Table.new
        (count // 2).times do
          key = read_string_value(reader)
          value = read_application_property_value(reader)
          headers[key] = value if key
        end
        props.headers = headers unless headers.empty?
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      props
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_application_property_value(reader) : LavinMQ::AMQP::Field
      case code = Codec.read_byte(reader)
      when 0x40 then nil
      when 0x41 then true
      when 0x42 then false
      when 0x50 then Codec.read_byte(reader)
      when 0x43 then 0_u32
      when 0x44 then 0_i64
      when 0x51 then Codec.read_byte(reader).to_i8!
      when 0x52 then Codec.read_byte(reader).to_u32
      when 0x53 then Codec.read_byte(reader).to_i64
      when 0x54 then Codec.read_byte(reader).to_i8!.to_i32
      when 0x55 then Codec.read_byte(reader).to_i8!.to_i64
      when 0x56 then !Codec.read_byte(reader).zero?
      when 0x60 then Codec.read_u16(reader)
      when 0x61 then Codec.read_i16(reader)
      when 0x70 then Codec.read_u32(reader)
      when 0x71 then Codec.read_i32(reader)
      when 0x72 then Codec.read_f32(reader)
      when 0x80
        value = Codec.read_u64(reader)
        value <= Int64::MAX ? value.to_i64 : nil
      when 0x81       then Codec.read_i64(reader)
      when 0x82       then Codec.read_f64(reader)
      when 0x83       then safe_time(Codec.read_i64(reader))
      when 0xa0       then Codec.read_slice(reader, Codec.read_byte(reader).to_i)
      when 0xb0       then Codec.read_slice(reader, Codec.read_size32(reader, "binary32"))
      when 0xa1, 0xa3 then reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb1, 0xb3 then reader.read_string(Codec.read_size32(reader, "string32"))
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_properties(reader, props) : Tuple(LavinMQ::AMQP::Properties, String?)
      count, end_pos = read_list_header(reader)
      to = nil
      index = 0
      while index < count
        case index
        when 0
          props.message_id = shortstr(read_message_id(reader))
        when 1
          if user_id = read_binary_value(reader)
            props.user_id = shortstr(String.new(user_id))
          end
        when 2
          to = read_string_value(reader)
        when 3
          props.type = shortstr(read_string_value(reader))
        when 4
          props.reply_to = shortstr(read_string_value(reader))
        when 5
          props.correlation_id = shortstr(read_message_id(reader))
        when 6
          props.content_type = shortstr(read_string_value(reader))
        when 7
          props.content_encoding = shortstr(read_string_value(reader))
        when 8
          if expiry = read_timestamp_value(reader)
            # Guard against Int64 underflow on hostile far-past timestamps: any
            # expiry at or before now yields a zero ttl.
            now = RoughTime.unix_ms
            props.expiration = (expiry > now ? expiry - now : 0_i64).to_s
          end
        when 9
          if created = read_timestamp_value(reader)
            if ts = safe_time(created)
              props.timestamp = ts
            end
          end
        else
          skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      {props, to}
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_message_id(reader) : String?
      case code = Codec.read_byte(reader)
      when 0x40
        nil
      when 0xa1, 0xa3
        reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb1, 0xb3
        reader.read_string(Codec.read_size32(reader, "string32"))
      when 0x43
        "0"
      when 0x52
        Codec.read_byte(reader).to_s
      when 0x70
        Codec.read_u32(reader).to_s
      when 0x44
        "0"
      when 0x53
        Codec.read_byte(reader).to_s
      when 0x80
        Codec.read_u64(reader).to_s
      when 0xa0
        reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb0
        reader.read_string(Codec.read_size32(reader, "binary32"))
      when 0x98
        read_uuid_value(reader)
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_timestamp_value(reader) : Int64?
      case code = Codec.read_byte(reader)
      when 0x40 then nil
      when 0x83 then Codec.read_i64(reader)
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_optional_bool_value(reader) : Bool?
      case code = Codec.read_byte(reader)
      when 0x40 then nil
      when 0x41 then true
      when 0x42 then false
      when 0x56 then !Codec.read_byte(reader).zero?
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_optional_uint_value(reader) : UInt32?
      case code = Codec.read_byte(reader)
      when 0x40             then nil
      when 0x43, 0x44       then 0_u32
      when 0x50, 0x52, 0x53 then Codec.read_byte(reader).to_u32
      when 0x60             then Codec.read_u16(reader).to_u32
      when 0x70             then Codec.read_u32(reader)
      when 0x80
        value = Codec.read_u64(reader)
        value <= UInt32::MAX ? value.to_u32 : nil
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    # Time.unix_ms raises ArgumentError outside the year 1..9999 range; any Int64
    # is a legal wire timestamp, so clamp invalid values to nil instead of
    # tearing down the connection.
    private def safe_time(ms : Int64) : Time?
      Time.unix_ms(ms)
    rescue ArgumentError
      nil
    end

    # AMQP 1.0 string properties may exceed 255 bytes, but the 0-9-1 properties
    # they map to are short strings; reject over-long values at decode time so we
    # never crash mid-write into the message store.
    private def shortstr(value : String?) : String?
      if value && value.bytesize > 255
        raise DecodeError.new("AMQP 1.0 string property exceeds 255 bytes")
      end
      value
    end

    private def read_optional_ubyte_value(reader) : UInt8?
      case code = Codec.read_byte(reader)
      when 0x40             then nil
      when 0x43, 0x44       then 0_u8
      when 0x50, 0x52, 0x53 then Codec.read_byte(reader)
      when 0x60
        value = Codec.read_u16(reader)
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x70
        value = Codec.read_u32(reader)
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x80
        value = Codec.read_u64(reader)
        value <= UInt8::MAX ? value.to_u8 : nil
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    def skip_value(reader : IO::Memory) : Nil
      skip_value_payload(reader, Codec.read_byte(reader))
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def skip_value_payload(reader : IO::Memory, code : UInt8) : Nil
      case code
      when 0x00
        skip_value(reader)
        skip_value(reader)
      when 0x40, 0x41, 0x42, 0x43, 0x44, 0x45
      when 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56
        reader.skip(1)
      when 0x60
        reader.skip(2)
      when 0x70, 0x71, 0x72, 0x73, 0x74
        reader.skip(4)
      when 0x80, 0x81, 0x82, 0x83, 0x84
        reader.skip(8)
      when 0x94, 0x98
        reader.skip(16)
      when 0xa0, 0xa1, 0xa3
        reader.skip(Codec.read_byte(reader).to_i)
      when 0xb0, 0xb1, 0xb3
        reader.skip(Codec.read_size32(reader, "value32"))
      when 0xc0, 0xc1, 0xe0
        reader.skip(Codec.read_byte(reader).to_i)
      when 0xd0, 0xd1, 0xf0
        reader.skip(Codec.read_size32(reader, "compound32"))
      else
        raise DecodeError.new("unsupported value 0x#{code.to_s(16)}")
      end
    end

    def read_list_header(reader : IO::Memory) : Tuple(Int32, Int32)
      case code = Codec.read_byte(reader)
      when 0x45
        {0, reader.pos}
      when 0xc0
        read_compound8_header(reader, "list8")
      when 0xd0
        read_compound32_header(reader, "list32")
      else
        raise DecodeError.new("expected list, got 0x#{code.to_s(16)}")
      end
    end

    private def read_map_header(reader : IO::Memory) : Tuple(Int32, Int32)
      case code = Codec.read_byte(reader)
      when 0xc1
        read_compound8_header(reader, "map8")
      when 0xd1
        read_compound32_header(reader, "map32")
      else
        raise DecodeError.new("expected map, got 0x#{code.to_s(16)}")
      end
    end

    private def read_compound8_header(reader : IO::Memory, type : String) : Tuple(Int32, Int32)
      size = Codec.read_byte(reader).to_i
      count = Codec.read_byte(reader).to_i
      if size < 1
        raise DecodeError.new("#{type} size #{size} smaller than count field")
      end
      payload_size = size - 1
      if payload_size > Codec.remaining(reader)
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      if count > payload_size
        raise DecodeError.new("#{type} count #{count} exceeds payload size #{payload_size}")
      end
      {count, reader.pos + payload_size}
    end

    private def read_compound32_header(reader : IO::Memory, type : String) : Tuple(Int32, Int32)
      size = Codec.read_u32(reader)
      count = Codec.read_u32(reader)
      if size < 4
        raise DecodeError.new("#{type} size #{size} smaller than count field")
      end
      payload_size = size - 4
      if payload_size > Codec.remaining(reader).to_u32
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      if count > payload_size
        raise DecodeError.new("#{type} count #{count} exceeds payload size #{payload_size}")
      end
      {count.to_i, reader.pos + payload_size.to_i}
    end

    private def read_uuid_value(reader : IO::Memory) : String
      UUID.new(Codec.read_slice(reader, 16)).to_s
    end
  end
end
