require "../amqp"
require "../message"
require "./slice_reader"
require "./types"

module LavinMQ::AMQP10
  module MessageCodec
    extend self

    EMPTY_BODY = Bytes.empty

    record Incoming, properties : LavinMQ::AMQP::Properties, body : Bytes, to : String?

    def decode(reader : SliceReader) : Incoming
      props = LavinMQ::AMQP::Properties.new
      to = nil
      body = EMPTY_BODY
      body_io : IO::Memory? = nil

      until reader.empty?
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

    def read_descriptor_code(reader : SliceReader) : UInt64
      code = reader.read_byte
      raise DecodeError.new("expected described type") unless code == 0x00
      read_uint_value(reader)
    end

    def read_uint_value(reader : SliceReader) : UInt64
      case code = reader.read_byte
      when 0x43 then 0_u64
      when 0x44 then 0_u64
      when 0x50 then reader.read_byte.to_u64
      when 0x52 then reader.read_byte.to_u64
      when 0x53 then reader.read_byte.to_u64
      when 0x60 then reader.read_u16.to_u64
      when 0x70 then reader.read_u32.to_u64
      when 0x80 then reader.read_u64
      else
        raise DecodeError.new("expected uint-like value, got 0x#{code.to_s(16)}")
      end
    end

    def read_bool_value(reader : SliceReader) : Bool
      case code = reader.read_byte
      when 0x41 then true
      when 0x42 then false
      when 0x56 then !reader.read_byte.zero?
      else
        raise DecodeError.new("expected boolean, got 0x#{code.to_s(16)}")
      end
    end

    def read_binary_value(reader : SliceReader) : Bytes
      case code = reader.read_byte
      when 0xa0
        reader.read_slice(reader.read_byte.to_i)
      when 0xb0
        reader.read_slice(read_size32(reader, "binary32"))
      when 0x40
        EMPTY_BODY
      else
        raise DecodeError.new("expected binary, got 0x#{code.to_s(16)}")
      end
    end

    def read_string_value(reader : SliceReader) : String?
      case code = reader.read_byte
      when 0x40
        nil
      when 0xa1, 0xa3
        String.new(reader.read_slice(reader.read_byte.to_i))
      when 0xb1, 0xb3
        String.new(reader.read_slice(read_size32(reader, "string32")))
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_amqp_value_body(reader : SliceReader) : Bytes
      case code = reader.read_byte
      when 0x40
        EMPTY_BODY
      when 0xa0, 0xa1, 0xa3
        reader.read_slice(reader.read_byte.to_i)
      when 0xb0, 0xb1, 0xb3
        reader.read_slice(read_size32(reader, "value32"))
      else
        skip_value_payload(reader, code)
        EMPTY_BODY
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
      case code = reader.read_byte
      when 0x40 then nil
      when 0x41 then true
      when 0x42 then false
      when 0x50 then reader.read_byte
      when 0x51 then reader.read_byte.to_i8
      when 0x52 then reader.read_byte.to_u32
      when 0x53 then reader.read_byte.to_i64
      when 0x54 then reader.read_byte.to_i8.to_i32
      when 0x55 then reader.read_byte.to_i8.to_i64
      when 0x56 then !reader.read_byte.zero?
      when 0x60 then reader.read_u16
      when 0x61 then IO::ByteFormat::NetworkEndian.decode(Int16, reader.read_slice(2))
      when 0x70 then reader.read_u32
      when 0x71 then reader.read_i32
      when 0x72 then reader.read_f32
      when 0x80
        value = reader.read_u64
        value <= Int64::MAX ? value.to_i64 : nil
      when 0x81       then reader.read_i64
      when 0x82       then reader.read_f64
      when 0x83       then Time.unix_ms(reader.read_i64)
      when 0xa0       then reader.read_slice(reader.read_byte.to_i)
      when 0xb0       then reader.read_slice(read_size32(reader, "binary32"))
      when 0xa1, 0xa3 then String.new(reader.read_slice(reader.read_byte.to_i))
      when 0xb1, 0xb3 then String.new(reader.read_slice(read_size32(reader, "string32")))
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
          props.message_id = read_message_id(reader)
        when 1
          if user_id = read_binary_value(reader)
            props.user_id = String.new(user_id)
          end
        when 2
          to = read_string_value(reader)
        when 3
          props.type = read_string_value(reader)
        when 4
          props.reply_to = read_string_value(reader)
        when 5
          props.correlation_id = read_message_id(reader)
        when 6
          props.content_type = read_string_value(reader)
        when 7
          props.content_encoding = read_string_value(reader)
        when 8
          expiry = read_timestamp_value(reader)
          if expiry
            ttl = expiry - RoughTime.unix_ms
            props.expiration = Math.max(ttl, 0_i64).to_s
          end
        when 9
          created = read_timestamp_value(reader)
          props.timestamp = Time.unix_ms(created) if created
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
      case code = reader.read_byte
      when 0x40
        nil
      when 0xa1, 0xa3
        String.new(reader.read_slice(reader.read_byte.to_i))
      when 0xb1, 0xb3
        String.new(reader.read_slice(read_size32(reader, "string32")))
      when 0x43
        "0"
      when 0x52
        reader.read_byte.to_s
      when 0x70
        reader.read_u32.to_s
      when 0x44
        "0"
      when 0x53
        reader.read_byte.to_s
      when 0x80
        reader.read_u64.to_s
      when 0xa0
        String.new(reader.read_slice(reader.read_byte.to_i))
      when 0xb0
        String.new(reader.read_slice(read_size32(reader, "binary32")))
      when 0x98
        read_uuid_value(reader)
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_timestamp_value(reader) : Int64?
      case code = reader.read_byte
      when 0x40 then nil
      when 0x83 then reader.read_i64
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_optional_bool_value(reader) : Bool?
      case code = reader.read_byte
      when 0x40 then nil
      when 0x41 then true
      when 0x42 then false
      when 0x56 then !reader.read_byte.zero?
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    private def read_optional_ubyte_value(reader) : UInt8?
      case code = reader.read_byte
      when 0x40             then nil
      when 0x43, 0x44       then 0_u8
      when 0x50, 0x52, 0x53 then reader.read_byte
      when 0x60
        value = reader.read_u16
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x70
        value = reader.read_u32
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x80
        value = reader.read_u64
        value <= UInt8::MAX ? value.to_u8 : nil
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    def skip_value(reader : SliceReader) : Nil
      skip_value_payload(reader, reader.read_byte)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def skip_value_payload(reader : SliceReader, code : UInt8) : Nil
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
      when 0x98
        reader.skip(16)
      when 0xa0, 0xa1, 0xa3
        reader.skip(reader.read_byte.to_i)
      when 0xb0, 0xb1, 0xb3
        reader.skip(read_size32(reader, "value32"))
      when 0xc0, 0xc1, 0xe0
        reader.skip(reader.read_byte.to_i)
      when 0xd0, 0xd1, 0xf0
        reader.skip(read_size32(reader, "compound32"))
      else
        raise DecodeError.new("unsupported value 0x#{code.to_s(16)}")
      end
    end

    def read_list_header(reader : SliceReader) : Tuple(Int32, Int32)
      case code = reader.read_byte
      when 0x45
        {0, reader.pos}
      when 0xc0
        size = reader.read_byte.to_i
        count = reader.read_byte.to_i
        {count, reader.pos + size - 1}
      when 0xd0
        read_compound32_header(reader, "list32")
      else
        raise DecodeError.new("expected list, got 0x#{code.to_s(16)}")
      end
    end

    private def read_map_header(reader : SliceReader) : Tuple(Int32, Int32)
      case code = reader.read_byte
      when 0xc1
        size = reader.read_byte.to_i
        count = reader.read_byte.to_i
        {count, reader.pos + size - 1}
      when 0xd1
        read_compound32_header(reader, "map32")
      else
        raise DecodeError.new("expected map, got 0x#{code.to_s(16)}")
      end
    end

    private def read_size32(reader : SliceReader, type : String) : Int32
      size = reader.read_u32
      if size > reader.remaining.to_u32
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      size.to_i
    end

    private def read_compound32_header(reader : SliceReader, type : String) : Tuple(Int32, Int32)
      size = reader.read_u32
      count = reader.read_u32
      if size < 4
        raise DecodeError.new("#{type} size #{size} smaller than count field")
      end
      payload_size = size - 4
      if payload_size > reader.remaining.to_u32
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      if count > payload_size
        raise DecodeError.new("#{type} count #{count} exceeds payload size #{payload_size}")
      end
      {count.to_i, reader.pos + payload_size.to_i}
    end

    private def read_uuid_value(reader : SliceReader) : String
      bytes = reader.read_slice(16)
      String.build(36) do |io|
        bytes.each_with_index do |byte, index|
          io << '-' if index.in?(4, 6, 8, 10)
          io << byte.to_s(16).rjust(2, '0')
        end
      end
    end
  end
end
