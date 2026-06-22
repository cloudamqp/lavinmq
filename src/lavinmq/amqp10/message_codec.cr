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

      until reader.empty?
        descriptor = read_descriptor_code(reader)
        case descriptor
        when Descriptor::HEADER, Descriptor::DELIVERY_ANNOTATIONS, Descriptor::MESSAGE_ANNOTATIONS, Descriptor::FOOTER
          skip_value(reader)
        when Descriptor::PROPERTIES
          to = read_properties(reader, props)
        when Descriptor::APPLICATION_PROPERTIES
          skip_value(reader)
        when Descriptor::DATA
          body = read_binary_value(reader)
        when Descriptor::AMQP_VALUE
          skip_value(reader)
        else
          skip_value(reader)
        end
      end

      Incoming.new(props, body, to)
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
        reader.read_slice(reader.read_u32.to_i)
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
        String.new(reader.read_slice(reader.read_u32.to_i))
      else
        skip_value_payload(reader, code)
        nil
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_properties(reader, props) : String?
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
      to
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_message_id(reader) : String?
      case code = reader.read_byte
      when 0x40
        nil
      when 0xa1, 0xa3
        String.new(reader.read_slice(reader.read_byte.to_i))
      when 0xb1, 0xb3
        String.new(reader.read_slice(reader.read_u32.to_i))
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
        String.new(reader.read_slice(reader.read_u32.to_i))
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
        reader.skip(reader.read_u32.to_i)
      when 0xc0, 0xc1, 0xe0
        reader.skip(reader.read_byte.to_i)
      when 0xd0, 0xd1, 0xf0
        reader.skip(reader.read_u32.to_i)
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
        size = reader.read_u32.to_i
        count = reader.read_u32.to_i
        {count, reader.pos + size - 4}
      else
        raise DecodeError.new("expected list, got 0x#{code.to_s(16)}")
      end
    end
  end
end
