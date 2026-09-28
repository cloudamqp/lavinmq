require "uuid"
require "../amqp"
require "../message"
require "./protocol"

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

      until reader.pos >= reader.bytesize
        descriptor = Codec.read_descriptor_code(reader)
        case descriptor
        when Descriptor::HEADER
          props = read_header(reader, props)
        when Descriptor::DELIVERY_ANNOTATIONS, Descriptor::MESSAGE_ANNOTATIONS, Descriptor::FOOTER
          Codec.skip_value(reader)
        when Descriptor::PROPERTIES
          props, to = read_properties(reader, props)
        when Descriptor::APPLICATION_PROPERTIES
          props = read_application_properties(reader, props)
        when Descriptor::DATA
          body, body_io = append_data_section(body, body_io, Codec.read_binary_value(reader))
        when Descriptor::AMQP_VALUE
          body_io = nil
          body = read_amqp_value_body(reader)
        else
          Codec.skip_value(reader)
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
        Codec.skip_value_payload(reader, code)
        Codec.slice_from(reader, start)
      end
    end

    private def read_header(reader, props) : LavinMQ::AMQP::Properties
      count, end_pos = Codec.read_list_header(reader)
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
          Codec.skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      props
    end

    private def read_application_properties(reader, props) : LavinMQ::AMQP::Properties
      count, end_pos = Codec.read_map_header(reader)
      if count > 0
        headers = LavinMQ::AMQP::Table.new
        (count // 2).times do
          key = Codec.read_string_value(reader)
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
      when 0x60 then reader.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      when 0x61 then reader.read_bytes(Int16, IO::ByteFormat::NetworkEndian)
      when 0x70 then reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      when 0x71 then reader.read_bytes(Int32, IO::ByteFormat::NetworkEndian)
      when 0x72 then reader.read_bytes(Float32, IO::ByteFormat::NetworkEndian)
      when 0x80
        value = reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        value <= Int64::MAX ? value.to_i64 : nil
      when 0x81       then reader.read_bytes(Int64, IO::ByteFormat::NetworkEndian)
      when 0x82       then reader.read_bytes(Float64, IO::ByteFormat::NetworkEndian)
      when 0x83       then safe_time(reader.read_bytes(Int64, IO::ByteFormat::NetworkEndian))
      when 0xa0       then Codec.read_slice(reader, Codec.read_byte(reader).to_i)
      when 0xb0       then Codec.read_slice(reader, Codec.read_size32(reader, "binary32"))
      when 0xa1, 0xa3 then reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb1, 0xb3 then reader.read_string(Codec.read_size32(reader, "string32"))
      else
        Codec.skip_value_payload(reader, code)
        nil
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def read_properties(reader, props) : Tuple(LavinMQ::AMQP::Properties, String?)
      count, end_pos = Codec.read_list_header(reader)
      to = nil
      index = 0
      while index < count
        case index
        when 0
          props.message_id = shortstr(read_message_id(reader))
        when 1
          if user_id = Codec.read_binary_value(reader)
            props.user_id = shortstr(String.new(user_id))
          end
        when 2
          to = Codec.read_string_value(reader)
        when 3
          props.type = shortstr(Codec.read_string_value(reader))
        when 4
          props.reply_to = shortstr(Codec.read_string_value(reader))
        when 5
          props.correlation_id = shortstr(read_message_id(reader))
        when 6
          props.content_type = shortstr(Codec.read_string_value(reader))
        when 7
          props.content_encoding = shortstr(Codec.read_string_value(reader))
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
          Codec.skip_value(reader)
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
        reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian).to_s
      when 0x44
        "0"
      when 0x53
        Codec.read_byte(reader).to_s
      when 0x80
        reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian).to_s
      when 0xa0
        reader.read_string(Codec.read_byte(reader).to_i)
      when 0xb0
        reader.read_string(Codec.read_size32(reader, "binary32"))
      when 0x98
        read_uuid_value(reader)
      else
        Codec.skip_value_payload(reader, code)
        nil
      end
    end

    private def read_timestamp_value(reader) : Int64?
      case code = Codec.read_byte(reader)
      when 0x40 then nil
      when 0x83 then reader.read_bytes(Int64, IO::ByteFormat::NetworkEndian)
      else
        Codec.skip_value_payload(reader, code)
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
        Codec.skip_value_payload(reader, code)
        nil
      end
    end

    private def read_optional_uint_value(reader) : UInt32?
      case code = Codec.read_byte(reader)
      when 0x40             then nil
      when 0x43, 0x44       then 0_u32
      when 0x50, 0x52, 0x53 then Codec.read_byte(reader).to_u32
      when 0x60             then reader.read_bytes(UInt16, IO::ByteFormat::NetworkEndian).to_u32
      when 0x70             then reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      when 0x80
        value = reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        value <= UInt32::MAX ? value.to_u32 : nil
      else
        Codec.skip_value_payload(reader, code)
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
        value = reader.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x70
        value = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        value <= UInt8::MAX ? value.to_u8 : nil
      when 0x80
        value = reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        value <= UInt8::MAX ? value.to_u8 : nil
      else
        Codec.skip_value_payload(reader, code)
        nil
      end
    end

    private def read_uuid_value(reader : IO::Memory) : String
      UUID.new(Codec.read_slice(reader, 16)).to_s
    end

    # ---- Outgoing: a stored (0-9-1) message as an AMQP 1.0 delivery ----

    # Precomputed encoded sizes for a message's sections, so the size pass and
    # the write pass do not each re-walk the (allocating) headers Table.
    private record SectionSizes,
      total : Int32,
      header_count : Int32,
      header_fields : Int32,
      props_count : Int32,
      props_fields : Int32,
      app_fields : Int32

    # Returns the number of AMQP 1.0 transfer frames written.
    def write_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                       delivery_tag : Bytes, msg : BytesMessage, max_frame_size = UInt32::MAX,
                       settled = false) : Tuple(UInt64, UInt32)
      if msg.bodysize > UInt32::MAX
        raise ProtocolError.new("message too large for AMQP 1.0 data section")
      end

      sizes = compute_section_sizes(msg)
      prefix_size = sizes.total
      message_size = prefix_size.to_u64 + msg.bodysize
      max = effective_max_frame_size(max_frame_size)
      transfer_size = TransferCodec.transfer_performative_size(handle, delivery_id, delivery_tag, false, settled)
      frame_size = 8_u64 + transfer_size.to_u64 + message_size

      if frame_size <= max
        FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
        TransferCodec.write_transfer_performative(io, handle, delivery_id, delivery_tag, false, settled)
        write_message_sections_prefix(io, msg, sizes)
        io.write msg.body
        return {frame_size, 1_u32}
      end

      write_fragmented_transfer(io, channel, handle, delivery_id, delivery_tag, msg, prefix_size, max, settled)
    end

    private def compute_section_sizes(msg : BytesMessage) : SectionSizes
      props = msg.properties
      header_count = header_field_count(props)
      header_fields = header_count.zero? ? 0 : header_fields_size(props, header_count)
      header_sec = header_count.zero? ? 0 : 3 + Codec.list_header_size(header_fields) + header_fields
      props_count = properties_field_count(props)
      props_fields = props_count.zero? ? 0 : properties_fields_size(props, props_count)
      props_sec = props_count.zero? ? 0 : 3 + Codec.list_header_size(props_fields) + props_fields
      headers = props.headers
      if headers && !headers.empty?
        app_fields = application_properties_fields_size(headers)
        app_sec = 3 + Codec.map_header_size(app_fields, headers.size * 2) + app_fields
      else
        app_fields = 0
        app_sec = 0
      end
      data_sec = 3 + Codec.binary_header_size(msg.bodysize)
      total = header_sec + props_sec + app_sec + data_sec
      SectionSizes.new(total, header_count, header_fields, props_count, props_fields, app_fields)
    end

    private def write_message_sections_prefix(io, msg : BytesMessage, sizes : SectionSizes? = nil) : Nil
      sizes ||= compute_section_sizes(msg)
      write_header_section(io, msg.properties, sizes.header_count, sizes.header_fields)
      write_properties_section(io, msg.properties, sizes.props_count, sizes.props_fields)
      write_application_properties_section(io, msg.properties.headers, sizes.app_fields)
      Codec.write_descriptor(io, Descriptor::DATA)
      Codec.write_binary_header(io, msg.bodysize)
    end

    private def write_fragmented_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                                          delivery_tag : Bytes, msg : BytesMessage, prefix_size : Int32,
                                          max : UInt64, settled : Bool) : Tuple(UInt64, UInt32)
      prefix_offset = 0
      body_offset = 0
      body = msg.body
      first = true
      written = 0_u64
      frames = 0_u32
      prefix_writer = PrefixRangeIO.new(io)

      loop do
        remaining = prefix_size - prefix_offset + body.bytesize - body_offset
        break if remaining <= 0

        more = true
        transfer_size = if first
                          TransferCodec.transfer_performative_size(handle, delivery_id, delivery_tag, true, settled)
                        else
                          final_size = TransferCodec.continuation_transfer_performative_size(handle, false)
                          if 8_u64 + final_size.to_u64 + remaining.to_u64 <= max
                            more = false
                            final_size
                          else
                            TransferCodec.continuation_transfer_performative_size(handle, true)
                          end
                        end
        overhead = 8_u64 + transfer_size.to_u64
        if overhead >= max
          raise ProtocolError.new("max-frame-size too small for AMQP 1.0 transfer")
        end
        chunk_size = Math.min(remaining, (max - overhead).to_i)
        frame_size = overhead + chunk_size.to_u64

        FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
        if first
          TransferCodec.write_transfer_performative(io, handle, delivery_id, delivery_tag, true, settled)
          first = false
        else
          TransferCodec.write_continuation_transfer_performative(io, handle, more)
        end
        prefix_offset, body_offset = write_message_bytes(io, msg, prefix_size, prefix_offset, body, body_offset,
          chunk_size, prefix_writer)
        written += frame_size
        frames += 1
      end

      {written, frames}
    end

    private def write_message_bytes(io, msg, prefix_size, prefix_offset, body, body_offset, count, prefix_writer)
      remaining = count
      if prefix_offset < prefix_size
        prefix_count = Math.min(remaining, prefix_size - prefix_offset)
        prefix_writer.reset(prefix_offset, prefix_count)
        write_message_sections_prefix(prefix_writer, msg)
        unless prefix_writer.written == prefix_count
          raise ProtocolError.new("AMQP 1.0 message section size mismatch")
        end
        prefix_offset += prefix_count
        remaining -= prefix_count
      end
      if remaining > 0
        io.write body[body_offset, remaining]
        body_offset += remaining
      end
      {prefix_offset, body_offset}
    end

    private class PrefixRangeIO < IO
      getter written = 0

      def initialize(@io : IO)
        @skip = 0
        @remaining = 0
      end

      def reset(skip : Int32, remaining : Int32) : Nil
        @skip = skip
        @remaining = remaining
        @written = 0
      end

      def read(slice : Bytes) : Int32
        0
      end

      def write(slice : Bytes) : Nil
        if @skip >= slice.bytesize
          @skip -= slice.bytesize
          return
        end

        start = @skip
        @skip = 0
        count = Math.min(@remaining, slice.bytesize - start)
        if count > 0
          @io.write slice[start, count]
          @remaining -= count
          @written += count
        end
      end

      def write_byte(byte : UInt8) : Nil
        if @skip > 0
          @skip -= 1
        elsif @remaining > 0
          @io.write_byte byte
          @remaining -= 1
          @written += 1
        end
      end
    end

    private def effective_max_frame_size(max_frame_size : UInt32) : UInt64
      return UInt32::MAX.to_u64 if max_frame_size.zero?
      Math.max(max_frame_size, MIN_MAX_FRAME_SIZE).to_u64
    end

    private def header_ttl(props) : UInt32?
      props.expiration.try(&.to_u32?)
    end

    private def header_field_count(props) : Int32
      count = 0
      count = 1 if props.delivery_mode
      count = 2 if props.priority
      count = 3 if header_ttl(props)
      count
    end

    private def header_fields_size(props, count : Int32) : Int32
      size = 0
      index = 0
      while index < count
        size += case index
                when 0 then 1                      # durable bool
                when 1 then props.priority ? 2 : 1 # ubyte or null
                when 2
                  (ttl = header_ttl(props)) ? Codec.uint_size(ttl) : 1
                else 1
                end
        index += 1
      end
      size
    end

    private def write_header_section(io, props, count : Int32, fields_size : Int32) : Nil
      return if count.zero?
      Codec.write_descriptor(io, Descriptor::HEADER)
      Codec.write_list_header(io, fields_size, count)
      index = 0
      while index < count
        case index
        when 0 then Codec.write_bool(io, props.delivery_mode == 2_u8)
        when 1
          if priority = props.priority
            io.write_byte 0x50_u8
            io.write_byte priority
          else
            io.write_byte 0x40_u8
          end
        when 2
          if ttl = header_ttl(props)
            Codec.write_uint(io, ttl.to_u64)
          else
            io.write_byte 0x40_u8
          end
        end
        index += 1
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def write_properties_section(io, props, count : Int32, fields_size : Int32) : Nil
      return if count.zero?
      Codec.write_descriptor(io, Descriptor::PROPERTIES)
      Codec.write_list_header(io, fields_size, count)
      index = 0
      while index < count
        case index
        when 0 then Codec.write_nullable_string(io, props.message_id)
        when 1 then write_nullable_binary_string(io, props.user_id)
        when 2 then io.write_byte 0x40_u8
        when 3 then Codec.write_nullable_string(io, props.type)
        when 4 then Codec.write_nullable_string(io, props.reply_to)
        when 5 then Codec.write_nullable_string(io, props.correlation_id)
        when 6 then write_nullable_symbol(io, props.content_type)
        when 7 then write_nullable_symbol(io, props.content_encoding)
        when 8 then io.write_byte 0x40_u8
        when 9
          if ts = props.timestamp_raw
            io.write_byte 0x83_u8
            Codec.write_i64(io, timestamp_ms(ts))
          else
            io.write_byte 0x40_u8
          end
        else
          io.write_byte 0x40_u8
        end
        index += 1
      end
    end

    # 0-9-1 timestamps are seconds, AMQP 1.0 wants milliseconds. Clamp first so a
    # bogus timestamp from a publisher cannot overflow on the delivery path.
    MAX_TIMESTAMP_SECONDS = Int64::MAX.tdiv(1000)
    MIN_TIMESTAMP_SECONDS = Int64::MIN.tdiv(1000)

    private def timestamp_ms(seconds : Int64) : Int64
      seconds.clamp(MIN_TIMESTAMP_SECONDS, MAX_TIMESTAMP_SECONDS) * 1000_i64
    end

    private def write_application_properties_section(io, headers : LavinMQ::AMQP::Table?, fields_size : Int32) : Nil
      return unless headers
      return if headers.empty?
      Codec.write_descriptor(io, Descriptor::APPLICATION_PROPERTIES)
      Codec.write_map_header(io, fields_size, headers.size * 2)
      headers.each do |key, value|
        Codec.write_string(io, key)
        write_application_property_value(io, value)
      end
    end

    private def properties_field_count(props) : Int32
      count = 0
      count = 1 if props.message_id
      count = 2 if props.user_id
      count = 4 if props.type
      count = 5 if props.reply_to
      count = 6 if props.correlation_id
      count = 7 if props.content_type
      count = 8 if props.content_encoding
      count = 10 if props.timestamp_raw
      count
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def properties_fields_size(props, count) : Int32
      size = 0
      index = 0
      while index < count
        size += case index
                when 0 then Codec.nullable_string_size(props.message_id)
                when 1 then nullable_binary_string_size(props.user_id)
                when 3 then Codec.nullable_string_size(props.type)
                when 4 then Codec.nullable_string_size(props.reply_to)
                when 5 then Codec.nullable_string_size(props.correlation_id)
                when 6 then Codec.nullable_string_size(props.content_type)
                when 7 then Codec.nullable_string_size(props.content_encoding)
                when 9 then props.timestamp_raw ? 9 : 1
                else        1
                end
        index += 1
      end
      size
    end

    private def nullable_binary_string_size(value : String?) : Int32
      value ? Codec.binary_header_size(value.bytesize.to_u64) + value.bytesize : 1
    end

    private def application_properties_fields_size(headers : LavinMQ::AMQP::Table) : Int32
      size = 0
      headers.each do |key, value|
        size += Codec.string_size(key)
        size += application_property_value_size(value)
      end
      size
    end

    private def write_nullable_symbol(io, value : String?) : Nil
      value ? Codec.write_symbol(io, value) : io.write_byte(0x40_u8)
    end

    private def write_nullable_binary_string(io, value : String?) : Nil
      if value
        bytes = value.to_slice
        Codec.write_binary(io, bytes)
      else
        io.write_byte 0x40_u8
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def application_property_value_size(value) : Int32
      case value
      when Nil, Bool
        1
      when Int8, UInt8
        2
      when Int16, UInt16
        3
      when Int32
        int_size(value)
      when UInt32
        Codec.uint_size(value)
      when Float32
        5
      when Int64
        long_size(value)
      when Float64, Time
        9
      when String
        Codec.string_size(value)
      when Bytes
        Codec.binary_size(value)
      else
        Codec.string_size(value.to_s)
      end
    end

    private def int_size(value) : Int32
      Int8::MIN <= value <= Int8::MAX ? 2 : 5
    end

    private def long_size(value) : Int32
      Int8::MIN <= value <= Int8::MAX ? 2 : 9
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def write_application_property_value(io, value) : Nil
      case value
      when Nil
        io.write_byte 0x40_u8
      when Bool
        Codec.write_bool(io, value)
      when Int8
        io.write_byte 0x51_u8
        io.write_byte value.to_u8!
      when UInt8
        io.write_byte 0x50_u8
        io.write_byte value
      when Int16
        io.write_byte 0x61_u8
        Codec.write_i16(io, value)
      when UInt16
        io.write_byte 0x60_u8
        Codec.write_u16(io, value)
      when Int32
        Codec.write_int(io, value)
      when UInt32
        Codec.write_uint(io, value)
      when Int64
        Codec.write_long(io, value)
      when Float32
        io.write_byte 0x72_u8
        Codec.write_f32(io, value)
      when Float64
        io.write_byte 0x82_u8
        Codec.write_f64(io, value)
      when Time
        io.write_byte 0x83_u8
        Codec.write_i64(io, value.to_unix_ms)
      when String
        Codec.write_string(io, value)
      when Bytes
        Codec.write_binary(io, value)
      else
        Codec.write_string(io, value.to_s)
      end
    end
  end
end
