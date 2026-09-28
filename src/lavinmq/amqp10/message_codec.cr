require "uuid"
require "../amqp"
require "../message"
require "./protocol"

module LavinMQ::AMQP10
  module MessageCodec
    extend self

    EMPTY_BODY = Bytes.empty

    # Messages are stored in the 0-9-1 format. AMQP 1.0 details that format
    # has no field for are kept in headers with this prefix; they are never
    # delivered as application-properties, nor accepted from a publisher.
    INTERNAL_HEADER_PREFIX = "x-amqp10-"
    # Set when the body was an amqp-value section rather than data sections:
    # "string" or "binary" (the body holds the value's bytes) or "value" (the
    # body holds the value in its AMQP 1.0 encoding).
    BODY_TYPE_HEADER = "x-amqp10-body-type"
    # Set when message-id or correlation-id was not a string: "ulong",
    # "uuid" or "binary". The 0-9-1 property holds the id as a string.
    MESSAGE_ID_TYPE_HEADER     = "x-amqp10-message-id-type"
    CORRELATION_ID_TYPE_HEADER = "x-amqp10-correlation-id-type"
    # The message-annotations section's map, in its AMQP 1.0 encoding.
    MESSAGE_ANNOTATIONS_HEADER = "x-amqp10-message-annotations"

    record Incoming, properties : LavinMQ::AMQP::Properties, body : Bytes, to : String?

    def decode(reader : IO::Memory) : Incoming
      props = LavinMQ::AMQP::Properties.new
      to = nil
      body = EMPTY_BODY
      body_io : IO::Memory? = nil
      body_type : String? = nil
      message_id_type : String? = nil
      correlation_id_type : String? = nil
      annotations : Bytes? = nil

      until reader.pos >= reader.bytesize
        descriptor = Codec.read_descriptor_code(reader)
        case descriptor
        when Descriptor::HEADER
          props = read_header(reader, props)
        when Descriptor::MESSAGE_ANNOTATIONS
          annotations = read_annotations(reader)
        when Descriptor::DELIVERY_ANNOTATIONS, Descriptor::FOOTER
          # delivery-annotations are for the next hop only; footers carry
          # hashes and signatures of the bare message, not stored either.
          Codec.skip_value(reader)
        when Descriptor::PROPERTIES
          props, to, message_id_type, correlation_id_type = read_properties(reader, props)
        when Descriptor::APPLICATION_PROPERTIES
          props = read_application_properties(reader, props)
        when Descriptor::DATA
          body, body_io = append_data_section(body, body_io, Codec.read_binary_value(reader))
        when Descriptor::AMQP_VALUE
          body_io = nil
          body, body_type = read_amqp_value_body(reader)
        else
          Codec.skip_value(reader)
        end
      end

      if chunks = body_io
        body = chunks.to_slice
      end
      # Applied last: the application-properties section replaces the headers.
      props = with_internal_headers(props, body_type, message_id_type, correlation_id_type, annotations)
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

    # Returns the body and its BODY_TYPE_HEADER value. Strings and binaries
    # are stored as their bytes, so 0-9-1 consumers see the plain payload;
    # any other value (lists, maps, numbers, symbols, null) has no 0-9-1
    # equivalent and is stored in its AMQP 1.0 encoding.
    private def read_amqp_value_body(reader : IO::Memory) : Tuple(Bytes, String)
      start = reader.pos
      case code = Codec.read_byte(reader)
      when 0xa0 then {Codec.read_slice(reader, Codec.read_byte(reader).to_i), "binary"}
      when 0xb0 then {Codec.read_slice(reader, Codec.read_size32(reader, "binary32")), "binary"}
      when 0xa1 then {Codec.read_slice(reader, Codec.read_byte(reader).to_i), "string"}
      when 0xb1 then {Codec.read_slice(reader, Codec.read_size32(reader, "string32")), "string"}
      else
        Codec.skip_value_payload(reader, code)
        {Codec.slice_from(reader, start), "value"}
      end
    end

    # The encoded annotations map, copied out of the frame buffer, or nil for
    # a null or empty map.
    private def read_annotations(reader : IO::Memory) : Bytes?
      start = reader.pos
      if reader.peek.try(&.first?) == 0x40_u8 # null
        reader.skip(1)
        return
      end
      count, end_pos = Codec.read_map_header(reader)
      reader.pos = end_pos
      return if count.zero?
      Codec.slice_from(reader, start).dup
    end

    # Properties is a struct: returns the updated copy.
    private def with_internal_headers(props, body_type, message_id_type, correlation_id_type, annotations) : LavinMQ::AMQP::Properties
      return props unless body_type || message_id_type || correlation_id_type || annotations
      headers = props.headers || LavinMQ::AMQP::Table.new
      headers[BODY_TYPE_HEADER] = body_type if body_type
      headers[MESSAGE_ANNOTATIONS_HEADER] = annotations if annotations
      headers[MESSAGE_ID_TYPE_HEADER] = message_id_type if message_id_type
      headers[CORRELATION_ID_TYPE_HEADER] = correlation_id_type if correlation_id_type
      props.headers = headers
      props
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
          headers[key] = value if key && !key.starts_with?(INTERNAL_HEADER_PREFIX)
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

    # Returns the properties, the to address, and the message-id and
    # correlation-id types when they were not strings.
    # ameba:disable Metrics/CyclomaticComplexity
    private def read_properties(reader, props) : Tuple(LavinMQ::AMQP::Properties, String?, String?, String?)
      count, end_pos = Codec.read_list_header(reader)
      to = nil
      message_id_type = nil
      correlation_id_type = nil
      index = 0
      while index < count
        case index
        when 0
          message_id, message_id_type = read_message_id(reader)
          props.message_id = shortstr(message_id)
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
          correlation_id, correlation_id_type = read_message_id(reader)
          props.correlation_id = shortstr(correlation_id)
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
      {props, to, message_id_type, correlation_id_type}
    end

    # Returns the id as a string, and its type unless it was a string.
    # ameba:disable Metrics/CyclomaticComplexity
    private def read_message_id(reader) : Tuple(String?, String?)
      case code = Codec.read_byte(reader)
      when 0x40
        {nil, nil}
      when 0xa1, 0xa3
        {reader.read_string(Codec.read_byte(reader).to_i), nil}
      when 0xb1, 0xb3
        {reader.read_string(Codec.read_size32(reader, "string32")), nil}
      when 0x43 # uint is not a valid id type; kept as a string
        {"0", nil}
      when 0x52
        {Codec.read_byte(reader).to_s, nil}
      when 0x70
        {reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian).to_s, nil}
      when 0x44
        {"0", "ulong"}
      when 0x53
        {Codec.read_byte(reader).to_s, "ulong"}
      when 0x80
        {reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian).to_s, "ulong"}
      when 0xa0
        {reader.read_string(Codec.read_byte(reader).to_i), "binary"}
      when 0xb0
        {reader.read_string(Codec.read_size32(reader, "binary32")), "binary"}
      when 0x98
        {read_uuid_value(reader), "uuid"}
      else
        Codec.skip_value_payload(reader, code)
        {nil, nil}
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
      delivery_count : UInt32,
      header_count : Int32,
      header_fields : Int32,
      annotations : Bytes?,
      props_count : Int32,
      props_fields : Int32,
      message_id_kind : IdKind,
      correlation_id_kind : IdKind,
      app_count : Int32,
      app_fields : Int32,
      body_kind : BodyKind

    # How message-id and correlation-id are encoded, from their type headers.
    private enum IdKind
      String
      ULong
      UUID
      Binary
    end

    # The section the stored body is delivered in, from BODY_TYPE_HEADER.
    private enum BodyKind
      Data
      String
      Binary
      Value
    end

    # Returns the number of AMQP 1.0 transfer frames written.
    def write_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                       delivery_tag : Bytes, msg : BytesMessage, max_frame_size = UInt32::MAX,
                       settled = false, redelivered = false) : Tuple(UInt64, UInt32)
      if msg.bodysize > UInt32::MAX
        raise ProtocolError.new("message too large for AMQP 1.0 data section")
      end

      sizes = compute_section_sizes(msg, redelivered)
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

      write_fragmented_transfer(io, channel, handle, delivery_id, delivery_tag, msg, sizes, max, settled)
    end

    private def compute_section_sizes(msg : BytesMessage, redelivered : Bool) : SectionSizes
      props = msg.properties
      delivery_count = delivery_count(props, redelivered)
      header_count = header_field_count(props, delivery_count)
      header_fields = header_count.zero? ? 0 : header_fields_size(props, header_count, delivery_count)
      header_sec = header_count.zero? ? 0 : 3 + Codec.list_header_size(header_fields) + header_fields
      annotations = message_annotations(props.headers)
      annotations_sec = annotations ? 3 + annotations.bytesize : 0
      props_count = properties_field_count(props)
      headers = props.headers
      message_id_kind = id_kind(headers, MESSAGE_ID_TYPE_HEADER, props.message_id)
      correlation_id_kind = id_kind(headers, CORRELATION_ID_TYPE_HEADER, props.correlation_id)
      props_fields = props_count.zero? ? 0 : properties_fields_size(props, props_count, message_id_kind, correlation_id_kind)
      props_sec = props_count.zero? ? 0 : 3 + Codec.list_header_size(props_fields) + props_fields
      app_count, app_fields = headers ? application_properties_fields_size(headers) : {0, 0}
      app_sec = app_count.zero? ? 0 : 3 + Codec.map_header_size(app_fields, app_count * 2) + app_fields
      body_kind = body_kind(headers)
      body_sec = 3 + (body_kind.value? ? 0 : Codec.binary_header_size(msg.bodysize))
      total = header_sec + annotations_sec + props_sec + app_sec + body_sec
      SectionSizes.new(total, delivery_count, header_count, header_fields, annotations, props_count, props_fields,
        message_id_kind, correlation_id_kind, app_count, app_fields, body_kind)
    end

    # The stored message-annotations map, if any, as a view into the headers.
    # Anything but one complete encoded map (e.g. a header set by a 0-9-1
    # publisher) is not delivered.
    def message_annotations(headers : LavinMQ::AMQP::Table?) : Bytes?
      return unless headers && headers.has_key?(MESSAGE_ANNOTATIONS_HEADER)
      bytes = headers[MESSAGE_ANNOTATIONS_HEADER]?.as?(Bytes) || return
      bytes if encoded_map?(bytes)
    end

    # Merges two encoded annotation maps: entries of `update` replace those of
    # `base` with the same key. Works on the encoded entries, so values of
    # any type are carried over unchanged.
    def merge_annotations(base : Bytes?, update : Bytes) : Bytes
      update_entries = map_entries(update)
      return update.dup unless base
      replaced = update_entries.map { |key, _| key }.to_set
      entries = map_entries(base).reject! { |key, _| replaced.includes?(key) }
      entries.concat(update_entries)
      fields_size = entries.sum(0) { |_, entry| entry.bytesize }
      io = IO::Memory.new
      Codec.write_map_header(io, fields_size, entries.size * 2)
      entries.each { |_, entry| io.write entry }
      io.to_slice
    end

    # The entries of an encoded map as {key identity, encoded key and value}.
    private def map_entries(map : Bytes) : Array(Tuple(String, Bytes))
      reader = IO::Memory.new(map)
      count, end_pos = Codec.read_map_header(reader)
      entries = Array(Tuple(String, Bytes)).new(count // 2)
      (count // 2).times do
        start = reader.pos
        key = annotation_key(reader)
        Codec.skip_value(reader)
        entries << {key, map[start, reader.pos - start]}
      end
      raise DecodeError.new("annotations map entries overran its size") if reader.pos > end_pos
      entries
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated annotations map", cause: ex)
    end

    # Annotation keys are symbols or ulongs; compare them by value so that,
    # e.g., a sym8 and a sym32 encoding of the same symbol are the same key.
    private def annotation_key(reader : IO::Memory) : String
      start = reader.pos
      case Codec.read_byte(reader)
      when 0xa1, 0xa3, 0xb1, 0xb3
        reader.pos = start
        "s:#{Codec.read_string_value(reader)}"
      when 0x44, 0x53, 0x80
        reader.pos = start
        "u:#{Codec.read_uint_value(reader)}"
      else
        reader.pos = start
        Codec.skip_value(reader)
        "r:#{Codec.slice_from(reader, start).hexstring}"
      end
    end

    private def encoded_map?(bytes : Bytes) : Bool
      case bytes[0]?
      when 0xc1
        bytes.size >= 3 && bytes[1].to_i + 2 == bytes.size
      when 0xd1
        bytes.size >= 9 && IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[1, 4]).to_u64 + 5 == bytes.size
      else
        false
      end
    end

    # Falls back to a string when the stored id does not parse as its type,
    # e.g. a type header set by a 0-9-1 publisher.
    private def id_kind(headers : LavinMQ::AMQP::Table?, key : String, id : String?) : IdKind
      return IdKind::String unless id && headers && headers.has_key?(key)
      if headers.has_entry?(key, "ulong")
        id.to_u64? ? IdKind::ULong : IdKind::String
      elsif headers.has_entry?(key, "uuid")
        UUID.parse?(id) ? IdKind::UUID : IdKind::String
      elsif headers.has_entry?(key, "binary")
        IdKind::Binary
      else
        IdKind::String
      end
    end

    private def id_size(id : String?, kind : IdKind) : Int32
      return 1 unless id
      case kind
      in .string? then Codec.string_size(id)
      in .binary? then Codec.binary_header_size(id.bytesize.to_u64) + id.bytesize
      in .uuid?   then 17
      in .u_long?
        value = id.to_u64
        value.zero? ? 1 : value <= UInt8::MAX ? 2 : 9
      end
    end

    private def write_id(io, id : String?, kind : IdKind) : Nil
      return io.write_byte(0x40_u8) unless id
      case kind
      in .string? then Codec.write_string(io, id)
      in .binary? then Codec.write_binary(io, id.to_slice)
      in .u_long? then Codec.write_ulong(io, id.to_u64)
      in .uuid?
        # id_kind only picks UUID when the id parses as one
        if uuid = UUID.parse?(id)
          io.write_byte 0x98_u8
          io.write uuid.bytes.to_slice
        else
          Codec.write_string(io, id)
        end
      end
    end

    private def body_kind(headers : LavinMQ::AMQP::Table?) : BodyKind
      return BodyKind::Data unless headers
      return BodyKind::Data unless headers.has_key?(BODY_TYPE_HEADER)
      if headers.has_entry?(BODY_TYPE_HEADER, "string")
        BodyKind::String
      elsif headers.has_entry?(BODY_TYPE_HEADER, "binary")
        BodyKind::Binary
      elsif headers.has_entry?(BODY_TYPE_HEADER, "value")
        BodyKind::Value
      else
        BodyKind::Data
      end
    end

    # The descriptor and value constructor preceding the stored body bytes;
    # a "value" body already is a complete encoded value.
    private def write_body_section_header(io, kind : BodyKind, bodysize : UInt64) : Nil
      case kind
      in .data?
        Codec.write_descriptor(io, Descriptor::DATA)
        Codec.write_binary_header(io, bodysize)
      in .binary?
        Codec.write_descriptor(io, Descriptor::AMQP_VALUE)
        Codec.write_binary_header(io, bodysize)
      in .string?
        Codec.write_descriptor(io, Descriptor::AMQP_VALUE)
        if bodysize <= UInt8::MAX
          io.write_byte 0xa1_u8
          io.write_byte bodysize.to_u8
        else
          io.write_byte 0xb1_u8
          Codec.write_u32(io, bodysize.to_u32)
        end
      in .value?
        Codec.write_descriptor(io, Descriptor::AMQP_VALUE)
      end
    end

    private def write_message_sections_prefix(io, msg : BytesMessage, sizes : SectionSizes) : Nil
      write_header_section(io, msg.properties, sizes)
      if annotations = sizes.annotations
        Codec.write_descriptor(io, Descriptor::MESSAGE_ANNOTATIONS)
        io.write annotations
      end
      write_properties_section(io, msg.properties, sizes)
      write_application_properties_section(io, msg.properties.headers, sizes.app_count, sizes.app_fields)
      write_body_section_header(io, sizes.body_kind, msg.bodysize)
    end

    private def write_fragmented_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                                          delivery_tag : Bytes, msg : BytesMessage, sizes : SectionSizes,
                                          max : UInt64, settled : Bool) : Tuple(UInt64, UInt32)
      prefix_size = sizes.total
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
        prefix_offset, body_offset = write_message_bytes(io, msg, sizes, prefix_offset, body, body_offset,
          chunk_size, prefix_writer)
        written += frame_size
        frames += 1
      end

      {written, frames}
    end

    private def write_message_bytes(io, msg, sizes, prefix_offset, body, body_offset, count, prefix_writer)
      prefix_size = sizes.total
      remaining = count
      if prefix_offset < prefix_size
        prefix_count = Math.min(remaining, prefix_size - prefix_offset)
        prefix_writer.reset(prefix_offset, prefix_count)
        write_message_sections_prefix(prefix_writer, msg, sizes)
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

    # The number of earlier delivery attempts: the queue's x-delivery-count
    # when it tracks one (delivery-limit), otherwise at least 1 for a
    # redelivered message.
    private def delivery_count(props, redelivered : Bool) : UInt32
      if (headers = props.headers) && headers.has_key?("x-delivery-count")
        case count = headers["x-delivery-count"]?
        when Int
          return count.clamp(0, UInt32::MAX).to_u32 if count > 0
        end
      end
      redelivered ? 1_u32 : 0_u32
    end

    private def header_field_count(props, delivery_count : UInt32) : Int32
      count = 0
      count = 1 if props.delivery_mode
      count = 2 if props.priority
      count = 3 if header_ttl(props)
      count = 5 if delivery_count > 0
      count
    end

    private def header_fields_size(props, count : Int32, delivery_count : UInt32) : Int32
      size = 0
      index = 0
      while index < count
        size += case index
                when 0 then 1                      # durable bool
                when 1 then props.priority ? 2 : 1 # ubyte or null
                when 2
                  (ttl = header_ttl(props)) ? Codec.uint_size(ttl) : 1
                when 4 then Codec.uint_size(delivery_count)
                else        1 # first-acquirer: null
                end
        index += 1
      end
      size
    end

    private def write_header_section(io, props, sizes : SectionSizes) : Nil
      count = sizes.header_count
      return if count.zero?
      Codec.write_descriptor(io, Descriptor::HEADER)
      Codec.write_list_header(io, sizes.header_fields, count)
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
        when 3 then io.write_byte 0x40_u8 # first-acquirer: null
        when 4 then Codec.write_uint(io, sizes.delivery_count.to_u64)
        end
        index += 1
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def write_properties_section(io, props, sizes : SectionSizes) : Nil
      count = sizes.props_count
      return if count.zero?
      Codec.write_descriptor(io, Descriptor::PROPERTIES)
      Codec.write_list_header(io, sizes.props_fields, count)
      index = 0
      while index < count
        case index
        when 0 then write_id(io, props.message_id, sizes.message_id_kind)
        when 1 then write_nullable_binary_string(io, props.user_id)
        when 2 then io.write_byte 0x40_u8
        when 3 then Codec.write_nullable_string(io, props.type)
        when 4 then Codec.write_nullable_string(io, props.reply_to)
        when 5 then write_id(io, props.correlation_id, sizes.correlation_id_kind)
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

    private def write_application_properties_section(io, headers : LavinMQ::AMQP::Table?, count : Int32, fields_size : Int32) : Nil
      return unless headers
      return if count.zero?
      Codec.write_descriptor(io, Descriptor::APPLICATION_PROPERTIES)
      Codec.write_map_header(io, fields_size, count * 2)
      headers.each do |key, value|
        next if key.starts_with?(INTERNAL_HEADER_PREFIX)
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

    private def properties_fields_size(props, count, message_id_kind : IdKind, correlation_id_kind : IdKind) : Int32
      size = 0
      index = 0
      while index < count
        size += case index
                when 0 then id_size(props.message_id, message_id_kind)
                when 1 then nullable_binary_string_size(props.user_id)
                when 3 then Codec.nullable_string_size(props.type)
                when 4 then Codec.nullable_string_size(props.reply_to)
                when 5 then id_size(props.correlation_id, correlation_id_kind)
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

    # The number of headers delivered as application-properties and their
    # encoded size; internal headers are left out.
    private def application_properties_fields_size(headers : LavinMQ::AMQP::Table) : Tuple(Int32, Int32)
      count = 0
      size = 0
      headers.each do |key, value|
        next if key.starts_with?(INTERNAL_HEADER_PREFIX)
        count += 1
        size += Codec.string_size(key)
        size += application_property_value_size(value)
      end
      {count, size}
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
