require "./message_codec"
require "./frame"

module LavinMQ::AMQP10
  module TransferCodec
    extend self

    record TransferView,
      handle : UInt32,
      delivery_id : UInt32?,
      delivery_tag : Bytes?,
      message_format : UInt32?,
      settled : Bool,
      more : Bool,
      aborted : Bool

    record DispositionView,
      role : Role,
      first : UInt32,
      last : UInt32?,
      settled : Bool,
      outcome : Outcome?,
      state_present : Bool

    # ameba:disable Metrics/CyclomaticComplexity
    def read_transfer(reader : IO::Memory) : TransferView
      descriptor = MessageCodec.read_descriptor_code(reader)
      raise DecodeError.new("expected transfer") unless descriptor == Descriptor::TRANSFER
      count, end_pos = MessageCodec.read_list_header(reader)
      handle = nil
      delivery_id = nil
      delivery_tag = nil
      message_format = nil
      settled = false
      more = false
      aborted = false

      index = 0
      while index < count
        case index
        when 0
          handle = read_uint32_value(reader, "transfer handle")
        when 1
          delivery_id = read_optional_uint(reader, "transfer delivery-id")
        when 2
          delivery_tag = read_optional_binary(reader)
        when 3
          message_format = read_optional_uint(reader, "transfer message-format")
        when 4
          settled = read_optional_bool(reader) || false
        when 5
          more = read_optional_bool(reader) || false
        when 9
          aborted = read_optional_bool(reader) || false
        else
          MessageCodec.skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      handle_value = handle
      raise DecodeError.new("transfer missing handle") unless handle_value
      TransferView.new(handle_value, delivery_id, delivery_tag, message_format, settled, more, aborted)
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 transfer", cause: ex)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def read_disposition(reader : IO::Memory) : DispositionView
      descriptor = MessageCodec.read_descriptor_code(reader)
      raise DecodeError.new("expected disposition") unless descriptor == Descriptor::DISPOSITION
      count, end_pos = MessageCodec.read_list_header(reader)
      role = nil
      first = nil
      last = nil
      settled = false
      outcome = nil
      state_present = false

      index = 0
      while index < count
        case index
        when 0
          role = MessageCodec.read_bool_value(reader) ? Role::Receiver : Role::Sender
        when 1
          first = read_uint32_value(reader, "disposition first")
        when 2
          last = read_optional_uint(reader, "disposition last")
        when 3
          settled = read_optional_bool(reader) || false
        when 4
          state_present, outcome = read_state(reader)
        else
          MessageCodec.skip_value(reader)
        end
        index += 1
      end
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      role_value = role
      first_value = first
      raise DecodeError.new("disposition missing role") unless role_value
      raise DecodeError.new("disposition missing first") unless first_value
      DispositionView.new(role_value, first_value, last, settled, outcome, state_present)
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 disposition", cause: ex)
    end

    private def read_optional_uint(reader, field : String) : UInt32?
      return nil if peek_null(reader)
      read_uint32_value(reader, field)
    end

    private def read_uint32_value(reader, field : String) : UInt32
      value = MessageCodec.read_uint_value(reader)
      if value > UInt32::MAX
        raise DecodeError.new("#{field} #{value} exceeds uint range")
      end
      value.to_u32
    end

    private def read_optional_bool(reader) : Bool?
      return nil if peek_null(reader)
      MessageCodec.read_bool_value(reader)
    end

    private def read_optional_binary(reader) : Bytes?
      return nil if peek_null(reader)
      MessageCodec.read_binary_value(reader)
    end

    private def peek_null(reader) : Bool
      # Peek without consuming, rather than reading-then-rewinding, to avoid
      # complicating the hot path with a rewind.
      if reader.bytesize - reader.pos > 0
        # Null is a single byte and never followed by payload.
        slice = reader.peek
        if slice[0] == 0x40_u8
          reader.skip(1)
          return true
        end
      end
      false
    end

    # Returns whether a delivery-state field was present (vs. a null placeholder)
    # and, if it was a recognized terminal outcome, which one. A non-terminal
    # state (e.g. received) reports present=true with a nil outcome so the
    # caller does not mistake it for acceptance.
    private def read_state(reader) : Tuple(Bool, Outcome?)
      return {false, nil} if peek_null(reader)
      descriptor = MessageCodec.read_descriptor_code(reader)
      outcome = case descriptor
                when Descriptor::ACCEPTED then Outcome::Accepted
                when Descriptor::RELEASED then Outcome::Released
                when Descriptor::REJECTED then Outcome::Rejected
                when Descriptor::MODIFIED then Outcome::Modified
                else                           nil
                end
      MessageCodec.skip_value(reader)
      {true, outcome}
    end

    def write_disposition(io : IO, channel : UInt16, first : UInt32, outcome : Outcome, settled = true) : Nil
      state_size = outcome_size(outcome)
      fields_size = 1 + 1 + Codec.uint_size(first) + 1 + state_size
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      Codec.write_descriptor(io, Descriptor::DISPOSITION)
      Codec.write_list_header(io, fields_size, 5)
      io.write_byte 0x41_u8 # role receiver=true
      Codec.write_uint(io, first)
      io.write_byte 0x40_u8 # last
      io.write_byte(settled ? 0x41_u8 : 0x42_u8)
      write_outcome(io, outcome)
      io.flush
    end

    def write_flow(io : IO, channel : UInt16, next_incoming_id : UInt32, incoming_window : UInt32,
                   next_outgoing_id : UInt32, outgoing_window : UInt32, handle : UInt32? = nil,
                   delivery_count : UInt32? = nil, link_credit : UInt32? = nil, drain : Bool = false) : UInt64
      fields_size = Codec.uint_size(next_incoming_id) + Codec.uint_size(incoming_window) +
                    Codec.uint_size(next_outgoing_id) + Codec.uint_size(outgoing_window)
      fields_count = 4
      if handle
        fields_size += Codec.uint_size(handle) + Codec.uint_size(delivery_count || 0_u32) + Codec.uint_size(link_credit || 0_u32)
        fields_count = 7
        if drain
          # available (field 7) is encoded as null, drain (field 8) as a boolean
          fields_size += 1 + 1
          fields_count = 9
        end
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      Codec.write_descriptor(io, Descriptor::FLOW)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, next_incoming_id)
      Codec.write_uint(io, incoming_window)
      Codec.write_uint(io, next_outgoing_id)
      Codec.write_uint(io, outgoing_window)
      if handle
        Codec.write_uint(io, handle)
        Codec.write_uint(io, delivery_count || 0_u32)
        Codec.write_uint(io, link_credit || 0_u32)
        if drain
          io.write_byte 0x40_u8 # available: null
          Codec.write_bool(io, true)
        end
      end
      io.flush
      frame_size.to_u64
    end

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
      transfer_size = transfer_performative_size(handle, delivery_id, delivery_tag, false, settled)
      frame_size = 8_u64 + transfer_size.to_u64 + message_size

      if frame_size <= max
        FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
        write_transfer_performative(io, handle, delivery_id, delivery_tag, false, settled)
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
        app_sec = 3 + map_header_size(app_fields, headers.size * 2) + app_fields
      else
        app_fields = 0
        app_sec = 0
      end
      data_sec = 3 + binary_header_size(msg.bodysize)
      total = header_sec + props_sec + app_sec + data_sec
      SectionSizes.new(total, header_count, header_fields, props_count, props_fields, app_fields)
    end

    private def write_message_sections_prefix(io, msg : BytesMessage, sizes : SectionSizes? = nil) : Nil
      sizes ||= compute_section_sizes(msg)
      write_header_section(io, msg.properties, sizes.header_count, sizes.header_fields)
      write_properties_section(io, msg.properties, sizes.props_count, sizes.props_fields)
      write_application_properties_section(io, msg.properties.headers, sizes.app_fields)
      Codec.write_descriptor(io, Descriptor::DATA)
      write_binary_header(io, msg.bodysize)
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
                          transfer_performative_size(handle, delivery_id, delivery_tag, true, settled)
                        else
                          final_size = continuation_transfer_performative_size(handle, false)
                          if 8_u64 + final_size.to_u64 + remaining.to_u64 <= max
                            more = false
                            final_size
                          else
                            continuation_transfer_performative_size(handle, true)
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
          write_transfer_performative(io, handle, delivery_id, delivery_tag, true, settled)
          first = false
        else
          write_continuation_transfer_performative(io, handle, more)
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

    def write_transfer_performative(io, handle, delivery_id, delivery_tag, more, settled) : Nil
      # fields: handle(0) delivery-id(1) delivery-tag(2) message-format(3) settled(4) more(5)
      fields_count = more ? 6 : (settled ? 5 : 4)
      fields_size = Codec.uint_size(handle) + Codec.uint_size(delivery_id) + binary_size(delivery_tag) + 1
      fields_size += 1 if settled || more # settled field (bool or null)
      fields_size += 1 if more            # more field
      Codec.write_descriptor(io, Descriptor::TRANSFER)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      Codec.write_uint(io, delivery_id)
      Codec.write_binary(io, delivery_tag)
      io.write_byte 0x43_u8 # message-format = 0
      if more
        io.write_byte(settled ? 0x41_u8 : 0x40_u8) # settled (null when unsettled)
        io.write_byte 0x41_u8                      # more = true
      elsif settled
        io.write_byte 0x41_u8 # settled = true
      end
    end

    def transfer_performative_size(handle, delivery_id, delivery_tag, more, settled) : Int32
      fields_size = Codec.uint_size(handle) + Codec.uint_size(delivery_id) + binary_size(delivery_tag) + 1
      fields_size += 1 if settled || more
      fields_size += 1 if more
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    private def write_continuation_transfer_performative(io, handle, more) : Nil
      fields_count = more ? 6 : 1
      fields_size = Codec.uint_size(handle)
      fields_size += 5 if more
      Codec.write_descriptor(io, Descriptor::TRANSFER)
      Codec.write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      if more
        4.times { io.write_byte 0x40_u8 }
        io.write_byte 0x41_u8
      end
    end

    private def continuation_transfer_performative_size(handle, more) : Int32
      fields_size = Codec.uint_size(handle)
      fields_size += 5 if more
      3 + Codec.list_header_size(fields_size) + fields_size
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
            Codec.write_i64(io, ts * 1000_i64)
          else
            io.write_byte 0x40_u8
          end
        else
          io.write_byte 0x40_u8
        end
        index += 1
      end
    end

    private def write_application_properties_section(io, headers : LavinMQ::AMQP::Table?, fields_size : Int32) : Nil
      return unless headers
      return if headers.empty?
      Codec.write_descriptor(io, Descriptor::APPLICATION_PROPERTIES)
      write_map_header(io, fields_size, headers.size * 2)
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

    private def write_map_header(io, fields_size : Int32, count : Int32) : Nil
      Codec.write_compound_header(io, 0xc1_u8, 0xd1_u8, fields_size, count)
    end

    private def map_header_size(fields_size, count) : Int32
      fields_size + 1 <= UInt8::MAX && count <= UInt8::MAX ? 3 : 9
    end

    private def binary_size(value : Bytes) : Int32
      binary_header_size(value.bytesize.to_u64) + value.bytesize
    end

    private def binary_header_size(size : UInt64) : Int32
      size <= UInt8::MAX ? 2 : 5
    end

    private def write_binary_header(io, size : UInt64) : Nil
      if size <= UInt8::MAX
        io.write_byte 0xa0_u8
        io.write_byte size.to_u8
      else
        io.write_byte 0xb0_u8
        Codec.write_u32(io, size.to_u32)
      end
    end

    private def nullable_binary_string_size(value : String?) : Int32
      value ? binary_header_size(value.bytesize.to_u64) + value.bytesize : 1
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
        binary_size(value)
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

    # Every outcome we emit is a descriptor followed by an empty list (list0).
    OUTCOME_SIZE = 3 + 1

    private def outcome_size(outcome : Outcome) : Int32
      OUTCOME_SIZE
    end

    private def write_outcome(io, outcome : Outcome) : Nil
      case outcome
      in .accepted?
        Codec.write_descriptor(io, Descriptor::ACCEPTED)
      in .released?
        Codec.write_descriptor(io, Descriptor::RELEASED)
      in .rejected?
        Codec.write_descriptor(io, Descriptor::REJECTED)
      in .modified?
        Codec.write_descriptor(io, Descriptor::MODIFIED)
      end
      io.write_byte 0x45_u8
    end
  end
end
