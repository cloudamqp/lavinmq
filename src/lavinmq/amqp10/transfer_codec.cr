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
      outcome : Outcome?

    # ameba:disable Metrics/CyclomaticComplexity
    def read_transfer(reader : SliceReader) : TransferView
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
    end

    def read_disposition(reader : SliceReader) : DispositionView
      descriptor = MessageCodec.read_descriptor_code(reader)
      raise DecodeError.new("expected disposition") unless descriptor == Descriptor::DISPOSITION
      count, end_pos = MessageCodec.read_list_header(reader)
      role = nil
      first = nil
      last = nil
      settled = false
      outcome = nil

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
          outcome = read_outcome(reader)
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
      DispositionView.new(role_value, first_value, last, settled, outcome)
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
      # SliceReader is cheap to copy; peek by consuming and rewinding via a copy
      # is deliberately avoided because rewinding would complicate the hot path.
      if reader.remaining > 0
        # Null is a single byte and never followed by payload.
        slice = reader.remaining_slice
        if slice[0] == 0x40_u8
          reader.skip(1)
          return true
        end
      end
      false
    end

    private def read_outcome(reader) : Outcome?
      return nil if peek_null(reader)
      descriptor = MessageCodec.read_descriptor_code(reader)
      case descriptor
      when Descriptor::ACCEPTED
        MessageCodec.skip_value(reader)
        Outcome::Accepted
      when Descriptor::RELEASED
        MessageCodec.skip_value(reader)
        Outcome::Released
      when Descriptor::REJECTED
        MessageCodec.skip_value(reader)
        Outcome::Rejected
      when Descriptor::MODIFIED
        MessageCodec.skip_value(reader)
        Outcome::Modified
      else
        MessageCodec.skip_value(reader)
        nil
      end
    end

    def write_disposition(io : IO, channel : UInt16, first : UInt32, outcome : Outcome, settled = true) : Nil
      state_size = outcome_size(outcome)
      fields_size = 1 + 1 + uint_size(first) + 1 + state_size
      frame_size = 8 + 3 + list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      write_descriptor(io, Descriptor::DISPOSITION)
      write_list_header(io, fields_size, 5)
      io.write_byte 0x41_u8 # role receiver=true
      Codec.write_uint(io, first)
      io.write_byte 0x40_u8 # last
      io.write_byte(settled ? 0x41_u8 : 0x42_u8)
      write_outcome(io, outcome)
      io.flush
    end

    def write_flow(io : IO, channel : UInt16, next_incoming_id : UInt32, incoming_window : UInt32,
                   next_outgoing_id : UInt32, outgoing_window : UInt32, handle : UInt32? = nil,
                   delivery_count : UInt32? = nil, link_credit : UInt32? = nil) : UInt64
      fields_size = uint_size(next_incoming_id) + uint_size(incoming_window) +
                    uint_size(next_outgoing_id) + uint_size(outgoing_window)
      fields_count = 4
      if handle
        fields_size += uint_size(handle) + uint_size(delivery_count || 0_u32) + uint_size(link_credit || 0_u32)
        fields_count = 7
      end
      frame_size = 8 + 3 + list_header_size(fields_size) + fields_size
      FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
      write_descriptor(io, Descriptor::FLOW)
      write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, next_incoming_id)
      Codec.write_uint(io, incoming_window)
      Codec.write_uint(io, next_outgoing_id)
      Codec.write_uint(io, outgoing_window)
      if handle
        Codec.write_uint(io, handle)
        Codec.write_uint(io, delivery_count || 0_u32)
        Codec.write_uint(io, link_credit || 0_u32)
      end
      io.flush
      frame_size.to_u64
    end

    def write_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                       delivery_tag : Bytes, msg : BytesMessage, max_frame_size = UInt32::MAX) : UInt64
      if msg.bodysize > UInt32::MAX
        raise ProtocolError.new("message too large for AMQP 1.0 data section")
      end

      prefix_size = message_sections_prefix_size(msg)
      message_size = prefix_size.to_u64 + msg.bodysize
      max = effective_max_frame_size(max_frame_size)
      transfer_size = transfer_performative_size(handle, delivery_id, delivery_tag, false)
      frame_size = 8_u64 + transfer_size.to_u64 + message_size

      if frame_size <= max
        FrameWriter.write_frame_header(io, frame_size.to_u32, AMQP_FRAME_TYPE, channel)
        write_transfer_performative(io, handle, delivery_id, delivery_tag, false)
        write_message_sections_prefix(io, msg)
        io.write msg.body
        io.flush
        return frame_size
      end

      write_fragmented_transfer(io, channel, handle, delivery_id, delivery_tag, msg, prefix_size, max)
    end

    private def message_sections_prefix_size(msg : BytesMessage) : Int32
      properties_section_size(msg.properties) +
        application_properties_section_size(msg.properties.headers) +
        3 + binary_header_size(msg.bodysize)
    end

    private def write_message_sections_prefix(io, msg : BytesMessage) : Nil
      write_properties_section(io, msg.properties)
      write_application_properties_section(io, msg.properties.headers)
      write_descriptor(io, Descriptor::DATA)
      write_binary_header(io, msg.bodysize)
    end

    private def write_fragmented_transfer(io : IO, channel : UInt16, handle : UInt32, delivery_id : UInt32,
                                          delivery_tag : Bytes, msg : BytesMessage, prefix_size : Int32,
                                          max : UInt64) : UInt64
      prefix_offset = 0
      body_offset = 0
      body = msg.body
      first = true
      written = 0_u64
      prefix_writer = PrefixRangeIO.new(io)

      loop do
        remaining = prefix_size - prefix_offset + body.bytesize - body_offset
        break if remaining <= 0

        more = true
        transfer_size = if first
                          transfer_performative_size(handle, delivery_id, delivery_tag, true)
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
          write_transfer_performative(io, handle, delivery_id, delivery_tag, true)
          first = false
        else
          write_continuation_transfer_performative(io, handle, more)
        end
        prefix_offset, body_offset = write_message_bytes(io, msg, prefix_size, prefix_offset, body, body_offset,
          chunk_size, prefix_writer)
        written += frame_size
      end

      io.flush
      written
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

    def write_transfer_performative(io, handle, delivery_id, delivery_tag, more) : Nil
      fields_count = more ? 6 : 4
      fields_size = uint_size(handle) + uint_size(delivery_id) + binary_size(delivery_tag) + 1
      fields_size += 1 + 1 if more # settled null, more bool
      write_descriptor(io, Descriptor::TRANSFER)
      write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      Codec.write_uint(io, delivery_id)
      Codec.write_binary(io, delivery_tag)
      io.write_byte 0x43_u8 # message-format = 0
      if more
        io.write_byte 0x40_u8
        io.write_byte 0x41_u8
      end
    end

    def transfer_performative_size(handle, delivery_id, delivery_tag, more) : Int32
      fields_size = uint_size(handle) + uint_size(delivery_id) + binary_size(delivery_tag) + 1
      fields_size += 2 if more
      3 + list_header_size(fields_size) + fields_size
    end

    private def write_continuation_transfer_performative(io, handle, more) : Nil
      fields_count = more ? 6 : 1
      fields_size = uint_size(handle)
      fields_size += 5 if more
      write_descriptor(io, Descriptor::TRANSFER)
      write_list_header(io, fields_size, fields_count)
      Codec.write_uint(io, handle)
      if more
        4.times { io.write_byte 0x40_u8 }
        io.write_byte 0x41_u8
      end
    end

    private def continuation_transfer_performative_size(handle, more) : Int32
      fields_size = uint_size(handle)
      fields_size += 5 if more
      3 + list_header_size(fields_size) + fields_size
    end

    private def properties_section_size(props) : Int32
      count = properties_field_count(props)
      return 0 if count.zero?
      fields_size = properties_fields_size(props, count)
      3 + list_header_size(fields_size) + fields_size
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def write_properties_section(io, props) : Nil
      count = properties_field_count(props)
      return if count.zero?
      fields_size = properties_fields_size(props, count)
      write_descriptor(io, Descriptor::PROPERTIES)
      write_list_header(io, fields_size, count)
      index = 0
      while index < count
        case index
        when 0 then write_nullable_string(io, props.message_id)
        when 1 then write_nullable_binary_string(io, props.user_id)
        when 2 then io.write_byte 0x40_u8
        when 3 then write_nullable_string(io, props.type)
        when 4 then write_nullable_string(io, props.reply_to)
        when 5 then write_nullable_string(io, props.correlation_id)
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

    private def application_properties_section_size(headers : LavinMQ::AMQP::Table?) : Int32
      return 0 unless headers
      return 0 if headers.empty?
      fields_size = application_properties_fields_size(headers)
      3 + map_header_size(fields_size, headers.size * 2) + fields_size
    end

    private def write_application_properties_section(io, headers : LavinMQ::AMQP::Table?) : Nil
      return unless headers
      return if headers.empty?
      fields_size = application_properties_fields_size(headers)
      write_descriptor(io, Descriptor::APPLICATION_PROPERTIES)
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
                when 0 then nullable_string_size(props.message_id)
                when 1 then nullable_binary_string_size(props.user_id)
                when 3 then nullable_string_size(props.type)
                when 4 then nullable_string_size(props.reply_to)
                when 5 then nullable_string_size(props.correlation_id)
                when 6 then nullable_symbol_size(props.content_type)
                when 7 then nullable_symbol_size(props.content_encoding)
                when 9 then props.timestamp_raw ? 9 : 1
                else        1
                end
        index += 1
      end
      size
    end

    private def write_descriptor(io, code : UInt64) : Nil
      io.write_byte 0x00_u8
      Codec.write_ulong(io, code)
    end

    private def write_list_header(io, fields_size : Int32, count : Int32) : Nil
      write_compound_header(io, 0xc0_u8, 0xd0_u8, fields_size, count)
    end

    private def write_map_header(io, fields_size : Int32, count : Int32) : Nil
      write_compound_header(io, 0xc1_u8, 0xd1_u8, fields_size, count)
    end

    private def write_compound_header(io, code8 : UInt8, code32 : UInt8, fields_size : Int32, count : Int32) : Nil
      if fields_size + 1 <= UInt8::MAX && count <= UInt8::MAX
        io.write_byte code8
        io.write_byte((fields_size + 1).to_u8)
        io.write_byte count.to_u8
      else
        io.write_byte code32
        Codec.write_u32(io, (fields_size + 4).to_u32)
        Codec.write_u32(io, count.to_u32)
      end
    end

    private def list_header_size(fields_size) : Int32
      fields_size + 1 <= UInt8::MAX ? 3 : 9
    end

    private def map_header_size(fields_size, count) : Int32
      fields_size + 1 <= UInt8::MAX && count <= UInt8::MAX ? 3 : 9
    end

    private def uint_size(value) : Int32
      value = value.to_u64
      value.zero? ? 1 : value <= UInt8::MAX ? 2 : 5
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

    private def nullable_string_size(value : String?) : Int32
      value ? string_size(value) : 1
    end

    private def nullable_symbol_size(value : String?) : Int32
      value ? string_size(value) : 1
    end

    private def nullable_binary_string_size(value : String?) : Int32
      value ? binary_header_size(value.bytesize.to_u64) + value.bytesize : 1
    end

    private def string_size(value : String) : Int32
      (value.bytesize <= UInt8::MAX ? 2 : 5) + value.bytesize
    end

    private def application_properties_fields_size(headers : LavinMQ::AMQP::Table) : Int32
      size = 0
      headers.each do |key, value|
        size += string_size(key)
        size += application_property_value_size(value)
      end
      size
    end

    private def write_nullable_string(io, value : String?) : Nil
      value ? Codec.write_string(io, value) : io.write_byte(0x40_u8)
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
        uint_size(value)
      when Float32
        5
      when Int64
        long_size(value)
      when Float64, Time
        9
      when String
        string_size(value)
      when Bytes
        binary_size(value)
      else
        string_size(value.to_s)
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

    private def outcome_size(outcome : Outcome) : Int32
      case outcome
      in .accepted?, .released?, .modified?
        3 + 1 # descriptor + list0
      in .rejected?
        3 + 1
      end
    end

    private def write_outcome(io, outcome : Outcome) : Nil
      case outcome
      in .accepted?
        write_descriptor(io, Descriptor::ACCEPTED)
      in .released?
        write_descriptor(io, Descriptor::RELEASED)
      in .rejected?
        write_descriptor(io, Descriptor::REJECTED)
      in .modified?
        write_descriptor(io, Descriptor::MODIFIED)
      end
      io.write_byte 0x45_u8
    end
  end
end
