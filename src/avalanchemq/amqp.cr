require "./amqp/*"

module AvalancheMQ
  module AMQP
    PROTOCOL_START = UInt8.static_array(65, 77, 81, 80, 0, 0, 9, 1)
    class InvalidFrameEnd < Exception
    end

    alias Field =
      Nil |
      Bool |
      UInt8 |
      UInt16 |
      UInt32 |
      UInt64 |
      Int32 |
      Int64 |
      Float32 |
      Float64 |
      String |
      Array(Field) |
      Array(UInt8) |
      Time |
      Hash(String, Field)

    struct Table
      def initialize(@hash : Hash(String, Field))
      end

      def self.from_io(io, format, size : UInt32? = nil) : Hash(String, Field)
        size ||= UInt32.from_io(io, format)
        end_pos = io.pos + size
        hash = Hash(String, Field).new
        while io.pos < end_pos
          key = ShortString.from_io(io, format)
          val = read_field(io, format)
          hash[key] = val
        end
        hash
      end

      def to_io(io, format)
        io.write_bytes(bytesize.to_u32 - 4, format)
        @hash.each do |key, value|
          io.write_bytes(ShortString.new(key), format)
          write_field(value, io, format)
        end
      end

      def bytesize : UInt32
        size = 4_u32
        @hash.each do |key, value|
          size += 1_u32 + key.bytesize
          size += field_bytesize(value)
        end
        size
      end

      private def field_bytesize(value : Field) : UInt32
        size = 1_u32
        case value
        when String
          size += sizeof(UInt32) + value.bytesize
        when UInt8
          size += sizeof(UInt8)
        when UInt16
          size += sizeof(UInt16)
        when Int32
          size += sizeof(Int32)
        when Int64
          size += sizeof(Int64)
        when Time
          size += sizeof(Int64)
        when Hash(String, Field)
          size += Table.new(value).bytesize
        when Bool
          size += sizeof(Bool)
        when Array(UInt8)
          size += 4 + value.size
        when Array(Field)
          size += array_bytesize(value)
        when Float32
          size += sizeof(Float32)
        when Float64
          size += sizeof(Float64)
        else raise "Unsupported Field type: #{value.class}"
        end
        size
      end

      private def array_bytesize(a : Array(Field)) : UInt32
        size = 4_u32
        a.each do |v|
          size += field_bytesize(v)
        end
        size
      end

      private def write_field(value, io, format)
        case value
        when String
          io.write_byte 'S'.ord.to_u8
          io.write_bytes LongString.new(value), format
        when UInt8
          io.write_byte 'b'.ord.to_u8
          io.write_byte(value)
        when UInt16
          io.write_byte 's'.ord.to_u8
          io.write_bytes(value, format)
        when Int32
          io.write_byte 'I'.ord.to_u8
          io.write_bytes(value, format)
        when Int64
          io.write_byte 'l'.ord.to_u8
          io.write_bytes(value, format)
        when Time
          io.write_byte 'T'.ord.to_u8
          io.write_bytes(value.epoch_ms.to_i64, format)
        when Hash(String, Field)
          io.write_byte 'F'.ord.to_u8
          io.write_bytes Table.new(value), format
        when Bool
          io.write_byte 't'.ord.to_u8
          io.write_byte(value ? 1_u8 : 0_u8)
        when Array
          io.write_byte 'A'.ord.to_u8
          size = array_bytesize(value)
          io.write_bytes(size.to_u32, format)
          value.each { |v| write_field(v, io, format) }
        when Float32
          io.write_byte 'f'.ord.to_u8
          io.write_bytes(value, format)
        when Float64
          io.write_byte 'd'.ord.to_u8
          io.write_bytes(value, format)
        when nil
          io.write_byte 'V'.ord.to_u8
        else raise "Unsupported Field type: #{value.class}"
        end
      end

      private def self.read_field(io, format) : Field
        type = io.read_byte
        case type
        when 'S' then LongString.from_io(io, format)
        when 's' then UInt16.from_io(io, format)
        when 'I' then Int32.from_io(io, format)
        when 'l' then Int64.from_io(io, format)
        when 'F' then Table.from_io(io, format)
        when 't' then io.read_byte == 1_u8
        when 'T' then Time.epoch_ms(Int64.from_io(io, format))
        when 'V' then nil
        when 'b' then io.read_byte
        when 'A' then read_array(io, format)
        when 'f' then Float32.from_io(io, format)
        when 'd' then Float64.from_io(io, format)
        when 'D' then raise "Cannot parse decimal"
        when 'x' then raise "Cannot parse byte array"
        else raise "Unknown type '#{type}' at #{io.pos}"
        end
      end

      private def self.read_array(io, format)
        size = UInt32.from_io(io, format)
        end_pos = io.pos + size - 4
        a = Array(Field).new
        while io.pos < end_pos
          a << read_field(io, format)
        end
        a
      end
    end

    struct ShortString
      def initialize(@str : String)
      end

      def to_io(io, format)
        raise ArgumentError.new("ShortString too long, max 255") if @str.bytesize > 255
        io.write_byte(@str.bytesize.to_u8)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = io.read_byte || raise ::IO::EOFError.new
        io.read_string(sz.to_i32)
      end
    end

    struct LongString
      def initialize(@str : String)
      end

      def to_io(io, format)
        io.write_bytes(@str.bytesize.to_u32, format)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = UInt32.from_io(io, format)
        io.read_string(sz)
      end
    end

    enum Type : UInt8
      Method = 1
      Header = 2
      Body = 3
      Heartbeat = 8
    end

    class Properties
      FLAG_CONTENT_TYPE     = 0x8000_u16
      FLAG_CONTENT_ENCODING = 0x4000_u16
      FLAG_HEADERS          = 0x2000_u16
      FLAG_DELIVERY_MODE    = 0x1000_u16
      FLAG_PRIORITY         = 0x0800_u16
      FLAG_CORRELATION_ID   = 0x0400_u16
      FLAG_REPLY_TO         = 0x0200_u16
      FLAG_EXPIRATION       = 0x0100_u16
      FLAG_MESSAGE_ID       = 0x0080_u16
      FLAG_TIMESTAMP        = 0x0040_u16
      FLAG_TYPE             = 0x0020_u16
      FLAG_USER_ID          = 0x0010_u16
      FLAG_APP_ID           = 0x0008_u16
      FLAG_RESERVED1        = 0x0004_u16

      property content_type
      property content_encoding
      property headers
      property delivery_mode
      property priority
      property correlation_id
      property reply_to
      property expiration
      property message_id
      property timestamp
      property type
      property user_id
      property app_id
      property reserved1

      def_equals_and_hash content_type, content_encoding, headers, delivery_mode,
        priority, correlation_id, reply_to, expiration, message_id, timestamp,
        type, user_id, app_id, reserved1

      def initialize(@content_type : String?,
                     @content_encoding : String?,
                     @headers : Hash(String, Field)?,
                     @delivery_mode : UInt8?,
                     @priority : UInt8?,
                     @correlation_id : String?,
                     @reply_to : String?,
                     @expiration : String?,
                     @message_id : String?,
                     @timestamp : Time?,
                     @type : String? ,
                     @user_id : String?,
                     @app_id : String?,
                     @reserved1 : String?)
      end

      def self.seek_past(io)
        flags = io.read_uint16
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_CONTENT_TYPE > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_CONTENT_ENCODING > 0
        io.seek(io.read_uint64.to_i, ::IO::Seek::Current)   if flags & FLAG_HEADERS > 0
        io.seek(1, ::IO::Seek::Current)                     if flags & FLAG_DELIVERY_MODE > 0
        io.seek(1, ::IO::Seek::Current)                     if flags & FLAG_PRIORITY > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_CORRELATION_ID > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_REPLY_TO > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_EXPIRATION > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_MESSAGE_ID > 0
        io.seek(4, ::IO::Seek::Current)                     if flags & FLAG_TIMESTAMP > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_TYPE > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_USER_ID > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_APP_ID > 0
        io.seek(io.read_byte.to_i, ::IO::Seek::Current)     if flags & FLAG_RESERVED1 > 0
      end

      def self.decode(io)
        flags = io.read_uint16
        content_type = io.read_short_string     if flags & FLAG_CONTENT_TYPE > 0
        content_encoding = io.read_short_string if flags & FLAG_CONTENT_ENCODING > 0
        headers = io.read_table                 if flags & FLAG_HEADERS > 0
        delivery_mode = io.read_byte            if flags & FLAG_DELIVERY_MODE > 0
        priority = io.read_byte                 if flags & FLAG_PRIORITY > 0
        correlation_id = io.read_short_string   if flags & FLAG_CORRELATION_ID > 0
        reply_to = io.read_short_string         if flags & FLAG_REPLY_TO > 0
        expiration = io.read_short_string       if flags & FLAG_EXPIRATION > 0
        message_id = io.read_short_string       if flags & FLAG_MESSAGE_ID > 0
        timestamp = io.read_timestamp           if flags & FLAG_TIMESTAMP > 0
        type = io.read_short_string             if flags & FLAG_TYPE > 0
        user_id = io.read_short_string          if flags & FLAG_USER_ID > 0
        app_id = io.read_short_string           if flags & FLAG_APP_ID > 0
        reserved1 = io.read_short_string        if flags & FLAG_RESERVED1 > 0
        Properties.new(content_type, content_encoding, headers, delivery_mode,
                       priority, correlation_id, reply_to, expiration,
                       message_id, timestamp, type, user_id, app_id, reserved1)
      end

      def to_io(io, format)
        encode(io)
      end

      def encode(io)
        flags = 0_u16
        flags = flags | FLAG_CONTENT_TYPE     if @content_type
        flags = flags | FLAG_CONTENT_ENCODING if @content_encoding
        flags = flags | FLAG_HEADERS          if @headers
        flags = flags | FLAG_DELIVERY_MODE    if @delivery_mode
        flags = flags | FLAG_PRIORITY         if @priority
        flags = flags | FLAG_CORRELATION_ID   if @correlation_id
        flags = flags | FLAG_REPLY_TO         if @reply_to
        flags = flags | FLAG_EXPIRATION       if @expiration
        flags = flags | FLAG_MESSAGE_ID       if @message_id
        flags = flags | FLAG_TIMESTAMP        if @timestamp
        flags = flags | FLAG_TYPE             if @type
        flags = flags | FLAG_USER_ID          if @user_id
        flags = flags | FLAG_APP_ID           if @app_id
        flags = flags | FLAG_RESERVED1        if @reserved1

        io.write_int(flags)

        io.write_short_string(@content_type.not_nil!)     if @content_type
        io.write_short_string(@content_encoding.not_nil!) if @content_encoding
        io.write_table(@headers.not_nil!)                 if @headers
        io.write_byte(@delivery_mode.not_nil!.to_u8)      if @delivery_mode
        io.write_byte(@priority.not_nil!.to_u8)           if @priority
        io.write_short_string(@correlation_id.not_nil!)   if @correlation_id
        io.write_short_string(@reply_to.not_nil!)         if @reply_to
        io.write_short_string(@expiration.not_nil!)       if @expiration
        io.write_short_string(@message_id.not_nil!)       if @message_id
        io.write_timestamp(@timestamp.not_nil!)           if @timestamp
        io.write_short_string(@type.not_nil!)             if @type
        io.write_short_string(@user_id.not_nil!)          if @user_id
        io.write_short_string(@app_id.not_nil!)           if @app_id
        io.write_short_string(@reserved1.not_nil!)        if @reserved1
      end

      def bytesize
        size = 2
        size += 1 + @content_type.not_nil!.bytesize     if @content_type
        size += 1 + @content_encoding.not_nil!.bytesize if @content_encoding
        size += Table.new(@headers.not_nil!).bytesize   if @headers
        size += 1                                       if @delivery_mode
        size += 1                                       if @priority
        size += 1 + @correlation_id.not_nil!.bytesize   if @correlation_id
        size += 1 + @reply_to.not_nil!.bytesize         if @reply_to
        size += 1 + @expiration.not_nil!.bytesize       if @expiration
        size += 1 + @message_id.not_nil!.bytesize       if @message_id
        size += sizeof(Int64)                           if @timestamp
        size += 1 + @type.not_nil!.bytesize             if @type
        size += 1 + @user_id.not_nil!.bytesize          if @user_id
        size += 1 + @app_id.not_nil!.bytesize           if @app_id
        size += 1 + @reserved1.not_nil!.bytesize        if @reserved1
        size
      end

      def to_slice
        io = AMQP::MemoryIO.new(bytesize)
        encode(io)
        io.to_slice
      end
    end
  end
end
