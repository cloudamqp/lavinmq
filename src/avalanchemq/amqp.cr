require "./amqp/*"
require "./slice_to_json"

module AvalancheMQ
  module AMQP
    PROTOCOL_START = UInt8.static_array(65, 77, 81, 80, 0, 0, 9, 1)

    # Protocol header for AMQP 0-9
    PROTOCOL_START_ALT = UInt8.static_array(65, 77, 81, 80, 1, 1, 0, 9)

    class InvalidFrameEnd < Exception
    end

    alias Field =
      Nil |
      Bool |
      Int8 |
      UInt8 |
      Int16 |
      UInt16 |
      Int32 |
      UInt32 |
      Int64 |
      Float32 |
      Float64 |
      String |
      Time |
      Hash(String, Field) |
      Array(Field) |
      Bytes |
      Array(Hash(String, Field))

    # https://github.com/crystal-lang/crystal/issues/4885#issuecomment-325109328
    def self.cast_to_field(x :  Array)
      return x.map { |e| cast_to_field(e).as(Field) }.as(Field)
    end

    def self.cast_to_field(x : Hash)
      h = Hash(String, Field).new
      x.each do |(k, v)|
        h[k] = cast_to_field(v).as(Field)
      end
      h.as(Field)
    end

    def self.cast_to_field(x)
      x.try &.raw.as(Field)
    end

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
        when Bool
          size += sizeof(Bool)
        when Int8
          size += sizeof(Int8)
        when UInt8
          size += sizeof(UInt8)
        when Int16
          size += sizeof(Int16)
        when UInt16
          size += sizeof(UInt16)
        when Int32
          size += sizeof(Int32)
        when UInt32
          size += sizeof(UInt32)
        when Int64
          size += sizeof(Int64)
        when UInt64
          size += sizeof(UInt64)
        when Float32
          size += sizeof(Float32)
        when Float64
          size += sizeof(Float64)
        when String
          size += sizeof(UInt32) + value.bytesize
        when Slice
          size += sizeof(UInt32) + value.bytesize
        when Array
          size += 4
          value.each do |v|
            size += field_bytesize(v)
          end
        when Time
          size += sizeof(Int64)
        when Hash(String, Field)
          size += Table.new(value).bytesize
        when Nil
          size += 0
        else raise "Unsupported Field type: #{value.class}"
        end
        size
      end

      private def write_field(value, io, format)
        case value
        when Bool
          io.write_byte 't'.ord.to_u8
          io.write_byte(value ? 1_u8 : 0_u8)
        when Int8
          io.write_byte 'b'.ord.to_u8
          io.write_bytes(value, format)
        when UInt8
          io.write_byte 'B'.ord.to_u8
          io.write_byte(value)
        when Int16
          io.write_byte 's'.ord.to_u8
          io.write_bytes(value, format)
        when UInt16
          io.write_byte 'u'.ord.to_u8
          io.write_bytes(value, format)
        when Int32
          io.write_byte 'I'.ord.to_u8
          io.write_bytes(value, format)
        when UInt32
          io.write_byte 'i'.ord.to_u8
          io.write_bytes(value, format)
        when Int64
          io.write_byte 'l'.ord.to_u8
          io.write_bytes(value, format)
        when Float32
          io.write_byte 'f'.ord.to_u8
          io.write_bytes(value, format)
        when Float64
          io.write_byte 'd'.ord.to_u8
          io.write_bytes(value, format)
        when String
          io.write_byte 'S'.ord.to_u8
          io.write_bytes LongString.new(value), format
        when Bytes
          io.write_byte 'x'.ord.to_u8
          io.write_bytes(value.bytesize.to_u32, format)
          io.write value
        when Array
          io.write_byte 'A'.ord.to_u8
          size = value.map { |v| field_bytesize(v) }.sum
          io.write_bytes(size.to_u32, format)
          value.each { |v| write_field(v, io, format) }
        when Time
          io.write_byte 'T'.ord.to_u8
          io.write_bytes(value.epoch.to_i64, format)
        when Hash(String, Field)
          io.write_byte 'F'.ord.to_u8
          io.write_bytes Table.new(value), format
        when Nil
          io.write_byte 'V'.ord.to_u8
        else raise "Unsupported Field type: #{value.class}"
        end
      end

      private def self.read_field(io, format) : Field
        type = io.read_byte
        case type
        when 't' then io.read_byte == 1_u8
        when 'b' then Int8.from_io(io, format)
        when 'B' then UInt8.from_io(io, format)
        when 's' then Int16.from_io(io, format)
        when 'u' then UInt16.from_io(io, format)
        when 'I' then Int32.from_io(io, format)
        when 'i' then UInt32.from_io(io, format)
        when 'l' then Int64.from_io(io, format)
        when 'f' then Float32.from_io(io, format)
        when 'd' then Float64.from_io(io, format)
        when 'S' then LongString.from_io(io, format)
        when 'x' then read_slice(io, format)
        when 'A' then read_array(io, format)
        when 'T' then Time.epoch(Int64.from_io(io, format))
        when 'F' then Table.from_io(io, format)
        when 'V' then nil
        else raise "Unknown field type '#{type}' at #{io.pos}"
        end
      end

      private def self.read_array(io, format)
        size = UInt32.from_io(io, format)
        end_pos = io.pos + size
        a = Array(Field).new
        while io.pos < end_pos
          a << read_field(io, format)
        end
        a
      end

      private def self.read_slice(io, format)
        size = UInt32.from_io(io, format)
        bytes = Bytes.new(size)
        io.read_fully bytes
        bytes
      end
    end

    struct ShortString
      def initialize(@str : String)
      end

      def to_io(io, format = nil)
        raise ArgumentError.new("Short string too long, max #{UInt8::MAX}") if @str.bytesize > UInt8::MAX
        io.write_byte(@str.bytesize.to_u8)
        io.write(@str.to_slice)
      end

      def self.from_io(io, format) : String
        sz = io.read_byte
        raise ::IO::EOFError.new("Can't read short string") if sz.nil?
        io.read_string(sz.to_i32)
      end
    end

    struct LongString
      def initialize(@str : String)
      end

      def to_io(io, format)
        raise ArgumentError.new("Long string is too long, max #{UInt32::MAX}") if @str.bytesize > UInt32::MAX
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

      def initialize(@content_type : String? = nil,
                     @content_encoding : String? = nil,
                     @headers : Hash(String, Field)? = nil,
                     @delivery_mode : UInt8? = nil,
                     @priority : UInt8? = nil,
                     @correlation_id : String? = nil,
                     @reply_to : String? = nil,
                     @expiration : String? = nil,
                     @message_id : String? = nil,
                     @timestamp : Time? = nil,
                     @type : String? = nil,
                     @user_id : String? = nil,
                     @app_id : String? = nil,
                     @reserved1 : String? = nil)
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

      def self.from_json(data : JSON::Any)
        p = Properties.new
        p.content_type = data["content_type"]?.try(&.as_s)
        p.content_encoding = data["content_encoding"]?.try(&.as_s)
        p.headers = data["headers"]?.try(&.as_h?)
          .try { |hdrs| AMQP.cast_to_field(hdrs).as(Hash(String, Field)) }
        p.delivery_mode = data["delivery_mode"]?.try(&.as_i?.try(&.to_u8))
        p.priority = data["priority"]?.try(&.as_i?.try(&.to_u8))
        p.correlation_id = data["correlation_id"]?.try(&.as_s)
        p.reply_to = data["reply_to"]?.try(&.as_s)
        p.expiration = data["expiration"]?.try(&.as_s)
        p.message_id = data["message_id"]?.try(&.as_s)
        p.timestamp = data["timestamp"]?.try(&.as_i64?).try { |ms| Time.epoch_ms(ms) }
        p.type = data["type"]?.try(&.as_s)
        p.user_id = data["user_id"]?.try(&.as_s)
        p.app_id = data["app_id"]?.try(&.as_s)
        p.reserved1 = data["reserved"]?.try(&.as_s)
        p
      end

      def to_json(json : JSON::Builder)
        {
          "content_type" => @content_type,
          "content_encoding" => @content_encoding,
          "headers" => @headers,
          "delivery_mode" => @delivery_mode,
          "priority" => @priority,
          "correlation_id" => @correlation_id,
          "reply_to" => @reply_to,
          "expiration" => @expiration,
          "message_id" => @message_id,
          "timestamp" => @timestamp,
          "type" => @type,
          "user_id" => @user_id,
          "app_id" => @app_id,
          "reserved" => @reserved1,
        }.compact.to_json(json)
      end

      def to_io(io, format)
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

        io.write_bytes(flags, format)

        io.write_bytes ShortString.new(@content_type.not_nil!), format     if @content_type
        io.write_bytes ShortString.new(@content_encoding.not_nil!), format if @content_encoding
        io.write_bytes Table.new(@headers.not_nil!), format                if @headers
        io.write_byte @delivery_mode.not_nil!                              if @delivery_mode
        io.write_byte @priority.not_nil!                                   if @priority
        io.write_bytes ShortString.new(@correlation_id.not_nil!), format   if @correlation_id
        io.write_bytes ShortString.new(@reply_to.not_nil!), format         if @reply_to
        io.write_bytes ShortString.new(@expiration.not_nil!), format       if @expiration
        io.write_bytes ShortString.new(@message_id.not_nil!), format       if @message_id
        io.write_bytes @timestamp.not_nil!.epoch.to_i64, format            if @timestamp
        io.write_bytes ShortString.new(@type.not_nil!), format             if @type
        io.write_bytes ShortString.new(@user_id.not_nil!), format          if @user_id
        io.write_bytes ShortString.new(@app_id.not_nil!), format           if @app_id
        io.write_bytes ShortString.new(@reserved1.not_nil!), format        if @reserved1
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
