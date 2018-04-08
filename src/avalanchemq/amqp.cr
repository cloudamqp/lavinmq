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
    alias Table = Hash(String, Field)

    enum Type : UInt8
      Method = 1
      Header = 2
      Body = 3
      Heartbeat = 8
    end

    struct Properties
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

      def initialize(@content_type : String?,
                     @content_encoding : String?,
                     @headers : Table?,
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

      def to_slice
        io = AMQP::MemoryIO.new
        encode(io)
        io.to_slice
      end
    end
  end
end
