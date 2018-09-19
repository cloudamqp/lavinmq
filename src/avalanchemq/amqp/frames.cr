require "../version"

module AvalancheMQ
  module AMQP
    abstract struct Frame
      getter type, channel

      def initialize(@type : Type, @channel : UInt16)
      end

      abstract def to_slice : Bytes

      def method_id
        0_u16
      end

      def class_id
        0_u16
      end

      def wrap(io, body_size : Number, format : ::IO::ByteFormat)
        io.write_byte @type.value
        io.write_bytes @channel, format
        io.write_bytes body_size.to_u32, format
        yield
        io.write_byte(206_u8)
      end

      def to_slice(body : Bytes)
        io = MemoryIO.new(8 + body.bytesize)
        io.write_byte @type.value
        io.write_int @channel
        io.write_int body.bytesize.to_u32
        io.write body
        io.write_byte(206_u8)
        io.to_slice
      end

      def self.decode(io, buffer : ::IO::Memory) : Frame
        type = Type.new(io.read_byte || raise(::IO::EOFError.new))
        channel = UInt16.from_io(io, ::IO::ByteFormat::NetworkEndian)
        size = UInt32.from_io(io, ::IO::ByteFormat::NetworkEndian)
        frame =
          case type
          when Type::Method
            ::IO.copy io, buffer, size
            MethodFrame.decode(channel, buffer.to_slice)
          when Type::Body
            ::IO.copy io, buffer, size
            BodyFrame.new(channel, size, buffer)
          when Type::Header then HeaderFrame.decode(channel, io)
          when Type::Heartbeat then HeartbeatFrame.new
          else
            raise NotImplemented.new channel, 0_u16, 0_u16
          end
        frame_end = io.read_byte || raise FrameDecodeError.new("No frame ending")
        if frame_end != 206
          raise InvalidFrameEnd.new("Frame-end was #{frame_end.to_s}, expected 206")
        end
        frame
      rescue ex : ::IO::Error | Errno
        raise FrameDecodeError.new(ex.message, ex)
      ensure
        buffer.clear unless frame.is_a? BodyFrame
      end
    end

    class FrameDecodeError < Exception; end

    class NotImplemented < Exception
      getter channel, class_id, method_id

      def initialize(@channel : UInt16, @class_id : UInt16, @method_id : UInt16)
        super("Method id #{@method_id} not implemented in class #{@class_id} (Channel #{@channel})")
      end

      def initialize(frame : MethodFrame)
        @channel = frame.channel
        @class_id = frame.class_id
        @method_id = frame.method_id
        super("Method id #{@method_id} not implemented in class #{@class_id}")
      end

      def initialize(frame : Frame)
        @channel = 0_u16
        @class_id = 0_u16
        @method_id = 0_u16
        super("Frame type #{frame.type} not implemented")
      end
    end

    struct HeartbeatFrame < Frame
      def initialize
        @type = Type::Heartbeat
        @channel = 0_u16
      end

      def to_io(io)
        wrap(io, 0)
      end

      def to_slice
        super(Bytes.empty)
      end

      def self.decode
        self.new
      end
    end

    abstract struct MethodFrame < Frame
      def initialize(@channel : UInt16)
        @type = Type::Method
      end

      abstract def class_id : UInt16
      abstract def method_id : UInt16

      def wrap(io, bytesize, format)
        super(io, bytesize + sizeof(UInt16) + sizeof(UInt16), format) do
          io.write_bytes class_id, format
          io.write_bytes method_id, format
          yield
        end
      end

      def to_slice(body : Bytes)
        io = MemoryIO.new(4 + body.bytesize)
        io.write_int class_id
        io.write_int method_id
        io.write body
        super(io.to_slice)
      end

      def self.decode(channel, payload)
        body = AMQP::MemoryIO.new(payload, false)
        class_id = body.read_uint16
        case class_id
        when 10_u16 then Connection.decode(channel, body)
        when 20_u16 then Channel.decode(channel, body)
        when 40_u16 then Exchange.decode(channel, body)
        when 50_u16 then Queue.decode(channel, body)
        when 60_u16 then Basic.decode(channel, body)
        when 85_u16 then Confirm.decode(channel, body)
        when 90_u16 then Tx.decode(channel, body)
        else
          raise NotImplemented.new(channel, class_id, 0_u16)
        end
      end
    end

    abstract struct Connection < MethodFrame
      CLASS_ID = 10_u16

      def class_id
        CLASS_ID
      end

      def initialize
        super(0_u16)
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Start.decode(body)
        when 11_u16 then StartOk.decode(body)
        when 30_u16 then Tune.decode(body)
        when 31_u16 then TuneOk.decode(body)
        when 40_u16 then Open.decode(body)
        when 41_u16 then OpenOk.decode(body)
        when 50_u16 then Close.decode(body)
        when 51_u16 then CloseOk.decode(body)
        else             raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Start < Connection
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + 1 +
                                    4 + @mechanisms.bytesize +
                                    4 + @locales.bytesize)
          body.write_byte(@version_major)
          body.write_byte(@version_minor)
          body.write_table(@server_properties)
          body.write_long_string(@mechanisms)
          body.write_long_string(@locales)
          super(body.to_slice)
        end

        getter server_properties

        def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                       @server_properties = {
                         "product"      => "AvalancheMQ",
                         "version"      => VERSION,
                         "platform"     => "",
                         "capabilities" => {
                           "publisher_confirms"           => true,
                           "exchange_exchange_bindings"   => true,
                           "basic.nack"                   => true,
                           "per_consumer_qos"             => true,
                           "authentication_failure_close" => true,
                           "consumer_cancel_notify"       => true,
                         } of String => Field,
                       } of String => Field,
                       @mechanisms = "PLAIN", @locales = "en_US")
          super()
        end

        def self.decode(io)
          version_major = io.read_byte
          version_minor = io.read_byte
          server_properties = io.read_table
          mech = io.read_long_string
          locales = io.read_long_string
          self.new(version_major, version_minor, server_properties, mech, locales)
        end
      end

      struct StartOk < Connection
        getter client_properties, mechanism, response, locale

        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        def initialize(@client_properties : Hash(String, Field), @mechanism : String,
                       @response : String, @locale : String)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @mechanism.bytesize +
                                    4 + @response.bytesize +
                                    1 + @locale.bytesize)
          body.write_table(@client_properties)
          body.write_short_string(@mechanism)
          body.write_long_string(@response)
          body.write_short_string(@locale)
          super(body.to_slice)
        end

        def self.decode(io)
          props = io.read_table
          mech = io.read_short_string
          auth = io.read_long_string
          locale = io.read_short_string
          self.new(props, mech, auth, locale)
        end
      end

      struct Tune < Connection
        getter channel_max, frame_max, heartbeat
        METHOD_ID = 30_u16

        def method_id
          METHOD_ID
        end

        def initialize(@channel_max = 0_u16, @frame_max = 131072_u32, @heartbeat = 0_u16)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(2 + 4 + 2)
          body.write_int(@channel_max)
          body.write_int(@frame_max)
          body.write_int(@heartbeat)
          super(body.to_slice)
        end

        def self.decode(io)
          channel_max = io.read_uint16
          frame_max = io.read_uint32
          heartbeat = io.read_uint16
          self.new(channel_max, frame_max, heartbeat)
        end
      end

      struct TuneOk < Connection
        getter channel_max, frame_max, heartbeat
        METHOD_ID = 31_u16

        def method_id
          METHOD_ID
        end

        def initialize(@channel_max = 0_u16, @frame_max = 131072_u32, @heartbeat = 60_u16)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(2 + 4 + 2)
          body.write_int(@channel_max)
          body.write_int(@frame_max)
          body.write_int(@heartbeat)
          super(body.to_slice)
        end

        def self.decode(io)
          channel_max = io.read_uint16
          frame_max = io.read_uint32
          heartbeat = io.read_uint16
          self.new(channel_max, frame_max, heartbeat)
        end
      end

      struct Open < Connection
        getter vhost, reserved1, reserved2
        METHOD_ID = 40_u16

        def method_id
          METHOD_ID
        end

        def initialize(@vhost = "/", @reserved1 = "", @reserved2 = false)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @vhost.bytesize +
                                    1 + @reserved1.bytesize + 1)
          body.write_short_string(@vhost)
          body.write_short_string(@reserved1)
          body.write_bool(@reserved2)
          super(body.to_slice)
        end

        def self.decode(io)
          vhost = io.read_short_string
          reserved1 = io.read_short_string
          reserved2 = io.read_bool
          Open.new(vhost, reserved1, reserved2)
        end
      end

      struct OpenOk < Connection
        getter reserved1

        METHOD_ID = 41_u16

        def method_id
          METHOD_ID
        end

        def initialize(@reserved1 = "")
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @reserved1.bytesize)
          body.write_short_string(@reserved1)
          super(body.to_slice)
        end

        def self.decode(io)
          reserved1 = io.read_short_string
          self.new(reserved1)
        end
      end

      struct Close < Connection
        getter reply_code, reply_text, failing_class_id, failing_method_id

        METHOD_ID = 50_u16

        def method_id
          METHOD_ID
        end

        def initialize(@reply_code : UInt16, @reply_text : String, @failing_class_id : UInt16, @failing_method_id : UInt16)
          super()
        end

        def to_slice
          io = AMQP::MemoryIO.new(2 + 1 + @reply_text.bytesize + 2 + 2)
          io.write_int(@reply_code)
          io.write_short_string(@reply_text)
          io.write_int(@failing_class_id)
          io.write_int(@failing_method_id)
          super(io.to_slice)
        end

        def self.decode(io)
          code = io.read_uint16
          text = io.read_short_string
          failing_class_id = io.read_uint16
          failing_method_id = io.read_uint16
          self.new(code, text, failing_class_id, failing_method_id)
        end
      end

      struct CloseOk < Connection
        METHOD_ID = 51_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(io)
          self.new
        end
      end
    end

    abstract struct Channel < MethodFrame
      CLASS_ID = 20_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Open.decode(channel, body)
        when 11_u16 then OpenOk.decode(channel, body)
          # when 20_u16 then Flow.decode(channel, body)
          # when 21_u16 then FlowOk.decode(channel, body)
        when 40_u16 then Close.decode(channel, body)
        when 41_u16 then CloseOk.decode(channel, body)
        else             raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Open < Channel
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        getter reserved1

        def initialize(channel : UInt16, @reserved1 = "")
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(1 + @reserved1.bytesize)
          io.write_short_string @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_short_string
          Open.new channel, reserved1
        end
      end

      struct OpenOk < Channel
        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        getter reserved1

        def initialize(channel : UInt16, @reserved1 = "")
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(1 + @reserved1.bytesize)
          io.write_long_string @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_long_string
          OpenOk.new channel, reserved1
        end
      end

      struct Close < Channel
        METHOD_ID = 40_u16

        def method_id
          METHOD_ID
        end

        getter reply_code, reply_text, classid, methodid

        def initialize(channel : UInt16, @reply_code : UInt16, @reply_text : String, @classid : UInt16, @methodid : UInt16)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(2 + 1 + @reply_text.bytesize + 2 + 2)
          io.write_int(@reply_code)
          io.write_short_string(@reply_text)
          io.write_int(@classid)
          io.write_int(@methodid)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reply_code = io.read_uint16
          reply_text = io.read_short_string
          classid = io.read_uint16
          methodid = io.read_uint16
          Close.new channel, reply_code, reply_text, classid, methodid
        end
      end

      struct CloseOk < Channel
        METHOD_ID = 41_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          CloseOk.new channel
        end
      end
    end

    abstract struct Exchange < MethodFrame
      CLASS_ID = 40_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Declare.decode(channel, body)
        when 11_u16 then DeclareOk.decode(channel, body)
        when 20_u16 then Delete.decode(channel, body)
        when 21_u16 then DeleteOk.decode(channel, body)
        when 30_u16 then Bind.decode(channel, body)
        when 31_u16 then BindOk.decode(channel, body)
        when 40_u16 then Unbind.decode(channel, body)
        when 51_u16 then UnbindOk.decode(channel, body)
        else             raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Declare < Exchange
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, exchange_name, exchange_type, passive, durable, auto_delete, internal, no_wait, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String, @exchange_type : String, @passive : Bool, @durable : Bool, @auto_delete : Bool,
                       @internal : Bool, @no_wait : Bool, @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @exchange_name
          io.write_short_string @exchange_type
          bits = 0_u8
          bits = bits | (1 << 0) if @passive
          bits = bits | (1 << 1) if @durable
          bits = bits | (1 << 2) if @auto_delete
          bits = bits | (1 << 3) if @internal
          bits = bits | (1 << 4) if @no_wait
          io.write_byte(bits)
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          type = io.read_short_string
          bits = io.read_byte
          passive = bits.bit(0) == 1
          durable = bits.bit(1) == 1
          auto_delete = bits.bit(2) == 1
          internal = bits.bit(3) == 1
          no_wait = bits.bit(4) == 1
          args = io.read_table
          self.new channel, reserved1, name, type, passive, durable, auto_delete, internal, no_wait, args
        end
      end

      struct DeclareOk < Exchange
        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(io)
          self.new
        end
      end

      struct Delete < Exchange
        METHOD_ID = 20_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, exchange_name, if_unused, no_wait

        def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String,
                       @if_unused : Bool, @no_wait : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(2 + 1 + @exchange_name.bytesize + 1)
          io.write_int @reserved1
          io.write_short_string @exchange_name
          bits = 0_u8
          bits = bits | (1 << 0) if @if_unused
          bits = bits | (1 << 1) if @no_wait
          io.write_byte(bits)
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          bits = io.read_byte
          if_unused = bits.bit(0) == 1
          no_wait = bits.bit(1) == 1
          self.new channel, reserved1, name, if_unused, no_wait
        end
      end

      struct DeleteOk < Exchange
        METHOD_ID = 21_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(io)
          self.new
        end
      end

      struct Bind < Exchange
        METHOD_ID = 30_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, destination, source, routing_key, no_wait, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                       @source : String, @routing_key : String, @no_wait : Bool,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @destination
          io.write_short_string @source
          io.write_short_string @routing_key
          io.write_bool @no_wait
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          destination = io.read_short_string
          source = io.read_short_string
          routing_key = io.read_short_string
          bits = io.read_byte
          no_wait = bits.bit(0) == 1
          args = io.read_table
          self.new channel, reserved1, destination, source, routing_key, no_wait, args
        end
      end

      struct BindOk < Exchange
        METHOD_ID = 31_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Unbind < Exchange
        METHOD_ID = 40_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, destination, source, routing_key, no_wait, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @destination : String,
                       @source : String, @routing_key : String, @no_wait : Bool,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @destination
          io.write_short_string @source
          io.write_short_string @routing_key
          io.write_bool @no_wait
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          destination = io.read_short_string
          source = io.read_short_string
          routing_key = io.read_short_string
          bits = io.read_byte
          no_wait = bits.bit(0) == 1
          args = io.read_table
          self.new channel, reserved1, destination, source, routing_key, no_wait, args
        end
      end

      struct UnbindOk < Exchange
        METHOD_ID = 51_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end
    end

    abstract struct Queue < MethodFrame
      CLASS_ID = 50_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Declare.decode(channel, body)
        when 11_u16 then DeclareOk.decode(channel, body)
        when 20_u16 then Bind.decode(channel, body)
        when 21_u16 then BindOk.decode(channel, body)
        when 30_u16 then Purge.decode(channel, body)
        when 31_u16 then PurgeOk.decode(channel, body)
        when 40_u16 then Delete.decode(channel, body)
        when 41_u16 then DeleteOk.decode(channel, body)
        when 50_u16 then Unbind.decode(channel, body)
        when 51_u16 then UnbindOk.decode(channel, body)
        else             raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Declare < Queue
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        property reserved1, queue_name, passive, durable, exclusive, auto_delete, no_wait, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                       @passive : Bool, @durable : Bool, @exclusive : Bool,
                       @auto_delete : Bool, @no_wait : Bool, @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @queue_name
          bits = 0_u8
          bits = bits | (1 << 0) if @passive
          bits = bits | (1 << 1) if @durable
          bits = bits | (1 << 2) if @exclusive
          bits = bits | (1 << 3) if @auto_delete
          bits = bits | (1 << 4) if @no_wait
          io.write_byte(bits)
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          bits = io.read_byte
          passive = bits.bit(0) == 1
          durable = bits.bit(1) == 1
          exclusive = bits.bit(2) == 1
          auto_delete = bits.bit(3) == 1
          no_wait = bits.bit(4) == 1
          args = io.read_table
          self.new channel, reserved1, name, passive, durable, exclusive, auto_delete, no_wait, args
        end
      end

      struct DeclareOk < Queue
        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        getter queue_name, message_count, consumer_count

        def initialize(channel : UInt16, @queue_name : String, @message_count : UInt32, @consumer_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(1 + @queue_name.bytesize + 4 + 4)
          io.write_short_string @queue_name
          io.write_int @message_count
          io.write_int @consumer_count
          super io.to_slice
        end

        def self.decode(channel, io)
          queue_name = io.read_short_string
          message_count = io.read_uint32
          consumer_count = io.read_uint32
          self.new channel, queue_name, message_count, consumer_count
        end
      end

      struct Bind < Queue
        METHOD_ID = 20_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, queue_name, exchange_name, routing_key, no_wait, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                       @exchange_name : String, @routing_key : String, @no_wait : Bool,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @queue_name
          io.write_short_string @exchange_name
          io.write_short_string @routing_key
          io.write_bool @no_wait
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue_name = io.read_short_string
          exchange_name = io.read_short_string
          routing_key = io.read_short_string
          bits = io.read_byte
          no_wait = bits.bit(0) == 1
          args = io.read_table
          self.new channel, reserved1, queue_name, exchange_name, routing_key, no_wait, args
        end
      end

      struct BindOk < Queue
        METHOD_ID = 21_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Delete < Queue
        METHOD_ID = 40_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, queue_name, if_unused, if_empty, no_wait

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                       @if_unused : Bool, @if_empty : Bool, @no_wait : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(2 + 1 + @queue_name.bytesize + 1)
          io.write_int @reserved1
          io.write_short_string @queue_name
          bits = 0_u8
          bits = bits | (1 << 0) if @if_unused
          bits = bits | (1 << 1) if @if_empty
          bits = bits | (1 << 2) if @no_wait
          io.write_byte(bits)
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          bits = io.read_byte
          if_unused = bits.bit(0) == 1
          if_empty = bits.bit(1) == 1
          no_wait = bits.bit(2) == 1
          self.new channel, reserved1, name, if_unused, if_empty, no_wait
        end
      end

      struct DeleteOk < Queue
        METHOD_ID = 41_u16

        def method_id
          METHOD_ID
        end

        def initialize(channel : UInt16, @message_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(4)
          io.write_int @message_count
          super io.to_slice
        end

        def self.decode(channel, io)
          raise NotImplemented.new(channel, CLASS_ID, METHOD_ID)
        end
      end

      struct Unbind < Queue
        METHOD_ID = 50_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, queue_name, exchange_name, routing_key, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                       @exchange_name : String, @routing_key : String,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @queue_name
          io.write_short_string @exchange_name
          io.write_short_string @routing_key
          io.write_table @arguments
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue_name = io.read_short_string
          exchange_name = io.read_short_string
          routing_key = io.read_short_string
          args = io.read_table
          self.new channel, reserved1, queue_name, exchange_name, routing_key, args
        end
      end

      struct UnbindOk < Queue
        METHOD_ID = 51_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Purge < Queue
        METHOD_ID = 30_u16

        def method_id
          METHOD_ID
        end

        getter reserved1, queue_name, no_wait

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String, @no_wait : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(2 + 1 + @queue_name.bytesize + 1)
          io.write_int @reserved1
          io.write_short_string @queue_name
          io.write_bool @no_wait
          super io.to_slice
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue_name = io.read_short_string
          bits = io.read_byte
          no_wait = bits.bit(0) == 1
          self.new channel, reserved1, queue_name, no_wait
        end
      end

      struct PurgeOk < Queue
        METHOD_ID = 31_u16

        def method_id
          METHOD_ID
        end

        def initialize(channel : UInt16, @message_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(4)
          io.write_int @message_count
          super io.to_slice
        end

        def self.decode(channel, io)
          raise NotImplemented.new(channel, CLASS_ID, METHOD_ID)
        end
      end
    end

    abstract struct Basic < MethodFrame
      CLASS_ID = 60_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when  10_u16 then Qos.decode(channel, body)
        when  11_u16 then QosOk.decode(channel, body)
        when  20_u16 then Consume.decode(channel, body)
        when  21_u16 then ConsumeOk.decode(channel, body)
        when  30_u16 then Cancel.decode(channel, body)
        when  31_u16 then CancelOk.decode(channel, body)
        when  40_u16 then Publish.decode(channel, body)
        when  50_u16 then Return.decode(channel, body)
        when  60_u16 then Deliver.decode(channel, body)
        when  70_u16 then Get.decode(channel, body)
        when  71_u16 then GetOk.decode(channel, body)
        when  72_u16 then GetEmpty.decode(channel, body)
        when  80_u16 then Ack.decode(channel, body)
        when  90_u16 then Reject.decode(channel, body)
        when 120_u16 then Nack.decode(channel, body)
        else              raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Publish < Basic
        METHOD_ID = 40_u16

        def method_id
          METHOD_ID
        end

        getter exchange, routing_key, mandatory, immediate

        def initialize(channel, @reserved1 : UInt16, @exchange : String,
                       @routing_key : String, @mandatory : Bool, @immediate : Bool)
          super(channel)
        end

        def to_io(io, format)
          wrap(io, 2 + 1 + @exchange.bytesize + 1 + @routing_key.bytesize + 1,
            format) do
            io.write_bytes @reserved1, format
            io.write_bytes ShortString.new(@exchange), format
            io.write_bytes ShortString.new(@routing_key), format
            bits = 0_u8
            bits = bits | (1 << 0) if @mandatory
            bits = bits | (1 << 1) if @immediate
            io.write_byte(bits)
          end
        end

        def to_slice
          io = AMQP::MemoryIO.new(2 +
                                  1 + @exchange.bytesize +
                                  1 + @routing_key.bytesize +
                                  1)
          io.write_int @reserved1
          io.write_short_string @exchange
          io.write_short_string @routing_key
          bits = 0_u8
          bits = bits | (1 << 0) if @mandatory
          bits = bits | (1 << 1) if @immediate
          io.write_byte(bits)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          exchange = io.read_short_string
          routing_key = io.read_short_string
          bits = io.read_byte
          mandatory = bits.bit(0) == 1
          immediate = bits.bit(1) == 1
          self.new channel, reserved1, exchange, routing_key, mandatory, immediate
        end
      end

      struct Deliver < Basic
        METHOD_ID = 60_u16

        def method_id
          METHOD_ID
        end

        getter consumer_tag, delivery_tag, redelivered, exchange, routing_key

        def initialize(channel, @consumer_tag : String, @delivery_tag : UInt64,
                       @redelivered : Bool, @exchange : String, @routing_key : String)
          super(channel)
        end

        def to_io(io, format)
          wrap(io, 1 + @consumer_tag.bytesize + sizeof(UInt64) + 1 + 1 + @exchange.bytesize + 1 + @routing_key.bytesize, format) do
            io.write_bytes ShortString.new(@consumer_tag), format
            io.write_bytes @delivery_tag, format
            io.write_byte @redelivered ? 1_u8 : 0_u8
            io.write_bytes ShortString.new(@exchange), format
            io.write_bytes ShortString.new(@routing_key), format
          end
        end

        def to_slice
          io = AMQP::MemoryIO.new(1 + @consumer_tag.bytesize + 8 + 1 +
                                  1 + @exchange.bytesize +
                                  1 + @routing_key.bytesize)
          io.write_short_string @consumer_tag
          io.write_int @delivery_tag
          io.write_bool @redelivered
          io.write_short_string @exchange
          io.write_short_string @routing_key
          super(io.to_slice)
        end

        def self.decode(channel, io)
          consumer_tag = io.read_short_string
          delivery_tag = io.read_uint64
          redelivered = io.read_bool
          exchange = io.read_short_string
          routing_key = io.read_short_string
          self.new channel, consumer_tag, delivery_tag, redelivered, exchange, routing_key
        end
      end

      struct Get < Basic
        METHOD_ID = 70_u16

        def method_id
          METHOD_ID
        end

        getter queue, no_ack

        def initialize(channel, @reserved1 : UInt16, @queue : String, @no_ack : Bool)
          super(channel)
        end

        def to_slice
          raise NotImplemented.new(@channel, class_id, method_id)
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue = io.read_short_string
          no_ack = io.read_bool
          self.new channel, reserved1, queue, no_ack
        end
      end

      struct GetOk < Basic
        METHOD_ID = 71_u16

        def method_id
          METHOD_ID
        end

        def initialize(channel, @delivery_tag : UInt64, @redelivered : Bool,
                       @exchange : String, @routing_key : String, @message_count : UInt32)
          super(channel)
        end

        def to_io(io, format)
          wrap(io, sizeof(UInt64) + 1 + 1 + @exchange.bytesize + 1 + @routing_key.bytesize + sizeof(UInt32), format) do
            io.write_bytes @delivery_tag, format
            io.write_byte @redelivered ? 1_u8 : 0_u8
            io.write_bytes ShortString.new(@exchange), format
            io.write_bytes ShortString.new(@routing_key), format
            io.write_bytes @message_count, format
          end
        end

        def to_slice
          io = AMQP::MemoryIO.new(8 + 1 + 1 + @exchange.bytesize + 1 + @routing_key.bytesize + 4)
          io.write_int @delivery_tag
          io.write_bool @redelivered
          io.write_short_string @exchange
          io.write_short_string @routing_key
          io.write_int @message_count
          super(io.to_slice)
        end

        def self.decode(channel, io)
          raise NotImplemented.new(channel, CLASS_ID, METHOD_ID)
        end
      end

      struct GetEmpty < Basic
        METHOD_ID = 72_u16

        def method_id
          METHOD_ID
        end

        def initialize(channel, @reserved1 = 0_u16)
          super(channel)
        end

        def to_io(io, format)
          wrap(io, 2, format) do
            io.write_bytes @reserved1, format
          end
        end

        def to_slice
          io = AMQP::MemoryIO.new(2)
          io.write_int @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          self.new channel, reserved1
        end
      end

      struct Ack < Basic
        METHOD_ID = 80_u16

        def method_id
          METHOD_ID
        end

        getter :delivery_tag, :multiple

        def initialize(channel, @delivery_tag : UInt64, @multiple : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(8 + 1)
          io.write_int(@delivery_tag)
          io.write_bool(@multiple)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          multiple = io.read_bool
          self.new channel, delivery_tag, multiple
        end
      end

      struct Reject < Basic
        METHOD_ID = 90_u16

        def method_id
          METHOD_ID
        end

        getter :delivery_tag, :requeue

        def initialize(channel, @delivery_tag : UInt64, @requeue : Bool)
          super(channel)
        end

        def to_slice
          raise NotImplemented.new(@channel, class_id, method_id)
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          requeue = io.read_bool
          self.new channel, delivery_tag, requeue
        end
      end

      struct Nack < Basic
        METHOD_ID = 120_u16

        def method_id
          METHOD_ID
        end

        getter :delivery_tag, :multiple, :requeue

        def initialize(channel, @delivery_tag : UInt64, @multiple : Bool, @requeue : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(10)
          io.write_int(@delivery_tag)
          io.write_bool(@multiple)
          io.write_bool(@requeue)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          bits = io.read_byte
          multiple = bits.bit(0) == 1
          requeue = bits.bit(1) == 1
          self.new channel, delivery_tag, multiple, requeue
        end
      end

      struct Qos < Basic
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        getter prefetch_size, prefetch_count, global

        def initialize(channel, @prefetch_size : UInt32, @prefetch_count : UInt16, @global : Bool)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(4 + 2 + 1)
          io.write_int @prefetch_size
          io.write_int @prefetch_count
          io.write_bool @global
          super(io.to_slice)
        end

        def self.decode(channel, io)
          prefetch_size = io.read_uint32
          prefetch_count = io.read_uint16
          global = io.read_bool
          self.new channel, prefetch_size, prefetch_count, global
        end
      end

      struct QosOk < Basic
        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Consume < Basic
        METHOD_ID = 20_u16

        def method_id
          METHOD_ID
        end

        property queue, consumer_tag, no_local, no_ack, exclusive, no_wait, arguments

        def initialize(channel, @reserved1 : UInt16, @queue : String, @consumer_tag : String,
                       @no_local : Bool, @no_ack : Bool, @exclusive : Bool, @no_wait : Bool,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new
          io.write_int @reserved1
          io.write_short_string @queue
          io.write_short_string @consumer_tag
          bits = 0_u8
          bits = bits | (1 << 0) if @no_local
          bits = bits | (1 << 1) if @no_ack
          bits = bits | (1 << 2) if @exclusive
          bits = bits | (1 << 3) if @no_wait
          io.write_byte(bits)
          io.write_table @arguments
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue = io.read_short_string
          consumer_tag = io.read_short_string
          bits = io.read_byte
          no_local = bits.bit(0) == 1
          no_ack = bits.bit(1) == 1
          exclusive = bits.bit(2) == 1
          no_wait = bits.bit(3) == 1
          args = io.read_table
          self.new channel, reserved1, queue, consumer_tag, no_local, no_ack, exclusive, no_wait, args
        end
      end

      struct ConsumeOk < Basic
        METHOD_ID = 21_u16

        def method_id
          METHOD_ID
        end

        getter consumer_tag

        def initialize(channel, @consumer_tag : String)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(1 + @consumer_tag.bytesize)
          io.write_short_string @consumer_tag
          super(io.to_slice)
        end

        def self.decode(channel, io)
          self.new(channel, io.read_short_string)
        end
      end

      struct Return < Basic
        METHOD_ID = 50_u16

        def method_id
          METHOD_ID
        end

        getter reply_code, reply_text, exchange_name, routing_key

        def initialize(channel, @reply_code : UInt16, @reply_text : String,
                       @exchange_name : String, @routing_key : String)
          super(channel)
        end

        def to_io(io, format)
          wrap(io, 2 + 1 + @reply_text.bytesize + 1 + @exchange_name.bytesize + 1 + @routing_key.bytesize, format) do
            io.write_bytes(@reply_code, format)
            io.write_bytes ShortString.new(@reply_text), format
            io.write_bytes ShortString.new(@exchange_name), format
            io.write_bytes ShortString.new(@routing_key), format
          end
        end

        def to_slice
          io = MemoryIO.new(2 + 1 + @reply_text.bytesize +
                            1 + @exchange_name.bytesize +
                            1 + @routing_key.bytesize)
          io.write_int(@reply_code)
          io.write_short_string(@reply_text)
          io.write_short_string(@exchange_name)
          io.write_short_string(@routing_key)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reply_code = io.read_uint16
          reply_text = io.read_short_string
          exchange_name = io.read_short_string
          routing_key = io.read_short_string
          self.new(channel, reply_code, reply_text, exchange_name, routing_key)
        end
      end

      struct Cancel < Basic
        METHOD_ID = 30_u16

        def method_id
          METHOD_ID
        end

        getter consumer_tag, no_wait

        def initialize(channel : UInt16, @consumer_tag : String, @no_wait : Bool)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(1 + @consumer_tag.bytesize + 1)
          io.write_short_string(@consumer_tag)
          io.write_bool(@no_wait)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          consumer_tag = io.read_short_string
          no_wait = io.read_bool
          self.new(channel, consumer_tag, no_wait)
        end
      end

      struct CancelOk < Basic
        METHOD_ID = 31_u16

        def method_id
          METHOD_ID
        end

        getter consumer_tag

        def initialize(channel : UInt16, @consumer_tag : String)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(1 + @consumer_tag.bytesize)
          io.write_short_string(@consumer_tag)
          super(io.to_slice)
        end

        def self.decode(channel, io)
          consumer_tag = io.read_short_string
          self.new(channel, consumer_tag)
        end
      end
    end

    struct HeaderFrame < Frame
      getter body_size, properties

      def initialize(channel : UInt16, @class_id : UInt16, @weight : UInt16,
                     @body_size : UInt64, @properties : Properties)
        super(Type::Header, channel)
      end

      def to_io(io : ::IO, format : ::IO::ByteFormat)
        wrap(io, sizeof(UInt16) + sizeof(UInt16) + sizeof(UInt64) + @properties.bytesize, format) do
          io.write_bytes @class_id, format
          io.write_bytes @weight, format
          io.write_bytes @body_size, format
          io.write_bytes @properties, format
        end
      end

      def to_slice
        raise "dont to_slice header frame"
      end

      def self.decode(channel, io)
        class_id = UInt16.from_io(io, ::IO::ByteFormat::NetworkEndian)
        weight = UInt16.from_io(io, ::IO::ByteFormat::NetworkEndian)
        body_size = UInt64.from_io(io, ::IO::ByteFormat::NetworkEndian)
        props = Properties.from_io(io, ::IO::ByteFormat::NetworkEndian)
        self.new channel, class_id, weight, body_size, props
      end
    end

    struct BodyFrame < Frame
      getter body_size, body

      def initialize(@channel : UInt16, @body_size : UInt32, @body : ::IO)
        @type = Type::Body
      end

      def to_slice
        raise "Dont use this"
      end

      def to_io(io, format)
        wrap(io, @body_size, format) do
          ::IO.copy(@body, io, @body_size)
        end
      end

      def inspect(io)
        io << self.class.name
        io << "("
        io << "@body_size=" << @body_size
        io << ")"
        io
      end
    end

    abstract struct Confirm < MethodFrame
      CLASS_ID = 85_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Select.decode(channel, body)
        when 11_u16 then SelectOk.decode(channel, body)
        else             raise NotImplemented.new(channel, CLASS_ID, method_id)
        end
      end

      struct Select < Confirm
        METHOD_ID = 10_u16

        def method_id
          METHOD_ID
        end

        getter no_wait

        def initialize(channel : UInt16, @no_wait : Bool)
          super(channel)
        end

        def self.decode(channel, io)
          self.new channel, io.read_bool
        end

        def to_slice
          io = MemoryIO.new(1)
          io.write_bool(@no_wait)
          super(io.to_slice)
        end
      end

      struct SelectOk < Confirm
        METHOD_ID = 11_u16

        def method_id
          METHOD_ID
        end

        def to_slice
          super Bytes.empty
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end
    end

    abstract struct Tx < MethodFrame
      CLASS_ID = 90_u16

      def class_id
        CLASS_ID
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        raise NotImplemented.new(channel, CLASS_ID, method_id)
      end
    end

    alias MessageFrame = BodyFrame | HeaderFrame | Basic::Publish
  end
end
