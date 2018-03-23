module AvalancheMQ
  module AMQP
    abstract struct Frame
      getter type, channel
      def initialize(@type : Type, @channel : UInt16)
      end

      abstract def to_slice : Bytes

      def encode(io)
        io.write self.to_slice
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

      def self.decode(io)
        buf = uninitialized UInt8[7]
        io.read_fully(buf.to_slice)
        mem = MemoryIO.new(buf.to_slice)

        t = mem.read_byte
        raise ::IO::EOFError.new if t.nil?
        type = Type.new(t)
        channel = mem.read_uint16
        size = mem.read_uint32

        payload = Bytes.new(size + 1)
        io.read_fully(payload)

        frame_end = payload.at(size)
        if frame_end != 206
          raise InvalidFrameEnd.new("Frame-end was #{frame_end.to_s}, expected 206")
        end
        body = payload[0, size]
        case type
        when Type::Method then MethodFrame.decode(channel, body)
        when Type::Header then HeaderFrame.decode(channel, body)
        when Type::Body then BodyFrame.new(channel, body)
        when Type::Heartbeat then HeartbeatFrame.decode
        else GenericFrame.new(type, channel, body)
        end
      end
    end

    struct GenericFrame < Frame
      def initialize(@type : Type, @channel : UInt16,  @body : Bytes)
      end

      def to_slice
        super(@body)
      end
    end

    struct HeartbeatFrame < Frame
      def initialize
        @type = Type::Heartbeat
        @channel = 0_u16
      end

      def to_slice
        super(Slice(UInt8).new(0))
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
          #when 90_u16 then Tx.decode(channel, body)
        else
          puts "class-id #{class_id} not implemented yet"
          GenericFrame.new(Type::Method, channel, payload)
        end
      end
    end

    abstract struct Connection < MethodFrame
      def class_id
        10_u16
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
        else raise "Unknown method_id #{method_id}"
        end
      end

      struct Start < Connection
        def method_id
          10_u16
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + 1 + 1 + @mechanisms.bytesize + 1 + @locales.bytesize)
          body.write_byte(@version_major)
          body.write_byte(@version_minor)
          body.write_table(@server_props)
          body.write_long_string(@mechanisms)
          body.write_long_string(@locales)
          super(body.to_slice)
        end

        def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                       @server_props = { "Product" => "CloudAMQP" } of String => Field,
                       @mechanisms = "PLAIN", @locales = "en_US")
          super()
        end

        def self.decode(io)
          version_major = io.read_byte
          version_minor = io.read_byte
          server_props = io.read_table
          mech = io.read_long_string
          locales = io.read_long_string
          self.new(version_major, version_minor, server_props, mech, locales)
        end
      end

      struct StartOk < Connection
        getter client_props, mechanism, response, locale

        def method_id
          11_u16
        end

        def initialize(@client_props = {} of String => Field, @mechanism = "PLAIN",
                       @response = "\u0000guest\u0000guest", @locale = "en_US")
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @mechanism.bytesize + 4 + @response.bytesize + 1 + @locale.bytesize)
          body.write_table(@client_props)
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
        def method_id
          30_u16
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

      struct TuneOk < Connection
        getter channel_max, frame_max, heartbeat
        def method_id
          31_u16
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
        def method_id
          40_u16
        end

        def initialize(@vhost = "/", @reserved1 = "", @reserved2 = false)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @vhost.bytesize + 1 + @reserved1.bytesize + 1)
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

        def method_id
          41_u16
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
        def method_id
          50_u16
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
        def method_id
          51_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(io)
          self.new
        end
      end
    end

    abstract struct Channel < MethodFrame
      def class_id
        20_u16
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Open.decode(channel, body)
        when 11_u16 then OpenOk.decode(channel, body)
          #when 20_u16 then Flow.decode(channel, body)
          #when 21_u16 then FlowOk.decode(channel, body)
        when 40_u16 then Close.decode(channel, body)
        when 41_u16 then CloseOk.decode(channel, body)
        else raise "Unknown method_id #{method_id}"
        end
      end

      struct Open < Channel
        def method_id
          10_u16
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
        def method_id
          11_u16
        end

        getter reserved1

        def initialize(channel : UInt16, @reserved1 = "")
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(4 + @reserved1.bytesize)
          io.write_long_string @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_long_string
          OpenOk.new channel, reserved1
        end
      end

      struct Close < Channel
        def method_id
          40_u16
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
        def method_id
          41_u16
        end

        def to_slice
          super(Slice(UInt8).new(0))
        end

        def self.decode(channel, io)
          CloseOk.new channel
        end
      end
    end

    abstract struct Exchange < MethodFrame
      def class_id
        40_u16
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Declare.decode(channel, body)
        when 11_u16 then DeclareOk.decode(channel, body)
        when 20_u16 then Delete.decode(channel, body)
        when 21_u16 then DeleteOk.decode(channel, body)
        else raise "Unknown method_id #{method_id}"
        end
      end

      struct Declare < Exchange
        def method_id
          10_u16
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
        def method_id
          11_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(io)
          self.new
        end
      end

      struct Delete < Exchange
        def method_id
          20_u16
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
        def method_id
          21_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(io)
          self.new
        end
      end
    end

    abstract struct Queue < MethodFrame
      def class_id
        50_u16
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
        else raise "Unknown method_id #{method_id}"
        end
      end

      struct Declare < Queue
        def method_id
          10_u16
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
          passive = bits.bit(0)  == 1
          durable = bits.bit(1) == 1
          exclusive = bits.bit(2) == 1
          auto_delete = bits.bit(3) == 1
          no_wait = bits.bit(4) == 1
          args = io.read_table
          self.new channel, reserved1, name, passive, durable, exclusive, auto_delete, no_wait, args
        end
      end

      struct DeclareOk < Queue
        def method_id
          11_u16
        end

        def initialize(channel : UInt16, @queue_name : String, @message_count : UInt32, @consumer_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_short_string @queue_name
          io.write_int @message_count
          io.write_int @consumer_count
          super io.to_slice
        end

        def self.decode(io)
          raise "Not implemented"
        end
      end

      struct Bind < Queue
        def method_id
          20_u16
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
        def method_id
          21_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Delete < Queue
        def method_id
          40_u16
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
        def method_id
          41_u16
        end

        def initialize(channel : UInt16, @message_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new
          io.write_int @message_count
          super io.to_slice
        end

        def self.decode(io)
          raise "Not implemented"
        end
      end

      struct Unbind < Queue
        def method_id
          50_u16
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

      struct UnbindOk < Queue
        def method_id
          51_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(channel, io)
          self.new(channel)
        end
      end

      struct Purge < Queue
        def method_id
          30_u16
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
        def method_id
          31_u16
        end

        def initialize(channel : UInt16, @message_count : UInt32)
          super(channel)
        end

        def to_slice
          io = MemoryIO.new(2)
          io.write_int @message_count
          super io.to_slice
        end

        def self.decode(io)
          raise "Not implemented"
        end
      end
    end

    abstract struct Basic < MethodFrame
      def class_id
        60_u16
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Qos.decode(channel, body)
        when 11_u16 then QosOk.decode(channel, body)
        when 20_u16 then Consume.decode(channel, body)
        when 21_u16 then ConsumeOk.decode(channel, body)
        when 40_u16 then Publish.decode(channel, body)
        when 60_u16 then Deliver.decode(channel, body)
        when 70_u16 then Get.decode(channel, body)
        when 71_u16 then GetOk.decode(channel, body)
        when 72_u16 then GetEmpty.decode(channel, body)
        when 80_u16 then Ack.decode(channel, body)
        when 90_u16 then Reject.decode(channel, body)
        else raise "Unknown method_id #{method_id}"
        end
      end

      struct Publish < Basic
        def method_id
          40_u16
        end

        getter exchange, routing_key, mandatory, immediate
        def initialize(channel, @reserved1 : UInt16, @exchange : String,
                       @routing_key : String, @mandatory : Bool, @immediate : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
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
        def method_id
          60_u16
        end

        def initialize(channel, @consumer_tag : String, @delivery_tag : UInt64,
                       @redelivered : Bool, @exchange : String, @routing_key : String)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new
          io.write_short_string @consumer_tag
          io.write_int @delivery_tag
          io.write_bool @redelivered
          io.write_short_string @exchange
          io.write_short_string @routing_key
          super(io.to_slice)
        end

        def self.decode(channel, io)
          raise "Not implemented"
        end
      end

      struct Get < Basic
        def method_id
          70_u16
        end

        getter queue, no_ack
        def initialize(channel, @reserved1 : UInt16, @queue : String, @no_ack : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          queue = io.read_short_string
          no_ack = io.read_bool
          self.new channel, reserved1, queue, no_ack
        end
      end

      struct GetOk < Basic
        def method_id
          71_u16
        end

        def initialize(channel, @delivery_tag : UInt64, @redelivered : Bool,
                       @exchange : String, @routing_key : String, @message_count : UInt32)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new
          io.write_int @delivery_tag
          io.write_bool @redelivered
          io.write_short_string @exchange
          io.write_short_string @routing_key
          io.write_int @message_count
          super(io.to_slice)
        end

        def self.decode(channel, io)
          raise "Not implemented"
        end
      end

      struct GetEmpty < Basic
        def method_id
          72_u16
        end

        def initialize(channel, @reserved1 = 0_u16)
          super(channel)
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
        def method_id
          80_u16
        end

        getter :delivery_tag, :multiple
        def initialize(channel, @delivery_tag : UInt64, @multiple : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          multiple = io.read_bool
          self.new channel, delivery_tag, multiple
        end
      end

      struct Reject < Basic
        def method_id
          90_u16
        end

        getter :delivery_tag, :requeue
        def initialize(channel, @delivery_tag : UInt64, @requeue : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          requeue = io.read_bool
          self.new channel, delivery_tag, requeue
        end
      end

      struct Nack < Basic
        def method_id
          120_u16
        end

        getter :delivery_tag, :multiple, :requeue
        def initialize(channel, @delivery_tag : UInt64, @multiple : Bool, @requeue : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          delivery_tag = io.read_uint64
          multiple = io.read_bool
          requeue = io.read_bool
          self.new channel, delivery_tag, multiple, requeue
        end
      end

      struct Qos < Basic
        def method_id
          10_u16
        end

        getter prefetch_size, prefetch_count, global
        def initialize(channel, @prefetch_size : UInt32, @prefetch_count : UInt16, @global : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          prefetch_size = io.read_uint32
          prefetch_count = io.read_uint16
          global = io.read_bool
          self.new channel, prefetch_size, prefetch_count, global
        end
      end

      struct QosOk < Basic
        def method_id
          11_u16
        end

        def to_slice
          super Bytes.new(0)
        end

        def self.decode(io)
          self.new
        end
      end

      struct Consume < Basic
        def method_id
          20_u16
        end

        property queue, consumer_tag, no_local, no_ack, exclusive, no_wait, arguments
        def initialize(channel, @reserved1 : UInt16, @queue : String, @consumer_tag : String,
                       @no_local : Bool, @no_ack : Bool, @exclusive : Bool, @no_wait : Bool,
                       @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          raise "Not implemented"
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
        def method_id
          21_u16
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
          raise "Not implemented"
        end
      end
    end

    struct HeaderFrame < Frame
      getter body_size, properties
      def initialize(channel : UInt16, @class_id : UInt16, @weight : UInt16,
                     @body_size : UInt64, @properties : Properties)
        super(Type::Header, channel)
      end

      def to_slice
        body = AMQP::MemoryIO.new
        body.write_int @class_id
        body.write_int @weight
        body.write_int @body_size
        body.write @properties.to_slice
        super body.to_slice
      end

      def self.decode(channel, io)
        body = AMQP::MemoryIO.new(io, false)
        class_id = body.read_uint16
        weight = body.read_uint16
        body_size = body.read_uint64
        props = Properties.decode(body)
        self.new channel, class_id, weight, body_size, props
      end
    end

    struct BodyFrame < Frame
      getter body

      def initialize(@channel : UInt16,  @body : Bytes)
        @type = Type::Body
      end

      def to_slice
        super(@body)
      end
    end
  end
end
