module AMQPServer
  module AMQP
    abstract class Frame
      getter type, channel
      def initialize(@type : Type, @channel : UInt16)
      end

      abstract def to_slice : Bytes

      def encode(io)
        io.write self.to_slice
      end

      def to_slice(body : Bytes)
        io = MemoryIO.new(8 + body.size)
        io.write_byte @type.value
        io.write_int @channel
        io.write_int body.size.to_u32
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
        puts "type=#{type} channel=#{channel} size=#{size}"

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

    class GenericFrame < Frame
      def initialize(@type : Type, @channel : UInt16,  @body : Bytes)
      end

      def to_slice
        super(@body)
      end
    end

    class HeartbeatFrame < Frame
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

    abstract class MethodFrame < Frame
      def initialize(@channel : UInt16)
        @type = Type::Method
      end

      abstract def class_id : UInt16
      abstract def method_id : UInt16

      def to_slice(body : Bytes)
        io = MemoryIO.new(4 + body.size)
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
          #puts "class-id #{class_id} not implemented yet"
          GenericFrame.new(Type::Method, channel, payload)
        end
      end
    end

    abstract class Connection < MethodFrame
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

      class Start < Connection
        def method_id
          10_u16
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + 1 + 1 + @mechanisms.size + 1 + @locales.size)
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

      class StartOk < Connection
        getter client_props, mechanism, response, locale

        def method_id
          11_u16
        end

        def initialize(@client_props = {} of String => Field, @mechanism = "PLAIN",
                       @response = "\u0000guest\u0000guest", @locale = "en_US")
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @mechanism.size + 4 + @response.size + 1 + @locale.size)
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

      class Tune < Connection
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

      class TuneOk < Tune
        def method_id
          31_u16
        end
      end

      class Open < Connection
        getter vhost, reserved1, reserved2
        def method_id
          40_u16
        end

        def initialize(@vhost = "/", @reserved1 = "", @reserved2 = false)
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @vhost.size + 1 + @reserved1.size + 1)
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

      class OpenOk < Connection
        getter reserved1

        def method_id
          41_u16
        end

        def initialize(@reserved1 = "")
          super()
        end

        def to_slice
          body = AMQP::MemoryIO.new(1 + @reserved1.size)
          body.write_short_string(@reserved1)
          super(body.to_slice)
        end

        def self.decode(io)
          reserved1 = io.read_short_string
          OpenOk.new(reserved1)
        end
      end

      class Close < Connection
        def method_id
          50_u16
        end

        def initialize(@reply_code : UInt16, @reply_text : String, @failing_class_id : UInt16, @failing_method_id : UInt16)
          super()
        end

        def to_slice
          io = AMQP::MemoryIO.new(2 + 1 + @reply_text.size + 2 + 2)
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

      class CloseOk < Connection
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

    abstract class Channel < MethodFrame
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

      class Open < Channel
        def method_id
          10_u16
        end

        getter reserved1

        def initialize(channel : UInt16, @reserved1 = "")
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(1 + @reserved1.size)
          io.write_short_string @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_short_string
          Open.new channel, reserved1
        end
      end

      class OpenOk < Channel
        def method_id
          11_u16
        end

        getter reserved1

        def initialize(channel : UInt16, @reserved1 = "")
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(4 + @reserved1.size)
          io.write_long_string @reserved1
          super(io.to_slice)
        end

        def self.decode(channel, io)
          reserved1 = io.read_long_string
          OpenOk.new channel, reserved1
        end
      end

      class Close < Channel
        def method_id
          40_u16
        end

        getter reply_code, reply_text, classid, methodid

        def initialize(channel : UInt16, @reply_code : UInt16, @reply_text : String, @classid : UInt16, @methodid : UInt16)
          super(channel)
        end

        def to_slice
          io = AMQP::MemoryIO.new(2 + 1 + @reply_text.size + 2 + 2)
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

      class CloseOk < Channel
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

    abstract class Exchange < MethodFrame
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

      class Declare < Exchange
        def method_id
          10_u16
        end

        getter reserved1, exchange_name, exchange_type, passive, durable, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @exchange_name : String, @exchange_type : String, @passive : Bool, @durable : Bool, @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          type = io.read_short_string
          bits = io.read_byte
          passive = bits & (1 << 0) == 1
          durable = bits & (1 << 1) == 1
          auto_delete = bits & (1 << 2) == 1
          internal = bits & (1 << 3) == 1
          no_wait = bits & (1 << 4) == 1
          args = io.read_table
          self.new channel, reserved1, name, type, passive, durable, args
        end
      end

      class DeclareOk < Exchange
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

      class Delete < Exchange
        def method_id
          20_u16
        end

        getter reserved1, exchange_name, exchange_type, if_unused

        def initialize(channel : UInt16, @reserved1 : String, @exchange_name : String,
                       @if_unused : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          reserved1 = io.read_short_string
          name = io.read_short_string
          if_unused = io.read_bool
          nowait = io.read_bool
          self.new channel, reserved1, name, if_unused
        end
      end

      class DeleteOk < Exchange
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

    abstract class Queue < MethodFrame
      def class_id
        50_u16
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 10_u16 then Declare.decode(channel, body)
        when 11_u16 then DeclareOk.decode(channel, body)
        when 40_u16 then Delete.decode(channel, body)
        when 41_u16 then DeleteOk.decode(channel, body)
        else raise "Unknown method_id #{method_id}"
        end
      end

      class Declare < Queue
        def method_id
          10_u16
        end

        getter reserved1, queue_name, passive, durable, arguments

        def initialize(channel : UInt16, @reserved1 : UInt16, @queue_name : String,
                       @passive : Bool, @durable : Bool, @exclusive : Bool,
                       @auto_delete : Bool, @no_wait : Bool, @arguments : Hash(String, Field))
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          reserved1 = io.read_uint16
          name = io.read_short_string
          bits = io.read_byte
          passive = bits & (1 << 0) == 1
          durable = bits & (1 << 1) == 1
          exclusive = bits & (1 << 2) == 1
          auto_delete = bits & (1 << 3) == 1
          no_wait = bits & (1 << 4) == 1
          args = io.read_table
          self.new channel, reserved1, name, passive, durable, exclusive, auto_delete, no_wait, args
        end
      end

      class DeclareOk < Queue
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

      class Delete < Queue
        def method_id
          40_u16
        end

        getter reserved1, exchange_name, exchange_type, if_unused

        def initialize(channel : UInt16, @reserved1 : String, @queue_name : String,
                       @if_unused : Bool, @if_empty : Bool, @no_wait : Bool)
          super(channel)
        end

        def to_slice
          raise "Not implemented"
        end

        def self.decode(channel, io)
          reserved1 = io.read_short_string
          name = io.read_short_string
          bits = io.read_byte
          if_unused = bits & (1 << 0) == 1
          if_empty = bits & (1 << 1) == 1
          nowait = io.read_bool
          self.new channel, reserved1, name, if_unused, if_empty, nowait
        end
      end

      class DeleteOk < Exchange
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
    end

    abstract class Basic < MethodFrame
      def class_id
        60_u16
      end

      def self.decode(channel, body)
        method_id = body.read_uint16
        case method_id
        when 40_u16 then Publish.decode(channel, body)
        when 70_u16 then Get.decode(channel, body)
        when 71_u16 then GetOk.decode(channel, body)
        else raise "Unknown method_id #{method_id}"
        end
      end

      class Publish < Basic
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
          mandatory = bits & (1 << 0) == 1
          immediate = bits & (1 << 1) == 1
          self.new channel, reserved1, exchange, routing_key, mandatory, immediate
        end
      end

      class Get < Basic
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

      class GetOk < Basic
        def method_id
          71_u16
        end

        getter queue, no_ack
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
    end

    class HeaderFrame < Frame
      getter body_size
      def initialize(@channel : UInt16, @class_id : UInt16, @weight : UInt16,
                     @body_size : UInt64)
        @type = Type::Header
      end

      def to_slice
        body = AMQP::MemoryIO.new
        body.write_int @class_id
        body.write_int @weight
        body.write_int @body_size
        super body.to_slice
      end

      def self.decode(channel, io)
        body = AMQP::MemoryIO.new(io, false)
        class_id = body.read_uint16
        weight = body.read_uint16
        body_size = body.read_uint64
        flags = body.read_uint16
        self.new channel, class_id, weight, body_size
      end
    end

    class BodyFrame < Frame
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
