module AMQP
  abstract class Frame
    getter type, channel
    def initialize(@type : UInt8, @channel : UInt16)
    end

    enum Type : UInt8
      Method = 1
      Header = 2
      Body = 3
      Heartbeat = 8
    end

    abstract def body : Bytes

    def to_slice
      b = body
      io = IO::Memory.new(b.size + 8)
      io.write_byte(@type)
      io.write_bytes(@channel, IO::ByteFormat::BigEndian)
      io.write_bytes(b.size, IO::ByteFormat::BigEndian)
      io.write b
      io.write_byte(206_u8)
      io.to_slice
    end

    def self.parse(io)
      buf = uninitialized UInt8[7]
      io.read_fully(buf.to_slice)
      mem = IO::Memory.new(buf.to_slice)

      type = mem.read_byte
      raise IO::EOFError.new if type.nil?
      channel = mem.read_bytes(UInt16, IO::ByteFormat::BigEndian)
      size = mem.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      puts "type=#{type} channel=#{channel} size=#{size}"

      payload = Bytes.new(size + 1)
      io.read_fully(payload)

      frame_end = payload.at(size)
      if frame_end != 206
        raise InvalidFrameEnd.new("Frame-end was #{frame_end.to_s}, expected 206")
      end
      body = payload[0, size]
      case type
      when Type::Method then MethodFrame.parse(channel, payload)
      when Type::Header then HeaderFrame.parse(channel, payload)
      when Type::Body then BodyFrame.parse(channel, payload)
      when Type::Heartbeat then HeartbeatFrame.parse
      else
        GenericFrame.new(type, channel, body)
      end
    end
  end

  class GenericFrame < Frame
    getter type, channel
    def initialize(@type : UInt8, @channel : UInt16,  @payload : Bytes)
    end

    def body
      @payload
    end
  end

  class MethodFrame < Frame
    def initialize(@channel : UInt16, body : Bytes)
      @type = Type::Method.value
    end

    def self.parse(channel, payload)
      body = AMQP::IO.new(payload)
      class_id = body.read_uint16
      raise "Class-ID is not 10 in method frame" if class_id != 10
      method_id = body.read_uint16
      case method_id
      when 10 then nil
      end
    end
  end

  abstract class Connection < Frame
    def class_id
      10_u16
    end

    def initialize
      @type = Type::Method.value
      @channel = 0_u16
      @body = AMQP::IO.new
      @body.write_bytes class_id, IO::ByteFormat::BigEndian
      @body.write_bytes method_id, IO::ByteFormat::BigEndian
      @payload = @body.to_slice
      @size = @payload.size.to_u32
    end
  end

  class Start < Connection
    def method_id
      10_u16
    end

    def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                   @server_props = { "Product" => "CloudAMQP" } of String => Field,
                   @mechanisms = "PLAIN AMQPLAIN", @locales = "en_US")
      super()
      @body.write_byte(@version_major)
      @body.write_byte(@version_minor)
      @body.write_table(@server_props)
      @body.write_long_string(@mechanisms)
      @body.write_long_string(@locales)
      @payload = @body.to_slice
      @size = @payload.size.to_u32
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
      @body.write_table(@client_props)
      @body.write_short_string(@mechanism)
      @body.write_long_string(@response)
      @body.write_short_string(@locale)
      @payload = @body.to_slice
      @size = @payload.size.to_u32
    end
  end

  class Tune < Connection
    getter channel_max, frame_max, heartbeat
    def method_id
      30_u16
    end

    def initialize(@channel_max = 0_u16, @frame_max = 131072_u32, @heartbeat = 60_u16)
      super()
      @body.write_int(@channel_max)
      @body.write_int(@frame_max)
      @body.write_int(@heartbeat)
      @payload = @body.to_slice
      @size = @payload.size.to_u32
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
      @body.write_short_string(@vhost)
      @body.write_short_string(@reserved1)
      @body.write_bool(@reserved2)
      @payload = @body.to_slice
      @size = @payload.size.to_u32
    end
  end

  class OpenOk < Connection
    getter reserved1

    def method_id
      41_u16
    end

    def initialize(@reserved1 = "")
      super()
      @body.write_short_string(@reserved1)
      @payload = @body.to_slice
      @size = @payload.size.to_u32
    end
  end
end
