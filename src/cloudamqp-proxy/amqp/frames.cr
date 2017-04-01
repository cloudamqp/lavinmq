module AMQP
  abstract class Frame
    getter type, channel
    def initialize(@type : UInt8, @channel : UInt16)
    end

    abstract def body : Bytes

    def to_slice
      b = self.body
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
      #when Type::Header then HeaderFrame.parse(channel, payload)
      #when Type::Body then BodyFrame.parse(channel, payload)
      #when Type::Heartbeat then HeartbeatFrame.parse
      else GenericFrame.new(type, channel, body)
      end
    end
  end

  class GenericFrame < Frame
    getter type, channel
    def initialize(@type : UInt8, @channel : UInt16,  @body : Bytes)
    end

    def body
      @body
    end
  end

  abstract class MethodFrame < Frame
    def initialize(@channel : UInt16, @body : Bytes)
      @type = Type::Method.value
    end

    def self.parse(channel, payload)
      body = AMQP::IO.new(payload)
      class_id = body.read_uint16
      raise "Class-ID is not 10 in method frame" if class_id != 10
      method_id = body.read_uint16
      case method_id
      when 10 then Start.parse(body)
      when 11 then StartOk.parse(body)
      when 30 then Tune.parse(body)
      when 31 then TuneOk.parse(body)
      when 40 then Open.parse(body)
      when 41 then OpenOk.parse(body)
      when 50 then Close.parse(body)
      when 51 then CloseOk.parse(body)
      else raise "Unknown method_id #{method_id}"
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
    end

    def body
      body = AMQP::IO.new
      body.write_bytes class_id, IO::ByteFormat::BigEndian
      body.write_bytes method_id, IO::ByteFormat::BigEndian
      body.to_slice
    end
  end

  class Start < Connection
    def method_id
      10_u16
    end

    def body
      body = AMQP::IO.new
      body.write_byte(@version_major)
      body.write_byte(@version_minor)
      body.write_table(@server_props)
      body.write_long_string(@mechanisms)
      body.write_long_string(@locales)
      body.to_slice
    end

    def initialize(@version_major = 0_u8, @version_minor = 9_u8,
                   @server_props = { "Product" => "CloudAMQP" } of String => Field,
                   @mechanisms = "PLAIN AMQPLAIN", @locales = "en_US")
      super()
    end

    def self.parse(io)
      version_major = io.read_byte
      version_minor = io.read_byte
      server_props = io.read_table
      mech = io.read_long_string
      locales = io.read_long_string
      puts "version_major=#{version_major} version_minor=#{version_minor} server-properties=#{hash} mechanisms=#{mech} locales=#{locales}"
      Start.new(version_major, version_minor, server_props, mech, locales)
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

    def body
      body = AMQP::IO.new
      body.write_table(@client_props)
      body.write_short_string(@mechanism)
      body.write_long_string(@response)
      body.write_short_string(@locale)
      body.to_slice
    end

    def self.parse(io)
      hash = io.read_table
      mech = io.read_short_string
      auth = io.read_long_string
      locale = io.read_short_string
      puts "client-properties=#{hash} mechanism=#{mech} response=#{auth} locale=#{locale}"
      StartOk.new(hash, mech, auth, locale)
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

    def body
      body = AMQP::IO.new
      body.write_int(@channel_max)
      body.write_int(@frame_max)
      body.write_int(@heartbeat)
      body.to_slice
    end

    def self.parse(io)
      channel_max = io.read_uint16
      frame_max = io.read_uint32
      heartbeat = io.read_uint16
      puts "channel_max=#{channel_max} frame_max=#{frame_max} heartbeat=#{heartbeat}"
      Tune.new(channel_max, frame_max, heartbeat)
      #io.seek(-6, IO::Seek::Current)
      #new_frame_max = UInt32.new(4096)
      #io.write_bytes(new_frame_max, IO::ByteFormat::BigEndian)
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

    def body
      body = AMQP::IO.new(1 + @vhost.size + 1 + @reserved1.size + 1)
      body.write_short_string(@vhost)
      body.write_short_string(@reserved1)
      body.write_bool(@reserved2)
      body.to_slice
    end

    def self.parse(io)
      vhost = io.read_short_string
      reserved1 = io.read_short_string
      reserved2 = io.read_bool
      puts "vhost=#{vhost} reserved1=#{reserved1} reserved2=#{reserved2}"
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

    def body
      body = AMQP::IO.new(@reserved1.size + 1)
      body.write_short_string(@reserved1)
      body.to_slice
    end

    def self.parse(io)
      reserved1 = io.read_short_string
      puts "reserved1=#{reserved1}"
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

    def body
      io = AMQP::IO.new(2 + 1 + @reply_text.size + 2 + 2)
      io.write_int(@reply_code)
      io.write_short_string(@reply_text)
      io.write_int(@failing_class_id)
      io.write_int(@failing_method_id)
      io.to_slice
    end

    def self.parse(io)
      code = io.read_uint16
      text = io.read_short_string
      failing_class_id = io.read_uint16
      failing_method_id = io.read_uint16
      Close.new(code, text, failing_class_id, failing_method_id)
    end
  end

  class CloseOk < Connection
    def method_id
      51_u16
    end

    def self.parse(io)
      CloseOk.new
    end
  end
end
