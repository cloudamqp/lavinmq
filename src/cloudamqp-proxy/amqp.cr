module AMQP
  extend self

  class IO < IO::Memory
    def write_short_string(str : String)
      raise "String too long" if str.bytesize > 255
      write_byte(str.bytesize.to_u8)
      write_utf8(str.to_slice)
    end

    def write_bool(val : Bool)
      write_byte(val ? 1_u8 : 0_u8)
    end

    def write_int(int)
      write_bytes(int, IO::ByteFormat::BigEndian)
    end

    def write_long_string(str : String)
      size = write_bytes(str.bytesize.to_u32, IO::ByteFormat::BigEndian)
      write_utf8(str.to_slice)
    end

    def write_table(hash : Hash(String, Field))
      tmp = AMQP::IO.new
      hash.each do |key, value|
        tmp.write_short_string(key)
        case value
        when String
          tmp.write_byte 'S'.ord.to_u8
          tmp.write_long_string(value)
        when Int
          tmp.write_byte 'I'.ord.to_u8
          tmp.write_int(value)
        when Hash(String, Field)
          tmp.write_byte 'F'.ord.to_u8
          tmp.write_table(value)
        when Bool
          tmp.write_byte 't'.ord.to_u8
          tmp.write_bool(value)
        when nil
          tmp.write_byte 'V'.ord.to_u8
          nil
        else raise "Unknown type: #{value.class}"
        end
      end
      write_int(tmp.size)
      write tmp.to_slice
    end
  end

  class Frame
    getter type, channel, size, payload
    def initialize(@type : UInt8, @channel : UInt16, @size : UInt32, @payload : Bytes)
    end

    enum Type : UInt8
      Method = 1
      Header = 2
      Body = 3
      Heartbeat = 8
    end

    def to_slice
      io = IO::Memory.new(@size + 8)
      io.write_byte(@type)
      io.write_bytes(@channel, IO::ByteFormat::BigEndian)
      io.write_bytes(@size, IO::ByteFormat::BigEndian)
      io.write @payload
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
      Frame.new(type, channel, size, payload[0, size])
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

  def parse_frame(io)
    head_buf = uninitialized UInt8[7]
    io.read_fully(head_buf.to_slice)
    head = IO::Memory.new(head_buf.to_slice)

    type = head.read_byte
    type_name =
      case type
      when 1 then "METHOD"
      when 2 then "HEADER"
      when 3 then "BODY"
      when 8 then "HEARTBEAT"
      else type
      end

    channel = head.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    size = head.read_bytes(UInt32, IO::ByteFormat::BigEndian)
    puts "type=#{type_name} channel=#{channel} size=#{size}"

    frame = Bytes.new(size + 8)
    frame.copy_from(head_buf.to_slice)
    io.read_fully(frame[7, size + 1])

    frame_end = frame.at(size + 7)
    if frame_end != 206
      raise InvalidFrameEnd.new("Frame-end was #{frame_end.to_s}, expected 206")
    end

    body = IO::Memory.new(frame)
    body.seek(7)
    case type
    when 1
      decode_method(body, size)
    end
    frame
  end

  def decode_method(io, size)
    class_id = io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    method_id = io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    clz = AMQPClass.new(class_id)
    methods = CLASS_METHODS[clz]?
      if methods.nil?
        puts "class=#{clz} method_id=#{method_id}"
        return
    end

    puts "class=#{clz} method=#{methods[method_id]}"
    offset = 0
    case clz
    when AMQPClass::Connection
      case CLASS_METHODS[clz][method_id]
      when :start
        version_major = io.read_byte
        version_minor = io.read_byte
        hash = decode_table(io)
        mech = decode_string(io)
        locales = decode_string(io)
        puts "version_major=#{version_major} version_minor=#{version_minor} server-properties=#{hash} mechanisms=#{mech} locales=#{locales}"
      when :start_ok
        hash = decode_table(io)
        mech = decode_short_string(io)
        auth = decode_string(io)
        locale = decode_short_string(io)
        puts "client-properties=#{hash} mechanism=#{mech} response=#{auth} locale=#{locale}"
      when :tune
        channel_max = io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        frame_max = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
        heartbeat = io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        puts "channel_max=#{channel_max} frame_max=#{frame_max} heartbeat=#{heartbeat}"
        io.seek(-6, IO::Seek::Current)

        new_frame_max = UInt32.new(4096)
        io.write_bytes(new_frame_max, IO::ByteFormat::BigEndian)
      when :tune_ok
      when :open
        vhost = decode_short_string(io)
        reserved1 = decode_short_string(io)
        reserved2 = decode_boolean(io)
        puts "vhost=#{vhost} reserved1=#{reserved1} reserved2=#{reserved2}"
      when :open_ok
      end
    end
  end

  def decode_table(io)
    size = io.read_bytes(UInt32, IO::ByteFormat::BigEndian) + 4
    hash = Hash(String, Field).new
    while io.pos < size
      key = decode_short_string(io)
      type = io.read_byte
      val = case type
            when 'S' then decode_string(io)
            when 'I' then decode_integer(io)
            when 'F' then decode_table(io)
            when 't' then decode_boolean(io)
            when 'V' then nil
            else raise "Unknown type: #{type}"
            end
      hash[key] = val
    end
    hash
  end

  def decode_short_string(io)
    size = io.read_byte.as(Int)
    io.read_string(size)
  end

  def decode_boolean(io)
    int = io.read_byte
    raise "Unknown boolen value: #{int}" if int.nil? || int > 1
    int == 1
  end

  def decode_uint32(io)
    io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
  end

  def decode_uint16(io)
    io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
  end

  def decode_string(io)
    size = io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
    io.read_string(size)
  end

  def decode_integer(io)
    io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
  end

  enum AMQPClass : UInt16
    Connection = 10
    Channel = 20
    Exchange = 40
    Queue = 50
    Basic = 60
    TX = 90
  end

  CLASS_METHODS = {
    AMQPClass::Connection => {
      10 => :start,
      11 => :start_ok,
      30 => :tune,
      31 => :tune_ok,
      40 => :open,
      41 => :open_ok,
      50 => :close,
      51 => :close_ok
    }, AMQPClass::Channel => {
      10 => :open,
      11 => :open_ok,
      20 => :flow,
      21 => :flow_ok,
      40 => :close,
      41 => :close_ok,
    }
  }

  class InvalidFrameEnd < Exception
  end

  alias Field = Nil |
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
end
