module AMQP
  extend self

  def parse_frame(io)
    head_buf = uninitialized UInt8[7]
    bytes = io.read_fully(head_buf.to_slice)
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
    io.read_fully(frame.to_slice[7, size + 1])
    body = IO::Memory.new(frame)

    body.seek(size + 7)
    frame_end = body.read_byte
    body.seek(7)
    if frame_end != 206
      #puts "Frame-end was #{frame_end.to_s}, expected 206"
      raise InvalidFrameEnd.new("Frame-end was #{frame_end.to_s}, expected 206")
    end
    case type
    when 1
      decode_method(body, size)
    end
    frame.to_slice
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
        puts "client-properties=#{hash} mechanism=#{hash} response=#{auth} locale=#{locale}"
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

  #enum Types
  #  String       = 'S'
  #  Integer      = 'I'
  #  Time         = 'T'
  #  Decimal      = 'D'
  #  Hash         = 'F'
  #  Array        = 'A'
  #  Byte         = 'b'
  #  Float64      = 'd'
  #  Float32      = 'f'
  #  Signed_64bit = 'l'
  #  Signed_16bit = 's'
  #  Boolean      = 't'
  #  Byte_array   = 'x'
  #  Void         = 'V'
  #  Boolean_true  = 1
  #  Boolean_false = 0
  #end

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
