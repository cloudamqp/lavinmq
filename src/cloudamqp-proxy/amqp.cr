module AMQP
  extend self

  def parse_frame(slice)
    type = IO::ByteFormat::BigEndian.decode(UInt8, slice[0, 1])
    type_name =
      case type
      when 1 then "METHOD"
      when 2 then "HEADER"
      when 3 then "BODY"
      when 8 then "HEARTBEAT"
      else type
      end

    channel = IO::ByteFormat::BigEndian.decode(UInt16, slice[1, 2])
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[3, 4])
    frame_end = IO::ByteFormat::BigEndian.decode(UInt8, slice[size + 7, 1])
    puts "type=#{type_name} channel=#{channel} size=#{size} end=#{frame_end}"
    if frame_end != 206
      raise InvalidFrameEnd.new("Frame-end was #{frame_end}, expected 206")
    end
    payload = slice[7, size + 7]
    decode_method(payload, size) if type == 1
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

  def decode_method(slice, size)
    class_id = IO::ByteFormat::BigEndian.decode(UInt16, slice[0, 2])
    method_id = IO::ByteFormat::BigEndian.decode(UInt16, slice[2, 2])
    clz = AMQPClass.new(class_id)
    methods = CLASS_METHODS[clz]?
    if methods.nil?
      puts "class=#{clz} method_id=#{method_id}"
      return
    end

    puts "class=#{clz} method=#{methods[method_id]}"
    args = slice[4, size]
    case clz
    when AMQPClass::Connection
      case CLASS_METHODS[clz][method_id]
      when :start
        version_major = IO::ByteFormat::BigEndian.decode(UInt8, args[0, 1])
        version_minor = IO::ByteFormat::BigEndian.decode(UInt8, args[1, 1])
        puts "version_major=#{version_major} version_minor=#{version_minor}"
        offset = 2
        off, hash = decode_table(args, offset)
        puts "server-properties=#{hash}"
        offset += off
        off, mech = decode_string(args, offset)
        puts "mechanisms=#{mech}"
        offset += off
        off, locales = decode_string(args, offset)
        puts "locales=#{locales}"
        offset += off
      when :start_ok
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

  def decode_table(slice, offset = 0)
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, 4]) + 4
    offset += 4
    hash = Hash(String, Bool | UInt32 | String | Hash(String, Bool | String | UInt32)).new
    while offset < size
      key_size = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
      offset += 1
      key = String.new(slice[offset, key_size])
      offset += key_size
      type = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
      offset += 1
      off, val = case type
                 when 'S' then decode_string(slice, offset)
                 when 'I' then decode_integer(slice, offset)
                 when 'F' then decode_hash(slice, offset)
                 when 't' then decode_boolean(slice, offset)
                 else raise "Unknown type: #{type}"
                 end
      offset += off
      hash[key] = val
    end
    { size, hash }
  end

  def decode_hash(slice, offset)
    off, val = decode_uint32(slice, offset)
    size = val + 4
    offset += off
    hash = Hash(String, Bool | String | UInt32).new
    while offset < size
      off, key_size = decode_uint8(slice, offset)
      offset += off
      key = String.new(slice[offset, key_size])
      offset += key_size
      type = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
      offset += 1
      off, val = case type
                 when 'S' then decode_string(slice, offset)
                 when 'I' then decode_integer(slice, offset)
                 when 't' then decode_boolean(slice, offset)
                 else raise "Unknown type: #{type}"
                 end
      offset += off
      hash[key] = val
    end
    { size, hash }
  end

  def decode_boolean(slice, offset)
    int = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
    raise "Unknown boolen value: #{int}" if int > 1
    val = (int == 1)
    { 1, val }
  end

  def decode_uint32(slice, offset)
    val = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, 4])
    { 4, val }
  end

  def decode_uint16(slice, offset)
    val = IO::ByteFormat::BigEndian.decode(UInt16, slice[offset, 2])
    { 2, val }
  end

  def decode_uint8(slice, offset)
    val = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
    { 1, val }
  end

  def decode_string(slice, offset)
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, 4])
    val = String.new(slice[offset + 4, size])
    { size + 4, val }
  end

  def decode_integer(slice, offset)
    size = 4
    val = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, size])
    { size, val }
  end

  class InvalidFrameEnd < Exception
  end
end
