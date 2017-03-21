require "socket"
require "./cloudamqp-proxy/*"

class Proxy
  START_FRAME = UInt8.slice(65, 77, 81, 80, 0, 0, 9, 1)

  def copy(i, o)
    slice = Bytes.new(4096)
    loop do
      bytes = i.read(slice)
      return if bytes == 0

      parse_frame slice
      o.write slice[0, bytes]
    end
  end

  def handle_connection(socket)
    start = Bytes.new(8)
    bytes = socket.read_fully(start)

    if bytes != 8 || start != START_FRAME
      socket.write(START_FRAME)
      return
    end

    remote = TCPSocket.new("localhost", 5672)
    remote.write START_FRAME
    spawn copy(remote, socket)
    spawn copy(socket, remote)
    sleep
  rescue ex : InvalidFrameEnd
    puts ex
    #socket.write Slice[1, 0, 0]
  rescue ex : Errno
    puts ex
  ensure
    socket.close
    remote.close if remote
    puts "conn closed"
  end

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
    puts "class_id=#{clz} method_id=#{CLASS_METHODS[clz][method_id] || method_id}"
    args = slice[4, size]
    case clz
    when AMQPClass::Connection
      case CLASS_METHODS[clz][method_id]
      when :start
        version_major = IO::ByteFormat::BigEndian.decode(UInt8, args[0, 1])
        version_minor = IO::ByteFormat::BigEndian.decode(UInt8, args[1, 1])
        puts "version_major=#{version_major} version_minor=#{version_minor}"
        offset = 2
        offset += decode_hash(args, offset)
        offset += decode_string(args, offset)
        offset += decode_string(args, offset)
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

  def decode_hash(slice, offset = 0)
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, 4]) + 4
    offset += 4
    puts "table_size=#{size}"
    while offset < size
      key_size = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
      offset += 1
      key = String.new(slice[offset, key_size])
      offset += key_size
      puts "key=#{key}"
      type = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
      offset += 1
      offset +=
        case type
        when 'S'
          decode_string(slice, offset)
        when 'I'
          decode_integer(slice, offset)
        when 'F'
          decode_hash(slice, offset)
        when 't'
          decode_boolean(slice, offset)
        else
          raise "Unknown type: #{type}"
        end
      puts "offset=#{offset} hash_size=#{size}"
    end
    size
  end

  def decode_boolean(slice, offset)
    int = IO::ByteFormat::BigEndian.decode(UInt8, slice[offset, 1])
    raise "Unknown boolen value: #{int}" if int > 1
    val = int == 1
    puts "value=#{val}"
    1
  end

  def decode_string(slice, offset)
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, 4])
    val = String.new(slice[offset + 4, size])
    puts "value=#{val}"
    size + 4
  end

  def decode_integer(slice, offset)
    size = 4
    val = IO::ByteFormat::BigEndian.decode(UInt32, slice[offset, size])
    puts "value=#{val}"
    size
  end

  class InvalidFrameEnd < Exception
  end

  def start
    server = TCPServer.new("localhost", 1234)
    loop do
      puts "Waiting for connections"
      if socket = server.accept?
        puts "Accepted conn"
        # handle the client in a fiber
        spawn handle_connection(socket)
        puts "Spawned fiber"
      else
        # another fiber closed the server
        break
      end
    end
  end
end

Proxy.new.start
