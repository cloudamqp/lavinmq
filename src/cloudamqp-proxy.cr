require "socket"
require "./cloudamqp-proxy/*"

class Proxy
  START_FRAME = UInt8.slice(65, 77, 81, 80, 0, 0, 9, 1)

  def handle_connection(socket)
    start = Bytes.new(8)
    bytes = socket.read(start)

    if bytes != 8 || start != START_FRAME
      socket.write(START_FRAME)
      socket.close
      return
    end

    TCPSocket.open("localhost", 5672) do |remote|
      remote.write START_FRAME
      slice = Bytes.new(4096)
      loop do
        ready = IO.select([socket, remote], nil, nil)
        puts ready

        ready.each do |s|
          case s
          when socket
            bytes = socket.read(slice)
            return if bytes == 0
            parse_frame slice

            remote.write slice[0, bytes]
          when remote
            bytes = remote.read(slice)
            return if bytes == 0

            parse_frame slice
            socket.write slice[0, bytes]
          end
        end
      end
    end
  rescue ex : InvalidFrameEnd
    puts ex
    #socket.write Slice[1, 0, 0]
  rescue ex : Errno
    puts ex
  ensure
    socket.close
    puts "conn closed"
  end

  def parse_frame(slice)
    type = IO::ByteFormat::BigEndian.decode(UInt8, slice[0,1])
    type_name =
      case type
      when 1 then "METHOD"
      when 2 then "HEADER"
      when 3 then "BODY"
      when 8 then "HEARTBEAT"
      else type
      end

    channel = IO::ByteFormat::BigEndian.decode(UInt16, slice[1, 3])
    size = IO::ByteFormat::BigEndian.decode(UInt32, slice[3, 7])
    frame_end = IO::ByteFormat::BigEndian.decode(UInt8, slice[size + 7, size + 8])
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
      11 => :open_ok
    }
  }

  def decode_method(slice, size)
    class_id = IO::ByteFormat::BigEndian.decode(UInt16, slice[0, 2])
    method_id = IO::ByteFormat::BigEndian.decode(UInt16, slice[2, 4])
    clz = AMQPClass.new(class_id)
    puts "class_id=#{clz} method_id=#{CLASS_METHODS[clz][method_id] || method_id}"
    args = slice[4, size]
    case clz
    when AMQPClass::Connection
      case CLASS_METHODS[clz][method_id]
      when :start
        version_major = IO::ByteFormat::BigEndian.decode(UInt8, args[0, 1])
        version_minor = IO::ByteFormat::BigEndian.decode(UInt8, args[1, 2])
        decode_field_table(args[2, args.size])
      end
    end
  end

  
  def decode_field_table(slice)
    slice.read_bytes(UInt16, IO::ByteFormat::LittleEndian)

    IO::ByteFormat::BigEndian.decode(UInt8, args[1, 2])
  end

  def decode_header(slice)
  end

  class InvalidFrameEnd < Exception
  end

  def start
    server = TCPServer.new("localhost", 1234)
    loop do
      if socket = server.accept?
        puts "Accepted conn"
        # handle the client in a fiber
        spawn handle_connection(socket)
      else
        # another fiber closed the server
        break
      end
    end
  end
end

Proxy.new.start
