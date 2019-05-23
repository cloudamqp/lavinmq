require "socket"

module AvalancheMQ
  struct ProxyHeader
    class InvalidSignature < Exception; end
    class InvalidVersionCmd < Exception; end
    class InvalidFamily < Exception; end

    SIGNATURE_V1 = StaticArray[80u8, 82u8, 79u8, 88u8, 89u8]
    SIGNATURE_V2 = StaticArray[13u8, 10u8, 13u8, 10u8, 0u8, 13u8, 10u8, 81u8, 85u8, 73u8, 84u8, 10u8]

    getter src : Socket::IPAddress
    getter dst : Socket::IPAddress

    def initialize(@src, @dst)
    end

    def self.parse_v1(io)
      io.read_timeout = 5
      io.write_timeout = 5
      header = io.gets('\n', 107) || raise IO::EOFError.new

      src_addr = "255.255.255.255"
      dst_addr = "255.255.255.255"
      src_port = 0
      dst_port = 0

      i = 0
      header.split(' ') do |v|
        case i
        when 0 then raise InvalidSignature.new(v) if v != "PROXY"
        when 2 then src_addr = v
        when 3 then dst_addr = v
        when 4 then src_port = v.to_i32
        when 5 then dst_port = v.to_i32
        end
        i += 1
      end
      src = Socket::IPAddress.new(src_addr, src_port)
      dst = Socket::IPAddress.new(dst_addr, dst_port)
      io.read_timeout = nil
      return new(src, dst)
    end

    def self.parse_v2(io)
      buffer = uninitialized UInt8[16]
      io.read_fully(buffer.to_slice)
      signature = buffer.to_slice[0, 12]
      unless signature == SIGNATURE_V2
        raise InvalidSignature.new(signature.to_s)
      end
      ver_cmd = buffer[13]
      case ver_cmd
      when 32
        # LOCAL
      when 33
        # PROXY
      else
        raise InvalidVersionCmd.new ver_cmd.to_s
      end
      family = buffer[14]
      length = IO::ByteFormat::NetworkEndian.decode(UInt16, buffer.to_slice[15, 2])

      case family
      when Family::TCPv4
        buf = uninitialized UInt8[12]
        io.read_fully(buf.to_slice)
        src_addr = buf.to_slice[0, 4].map(&.to_u8).join(".")
        dst_addr = buf.to_slice[4, 4].map(&.to_u8).join(".")
        src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[8, 2])
        dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[10, 2])

        src = Socket::IPAddress.new(src_addr, src_port.to_i32)
        dst = Socket::IPAddress.new(dst_addr, dst_port.to_i32)
        io.skip(length - 12)
        return new(src, dst)
      #when Family::TCPv6
      #  buf = uninitialized UInt8[36]
      #  io.read_fully(buf.to_slice)
      #  src_addr = buf.to_slice[0, 16].map(&.to_u8).slice(2).join(":")
      #  dst_addr = buf.to_slice[16, 16].map(&.to_u8).join(".")
      #  src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[8, 2])
      #  dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[10, 2])

      #  src = Socket::IPAddress.new(src_addr, src_port)
      #  dst = Socket::IPAddress.new(dst_addr, dst_port)

      #  io.skip(length - 36)
      else raise InvalidFamily.new family.to_s
      end
    end

    enum Family : UInt8
      UNSPEC = 0
      TCPv4 = 17
      UDPv4 = 18
      TCPv6 = 33
      UDPv6 = 34
      UNIXStream = 49
      UNIXDatagram = 50
    end

  end
end
