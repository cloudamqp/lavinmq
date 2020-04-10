require "socket"

module AvalancheMQ
  module ProxyProtocol
    class InvalidSignature < Exception; end
    class InvalidVersionCmd < Exception; end
    class InvalidFamily < Exception; end

    struct Header
      getter src : Socket::IPAddress
      getter dst : Socket::IPAddress

      def initialize(@src, @dst)
      end

      def self.local
        src = Socket::IPAddress.new("127.0.0.1", 0)
        dst = Socket::IPAddress.new("127.0.0.1", 0)
        new(src, dst)
      end
    end

    module V1
      # Examples:
      # PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n
      # PROXY TCP6 ffff:f...f:ffff ffff:f...f:ffff 65535 65535\r\n
      # PROXY UNKNOWN\r\n
      def self.parse(io)
        io.read_timeout = 15
        header = io.gets('\n', 107) || raise IO::EOFError.new

        src_addr = "127.0.0.1"
        dst_addr = "127.0.0.1"
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
          else nil
          end
          i += 1
        end
        src = Socket::IPAddress.new(src_addr, src_port)
        dst = Socket::IPAddress.new(dst_addr, dst_port)
        Header.new(src, dst)
      ensure
        io.read_timeout = nil
      end

      def self.encode(header, io)
        ipv = header.src.family.inet6? ? 6 : 4
        io.print "PROXY TCP#{ipv} #{header.src.address} #{header.dst.address} #{header.src.port} #{header.dst.port}\r\n"
        io.flush
      end
    end

    module V2
      SIGNATURE = StaticArray[13u8, 10u8, 13u8, 10u8, 0u8, 13u8, 10u8, 81u8, 85u8, 73u8, 84u8, 10u8]

      def self.parse(io)
        io.read_timeout = 15
        buffer = uninitialized UInt8[16]
        io.read(buffer.to_slice)
        signature = buffer.to_slice[0, 12]
        unless signature == SIGNATURE
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
          io.read(buf.to_slice)
          src_addr = buf.to_slice[0, 4].map(&.to_u8).join(".")
          dst_addr = buf.to_slice[4, 4].map(&.to_u8).join(".")
          src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[8, 2])
          dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[10, 2])

          src = Socket::IPAddress.new(src_addr, src_port.to_i32)
          dst = Socket::IPAddress.new(dst_addr, dst_port.to_i32)
          io.skip(length - 12)
          Header.new(src, dst)
          #when Family::TCPv6
          #  buf = uninitialized UInt8[36]
          #  io.read(buf.to_slice)
          #  src_addr = buf.to_slice[0, 16].map(&.to_u8).slice(2).join(":")
          #  dst_addr = buf.to_slice[16, 16].map(&.to_u8).join(".")
          #  src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[8, 2])
          #  dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, buf.to_slice[10, 2])

          #  src = Socket::IPAddress.new(src_addr, src_port)
          #  dst = Socket::IPAddress.new(dst_addr, dst_port)

          #  io.skip(length - 36)
        else raise InvalidFamily.new family.to_s
        end
      ensure
        io.read_timeout = nil
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
end
