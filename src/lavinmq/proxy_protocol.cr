require "socket"
require "./connection_info"

module LavinMQ
  # https://raw.githubusercontent.com/haproxy/haproxy/master/doc/proxy-protocol.txt
  module ProxyProtocol
    class Error < Exception; end

    class InvalidSignature < Error; end

    class InvalidVersionCmd < Error; end

    class InvalidFamily < Error; end

    class UnsupportedTLVType < Error; end

    module V1
      # Examples:
      # PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n
      # PROXY TCP6 ffff:f...f:ffff ffff:f...f:ffff 65535 65535\r\n
      # PROXY UNKNOWN\r\n
      def self.parse(io)
        io.read_timeout = 15.seconds
        header = io.gets('\n', 107) || raise IO::EOFError.new

        src_addr = "127.0.0.1"
        dst_addr = "127.0.0.1"
        src_port = 0
        dst_port = 0

        i = 0
        header.split(' ') do |v|
          case i
          when 0 then raise InvalidSignature.new(v) if v != "PROXY"
          when 1 then nil
          when 2 then src_addr = v
          when 3 then dst_addr = v
          when 4 then src_port = v.to_i32
          when 5 then dst_port = v.to_i32
          else        break
          end
          i += 1
        end
        src = Socket::IPAddress.new(src_addr, src_port)
        dst = Socket::IPAddress.new(dst_addr, dst_port)
        ConnectionInfo.new(src, dst)
      ensure
        io.read_timeout = nil
      end
    end

    module V2
      SIGNATURE = StaticArray[13u8, 10u8, 13u8, 10u8, 0u8, 13u8, 10u8, 81u8, 85u8, 73u8, 84u8, 10u8]

      def self.parse(io)
        io.read_timeout = 15
        buffer = uninitialized UInt8[16]
        io.read(buffer.to_slice)
        signature = buffer.to_slice[0, 12]
        unless signature == SIGNATURE.to_slice
          raise InvalidSignature.new(signature.to_s)
        end
        ver_cmd = buffer[12]
        case ver_cmd
        when 32
          # LOCAL
        when 33
          # PROXY
        else
          raise InvalidVersionCmd.new ver_cmd.to_s
        end
        family = Family.from_value(buffer[13])
        length = IO::ByteFormat::NetworkEndian.decode(UInt16, buffer.to_slice[14, 2])

        bytes = Bytes.new(length)
        io.read_fully(bytes)

        header, pos = extract_address(family, bytes)
        bytes = bytes + pos
        header = extract_tlv(header, bytes)
        header
      ensure
        io.read_timeout = nil
      end

      private def self.extract_tlv(header : ConnectionInfo, bytes)
        pos = 0
        while pos < bytes.size
          type = TLVType.from_value(bytes[pos]); pos += 1
          length = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[pos, 2]); pos += 2
          value = bytes[pos, length]; pos += length
          case type
          when TLVType::SSL
            header = extract_tlv_ssl(header, value)
          else raise UnsupportedTLVType.new
          end
        end
        header
      end

      private def self.extract_tlv_ssl(header, bytes)
        pos = 0
        _client = SSLCLIENT.from_value(bytes[pos]); pos += 1
        verify = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[pos, 4]); pos += 4
        header.ssl = true
        header.ssl_verify = verify.zero?

        value = bytes + pos
        extract_tlv_sub_ssl(header, value)
      end

      private def self.extract_tlv_sub_ssl(header, bytes)
        pos = 0
        while pos < bytes.size
          ssl_type = SSLSubType.from_value(bytes[pos]); pos += 1
          ssl_len = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[pos, 2]); pos += 2
          ssl_value = String.new(bytes[pos, ssl_len]); pos += ssl_len
          case ssl_type
          when SSLSubType::VERSION then header.ssl_version = ssl_value
          when SSLSubType::CIPHER  then header.ssl_cipher = ssl_value
          when SSLSubType::KEY_ALG then header.ssl_key_alg = ssl_value
          when SSLSubType::SIG_ALG then header.ssl_sig_alg = ssl_value
          when SSLSubType::CN      then header.ssl_cn = ssl_value
          end
        end
        header
      end

      private def self.extract_address(family, bytes) : Tuple(ConnectionInfo, Int32)
        case family
        when Family::TCPv4
          src_addr = bytes[0, 4].map(&.to_u8).join(".")
          dst_addr = bytes[4, 4].map(&.to_u8).join(".")
          src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[8, 2])
          dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[10, 2])

          src = Socket::IPAddress.new(src_addr, src_port.to_i32)
          dst = Socket::IPAddress.new(dst_addr, dst_port.to_i32)
          {ConnectionInfo.new(src, dst), 12}
        when Family::TCPv6
          # TODO: should be optmizied, now converted from binary to string to binary
          src_addr = String.build(39) do |str|
            8.times do |i|
              str << ":" unless i.zero?
              str << bytes[i * 2, 2].hexstring
            end
          end
          dst_addr = String.build(39) do |str|
            8.times do |i|
              str << ":" unless i.zero?
              str << bytes[16 + i * 2, 2].hexstring
            end
          end
          src_port = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[32, 2])
          dst_port = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[34, 2])

          src = Socket::IPAddress.new(src_addr, src_port.to_i32)
          dst = Socket::IPAddress.new(dst_addr, dst_port.to_i32)
          {ConnectionInfo.new(src, dst), 36}
        else
          raise InvalidFamily.new family.to_s
        end
      end

      enum SSLCLIENT : UInt8
        SSL       = 0x01
        CERT_CONN = 0x02
        CERT_SESS = 0x04
      end

      enum TLVType : UInt8
        ALPN      = 0x01
        AUTHORITY = 0x02
        CRC32C    = 0x03
        NOOP      = 0x04
        UNIQUE_ID = 0x05
        SSL       = 0x20
        NETNS     = 0x30
      end

      enum SSLSubType : UInt8
        VERSION = 0x21
        CN      = 0x22
        CIPHER  = 0x23
        SIG_ALG = 0x24
        KEY_ALG = 0x25
      end

      enum Family : UInt8
        UNSPEC       =  0
        TCPv4        = 17
        UDPv4        = 18
        TCPv6        = 33
        UDPv6        = 34
        UNIXStream   = 49
        UNIXDatagram = 50
      end
    end
  end
end
