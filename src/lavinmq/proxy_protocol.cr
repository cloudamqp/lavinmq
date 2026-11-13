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

    # Bound how long a pre-auth connection may keep the accept fiber (and its FD)
    # parked while we wait for the first bytes. Without this the initial peek
    # blocks forever on a peer that completes the handshake but sends nothing.
    HANDSHAKE_TIMEOUT = 15.seconds

    # Reads a PROXY header, v1 or v2, and returns the connection info in it, or
    # nil if the connection doesn't start with a header
    def self.parse(io : IO, timeout : Time::Span = HANDSHAKE_TIMEOUT) : ConnectionInfo?
      read(io, timeout).try &.to_conn_info
    end

    # Reads a PROXY header, v1 or v2, or returns nil if the connection doesn't
    # start with one
    def self.read(io : IO, timeout : Time::Span = HANDSHAKE_TIMEOUT) : (V1 | V2)?
      io.read_timeout = timeout
      # A single peek can return fewer bytes than the full signature on a TCP
      # segment split (issue 2082), so we only need the bytes we have to match
      # the *start* of a signature. No supported protocol (AMQP "AMQP\0\0\9\1",
      # MQTT, HTTP) begins with "PROXY" or the V2 signature, so a prefix match
      # is unambiguous; the V1/V2 parsers then read the rest of the header.
      peeked = io.peek
      if header_prefix?(peeked, "PROXY".to_slice)
        V1.from_io(io)
      elsif header_prefix?(peeked, V2::Signature.to_slice)
        V2.from_io(io)
      end
    ensure
      io.read_timeout = nil
    end

    # True if the bytes seen so far match the start of *signature* (comparing
    # only the bytes we have, since a segment split may give us a partial peek).
    private def self.header_prefix?(peeked : Bytes, signature : Bytes) : Bool
      n = Math.min(peeked.size, signature.size)
      return false if n.zero?
      peeked[0, n] == signature[0, n]
    end

    struct V1
      getter src : Socket::IPAddress
      getter dst : Socket::IPAddress

      def initialize(@src : Socket::IPAddress, @dst : Socket::IPAddress)
      end

      def self.parse(io) : ConnectionInfo
        io.read_timeout = HANDSHAKE_TIMEOUT
        from_io(io).to_conn_info
      ensure
        io.read_timeout = nil
      end

      # Examples:
      # PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n
      # PROXY TCP6 ffff:f...f:ffff ffff:f...f:ffff 65535 65535\r\n
      # PROXY UNKNOWN\r\n
      def self.from_io(io : IO, format = nil) : self
        header = io.gets('\n', 107, chomp: true) || raise IO::EOFError.new

        src_addr = "127.0.0.1"
        dst_addr = "127.0.0.1"
        src_port = 0
        dst_port = 0

        i = 0
        header.split(' ') do |v|
          case i
          when 0 then raise InvalidSignature.new(v) if v != "PROXY"
          when 1 then raise InvalidFamily.new(v) unless v.in?("TCP4", "TCP6")
          when 2 then src_addr = v
          when 3 then dst_addr = v
          when 4 then src_port = v.to_i32
          when 5 then dst_port = v.to_i32
          else        break
          end
          i += 1
        end
        new(Socket::IPAddress.new(src_addr, src_port), Socket::IPAddress.new(dst_addr, dst_port))
      end

      def to_conn_info : ConnectionInfo
        ConnectionInfo.new(@src, @dst, proxied: true)
      end

      def to_io(io : IO, format = nil)
        case @src.family
        when .inet?
          io.print "PROXY TCP4 #{@src.address} #{@dst.address} #{@src.port} #{@dst.port}\r\n"
        when .inet6?
          io.print "PROXY TCP6 #{@src.address} #{@dst.address} #{@src.port} #{@dst.port}\r\n"
        else
          io.print "PROXY UNKNOWN\r\n"
        end
      end
    end

    struct V2
      enum Command : UInt8
        Local = 0x20
        Proxy = 0x21
      end

      # A type-length-value field, kept as read so that it's written back unchanged
      record TLV, type : UInt8, value : Bytes

      getter command : Command
      # Nil when the address family is UNSPEC
      getter src : Socket::IPAddress?
      getter dst : Socket::IPAddress?
      getter tlvs : Array(TLV)

      def initialize(@src : Socket::IPAddress?, @dst : Socket::IPAddress?,
                     @tlvs = Array(TLV).new, @command = Command::Proxy)
        if (src = @src) && (dst = @dst)
          raise ArgumentError.new("Mixed address families") unless src.family == dst.family
        elsif @src || @dst
          raise ArgumentError.new("Both or neither of src and dst must be given")
        end
      end

      def self.parse(io) : ConnectionInfo
        io.read_timeout = HANDSHAKE_TIMEOUT
        from_io(io).to_conn_info
      ensure
        io.read_timeout = nil
      end

      def self.from_io(io : IO, format = IO::ByteFormat::NetworkEndian) : self
        buffer = uninitialized UInt8[16]
        # read_fully so a 16-byte header split across TCP segments isn't short-read (issue 2082)
        io.read_fully(buffer.to_slice)
        signature = buffer.to_slice[0, 12]
        unless signature == Signature.to_slice
          raise InvalidSignature.new(signature.to_s)
        end
        command = Command.from_value?(buffer[12]) || raise InvalidVersionCmd.new(buffer[12].to_s)
        family = Family.from_value?(buffer[13]) || raise InvalidFamily.new(buffer[13].to_s)
        length = IO::ByteFormat::NetworkEndian.decode(UInt16, buffer.to_slice[14, 2])

        bytes = Bytes.new(length)
        io.read_fully(bytes)

        src, dst, pos = decode_addresses(family, bytes)
        new(src, dst, decode_tlvs(bytes + pos), command)
      end

      private def self.decode_addresses(family, bytes) : Tuple(Socket::IPAddress?, Socket::IPAddress?, Int32)
        case family
        when Family::UNSPEC
          {nil, nil, 0}
        when Family::TCPv4
          raise Error.new("Address block too short for TCPv4") if bytes.size < 12
          src = Socket::IPAddress.v4(v4_fields(bytes[0, 4]), port: decode_port(bytes[8, 2]))
          dst = Socket::IPAddress.v4(v4_fields(bytes[4, 4]), port: decode_port(bytes[10, 2]))
          {src, dst, 12}
        when Family::TCPv6
          raise Error.new("Address block too short for TCPv6") if bytes.size < 36
          src = Socket::IPAddress.v6(v6_fields(bytes[0, 16]), port: decode_port(bytes[32, 2]))
          dst = Socket::IPAddress.v6(v6_fields(bytes[16, 16]), port: decode_port(bytes[34, 2]))
          {src, dst, 36}
        else
          raise InvalidFamily.new family.to_s
        end
      end

      private def self.decode_tlvs(bytes) : Array(TLV)
        tlvs = Array(TLV).new
        pos = 0
        while pos < bytes.size
          raise Error.new("Truncated TLV") if bytes.size - pos < 3
          type = bytes[pos]
          length = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[pos + 1, 2]).to_i
          pos += 3
          raise Error.new("Truncated TLV") if bytes.size - pos < length
          tlvs << TLV.new(type, bytes[pos, length])
          pos += length
        end
        tlvs
      end

      private def self.v4_fields(bytes) : UInt8[4]
        UInt8.static_array(bytes[0], bytes[1], bytes[2], bytes[3])
      end

      private def self.v6_fields(bytes) : UInt16[8]
        StaticArray(UInt16, 8).new { |i| IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[i * 2, 2]) }
      end

      private def self.decode_port(bytes) : UInt16
        IO::ByteFormat::NetworkEndian.decode(UInt16, bytes)
      end

      def to_conn_info : ConnectionInfo
        src, dst = @src, @dst
        raise InvalidFamily.new("UNSPEC") unless src && dst
        conn_info = ConnectionInfo.new(src, dst, proxied: true)
        @tlvs.each do |tlv|
          case tlv.type
          when TLVType::SSL.value then apply_ssl(conn_info, tlv.value)
          else                         raise UnsupportedTLVType.new(tlv.type.to_s)
          end
        end
        conn_info
      end

      private def apply_ssl(conn_info, bytes)
        raise Error.new("Truncated SSL TLV") if bytes.size < 5
        # bytes[0] is the client field, which flags how the client connected
        verify = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[1, 4])
        conn_info.ssl = true
        conn_info.ssl_verify = verify.zero?
        pos = 5
        while pos < bytes.size
          raise Error.new("Truncated SSL sub-TLV") if bytes.size - pos < 3
          ssl_type = SSLSubType.from_value?(bytes[pos])
          ssl_len = IO::ByteFormat::NetworkEndian.decode(UInt16, bytes[pos + 1, 2]).to_i
          pos += 3
          raise Error.new("Truncated SSL sub-TLV") if bytes.size - pos < ssl_len
          ssl_value = String.new(bytes[pos, ssl_len])
          pos += ssl_len
          case ssl_type
          when SSLSubType::VERSION then conn_info.ssl_version = ssl_value
          when SSLSubType::CIPHER  then conn_info.ssl_cipher = ssl_value
          when SSLSubType::KEY_ALG then conn_info.ssl_key_alg = ssl_value
          when SSLSubType::SIG_ALG then conn_info.ssl_sig_alg = ssl_value
          when SSLSubType::CN      then conn_info.ssl_cn = ssl_value
          end
        end
      end

      # The header is always in network byte order, *format* is ignored
      def to_io(io : IO, format = IO::ByteFormat::NetworkEndian)
        src, dst = @src, @dst
        family, address_length =
          if src && dst
            case src.family
            when .inet?  then {Family::TCPv4, 12}
            when .inet6? then {Family::TCPv6, 36}
            else              raise InvalidFamily.new(src.family.to_s)
            end
          else
            {Family::UNSPEC, 0}
          end
        length = address_length + @tlvs.sum { |tlv| 3 + tlv.value.size }
        raise Error.new("Header too long") if length > UInt16::MAX

        io.write(Signature.to_slice)
        io.write_byte @command.value
        io.write_byte family.value
        io.write_bytes length.to_u16, IO::ByteFormat::NetworkEndian
        if src && dst
          write_address(io, src)
          write_address(io, dst)
          io.write_bytes src.port.to_u16, IO::ByteFormat::NetworkEndian
          io.write_bytes dst.port.to_u16, IO::ByteFormat::NetworkEndian
        end
        @tlvs.each do |tlv|
          io.write_byte tlv.type
          io.write_bytes tlv.value.size.to_u16, IO::ByteFormat::NetworkEndian
          io.write tlv.value
        end
        io.flush
      end

      private def write_address(io, addr : Socket::IPAddress)
        if addr.family.inet?
          fields = Socket::IPAddress.parse_v4_fields?(addr.address) || raise InvalidFamily.new(addr.address)
          io.write fields.to_slice
        else
          fields = Socket::IPAddress.parse_v6_fields?(addr.address) || raise InvalidFamily.new(addr.address)
          fields.each { |f| io.write_bytes f, IO::ByteFormat::NetworkEndian }
        end
      end

      Signature = StaticArray[13u8, 10u8, 13u8, 10u8, 0u8, 13u8, 10u8, 81u8, 85u8, 73u8, 84u8, 10u8]

      private enum TLVType : UInt8
        ALPN      = 0x01
        AUTHORITY = 0x02
        CRC32C    = 0x03
        NOOP      = 0x04
        UNIQUE_ID = 0x05
        SSL       = 0x20
        NETNS     = 0x30
      end

      private enum SSLSubType : UInt8
        VERSION = 0x21
        CN      = 0x22
        CIPHER  = 0x23
        SIG_ALG = 0x24
        KEY_ALG = 0x25
      end

      private enum Family : UInt8
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
