require "log"
require "socket"
require "digest/sha1"
require "crypto/subtle"

module LavinMQ
  module Clustering
    # Version byte bumped to 2: each replicated record is now prefixed with its
    # op-number (UInt64) and acks carry that op alongside the byte delta, so a v2
    # follower/leader cannot interoperate with a v1 one.
    Start = Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 2, 0, 0]

    # The verdict bytes the acceptor sends after checking the shared secret.
    AUTH_OK   = 0u8
    AUTH_FAIL = 1u8

    # The shared-secret handshake, used by both the data-replication path
    # (Client dials, Follower accepts) and the VR control mesh (ControlSession).
    # Kept here, in one place, so the wire format and — critically — the
    # constant-time secret comparison can't drift between those call sites.

    # Dialer side: send the shared secret, length-prefixed (UInt8 length + bytes).
    # The caller writes its protocol header first and flushes / relies on a sync
    # socket.
    def self.write_secret(io : IO, secret : String) : Nil
      io.write_bytes secret.bytesize.to_u8, IO::ByteFormat::LittleEndian
      io.write secret.to_slice
    end

    # Dialer side: read the acceptor's verdict. Returns on success; raises
    # AuthenticationError if the secret was rejected, IO::EOFError if the socket
    # closed first.
    def self.read_auth_response(io : IO) : Nil
      case io.read_byte
      when AUTH_OK # authenticated
      when AUTH_FAIL then raise AuthenticationError.new
      when nil       then raise IO::EOFError.new
      else                raise Error.new("Unexpected authentication response")
      end
    end

    # Acceptor side: read the dialer's length-prefixed secret, reply with the
    # verdict byte, and raise AuthenticationError (after sending the rejection) if
    # it doesn't match. Uses a constant-time comparison to avoid leaking the
    # secret through timing.
    def self.verify_secret(io : IO, secret : String) : Nil
      len = io.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
      client_secret = io.read_string(len)
      unless Crypto::Subtle.constant_time_compare(secret, client_secret)
        io.write_byte AUTH_FAIL
        io.flush
        raise AuthenticationError.new
      end
      io.write_byte AUTH_OK
      io.flush
    end

    class Error < Exception; end

    class InvalidStartHeaderError < Error
      def initialize(bytes)
        super("Invalid start header: #{bytes} #{String.new(bytes)} ")
      end
    end

    class AuthenticationError < Error
      def initialize
        super("Authentication error")
      end
    end
  end
end
