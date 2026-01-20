# kTLS-enabled SSL socket using SSL_set_fd for direct kernel access
#
# This implementation bypasses Crystal's custom BIO (which explicitly rejects kTLS)
# and uses SSL_set_fd() directly, allowing the kernel to intercept socket operations
# for hardware-accelerated TLS encryption/decryption.

require "openssl"
require "socket"

{% if compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 %}
  lib LibSSL
    # SSL_set_fd sets the file descriptor for the SSL connection
    # This is required for kTLS as it bypasses the custom BIO
    fun ssl_set_fd = SSL_set_fd(ssl : SSL, fd : LibC::Int) : LibC::Int
  end

  module OpenSSL::SSL
    # kTLS-enabled SSL socket using SSL_set_fd for direct kernel access.
    #
    # Unlike the standard `OpenSSL::SSL::Socket` which uses custom BIOs,
    # this socket uses `SSL_set_fd()` directly, allowing the kernel TLS
    # module to intercept socket operations for hardware-accelerated
    # encryption/decryption.
    #
    # Example:
    # ```
    # context = OpenSSL::SSL::Context::Server.new
    # context.certificate_chain = "cert.pem"
    # context.private_key = "key.pem"
    # context.enable_ktls
    #
    # tcp_server.accept do |client|
    #   ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(client, context)
    #   # kTLS is now active if supported
    # end
    # ```
    abstract class KTLSSocket < IO
      include IO::Buffered

      # If `#sync_close?` is `true`, closing this socket will close the underlying socket.
      property? sync_close : Bool

      # Returns `true` if this SSL socket has been closed.
      getter? closed : Bool = false

      @ssl : LibSSL::SSL
      @socket : TCPSocket

      # Server-side kTLS socket that performs SSL accept on initialization.
      class Server < KTLSSocket
        def initialize(@socket : TCPSocket, context : Context::Server, @sync_close : Bool = true)
          super(@socket, context)
          do_handshake { LibSSL.ssl_accept(@ssl) }
        end

        # Returns the `OpenSSL::X509::Certificate` the peer presented, if any.
        def peer_certificate : OpenSSL::X509::Certificate?
          raw_cert = LibSSL.ssl_get_peer_certificate(@ssl)
          if raw_cert
            begin
              OpenSSL::X509::Certificate.new(raw_cert)
            ensure
              LibCrypto.x509_free(raw_cert)
            end
          end
        end
      end

      protected def initialize(@socket : TCPSocket, context : Context)
        @sync_close = false
        @ssl = LibSSL.ssl_new(context)
        raise OpenSSL::Error.new("SSL_new") unless @ssl

        # CRITICAL: Use SSL_set_fd for kTLS support instead of custom BIO
        # This allows the kernel TLS module to intercept socket operations
        ret = LibSSL.ssl_set_fd(@ssl, @socket.fd)
        unless ret == 1
          LibSSL.ssl_free(@ssl)
          raise OpenSSL::Error.new("SSL_set_fd")
        end
      end

      def finalize
        LibSSL.ssl_free(@ssl)
      end

      private def do_handshake(&)
        loop do
          ret = yield
          return if ret == 1
          handle_error(ret, "SSL handshake")
        end
      end

      private def handle_error(ret : Int32, operation : String)
        error = LibSSL.ssl_get_error(@ssl, ret)
        case error
        when .want_read?
          wait_readable
        when .want_write?
          wait_writable
        when .zero_return?
          raise IO::EOFError.new
        else
          raise OpenSSL::SSL::Error.new(@ssl, ret, operation)
        end
      end

      private def wait_readable
        Crystal::EventLoop.current.wait_readable(@socket)
      end

      private def wait_writable
        Crystal::EventLoop.current.wait_writable(@socket)
      end

      def unbuffered_read(slice : Bytes) : Int32
        check_open
        return 0 if slice.empty?

        loop do
          ret = LibSSL.ssl_read(@ssl, slice, slice.size)
          if ret > 0
            return ret
          elsif ret == 0
            error = LibSSL.ssl_get_error(@ssl, ret)
            return 0 if error.zero_return?
            handle_error(ret, "SSL_read")
          else
            handle_error(ret, "SSL_read")
          end
        end
      end

      def unbuffered_write(slice : Bytes) : Nil
        check_open
        return if slice.empty?

        while slice.size > 0
          ret = LibSSL.ssl_write(@ssl, slice, slice.size)
          if ret > 0
            slice = slice[ret..]
          else
            handle_error(ret, "SSL_write")
          end
        end
      end

      def unbuffered_flush : Nil
        # No underlying buffered IO to flush when using SSL_set_fd
      end

      def unbuffered_rewind : Nil
        raise IO::Error.new("Can't rewind OpenSSL::SSL::KTLSSocket")
      end

      def unbuffered_close : Nil
        return if @closed
        @closed = true

        begin
          # Attempt graceful SSL shutdown
          2.times do
            ret = LibSSL.ssl_shutdown(@ssl)
            break if ret >= 0
            error = LibSSL.ssl_get_error(@ssl, ret)
            break unless error.want_read? || error.want_write?
            # Wait for socket to be ready and retry
            if error.want_read?
              wait_readable
            else
              wait_writable
            end
          end
        rescue IO::Error
          # Ignore errors during shutdown
        end

        @socket.close if @sync_close
      end

      private def check_open
        raise IO::Error.new("Closed stream") if @closed
      end

      # Returns the negotiated TLS protocol version (e.g., "TLSv1.3").
      def tls_version : String
        String.new(LibSSL.ssl_get_version(@ssl))
      end

      # Returns the negotiated cipher suite name.
      def cipher : String
        String.new(LibSSL.ssl_cipher_get_name(LibSSL.ssl_get_current_cipher(@ssl)))
      end

      # Returns the hostname provided through Server Name Indication (SNI).
      def hostname : String?
        if host_name = LibSSL.ssl_get_servername(@ssl, LibSSL::TLSExt::NAMETYPE_host_name)
          String.new(host_name)
        end
      end

      # Returns the negotiated ALPN protocol (e.g., "h2") or nil.
      def alpn_protocol : String?
        LibSSL.ssl_get0_alpn_selected(@ssl, out protocol, out len)
        String.new(protocol, len) unless protocol.null?
      end

      # Returns `true` if kTLS is active for sending data on this connection.
      def ktls_send? : Bool
        wbio = LibSSL.ssl_get_wbio(@ssl)
        return false if wbio.null?
        LibCrypto.bio_ctrl(wbio, LibCrypto::BIO_CTRL_GET_KTLS_SEND, 0, nil) == 1
      end

      # Returns `true` if kTLS is active for receiving data on this connection.
      def ktls_recv? : Bool
        rbio = LibSSL.ssl_get_rbio(@ssl)
        return false if rbio.null?
        LibCrypto.bio_ctrl(rbio, LibCrypto::BIO_CTRL_GET_KTLS_RECV, 0, nil) == 1
      end

      # Returns a string describing the kTLS status: "send+recv", "send", "recv", or nil.
      def ktls_status : String?
        send = ktls_send?
        recv = ktls_recv?
        case {send, recv}
        when {true, true}  then "send+recv"
        when {true, false} then "send"
        when {false, true} then "recv"
        else                    nil
        end
      end

      def local_address
        @socket.local_address
      end

      def remote_address
        @socket.remote_address
      end

      def read_timeout
        @socket.read_timeout
      end

      def read_timeout=(value)
        @socket.read_timeout = value
      end

      def write_timeout
        @socket.write_timeout
      end

      def write_timeout=(value)
        @socket.write_timeout = value
      end
    end
  end
{% else %}
  # Stub for OpenSSL < 3.0 - kTLS is not available
  module OpenSSL::SSL
    abstract class KTLSSocket < IO
      include IO::Buffered

      property? sync_close : Bool = false
      getter? closed : Bool = false

      class Server < KTLSSocket
        def initialize(socket : TCPSocket, context : Context::Server, sync_close : Bool = true)
          raise NotImplementedError.new("KTLSSocket requires OpenSSL 3.0+")
        end

        def peer_certificate : OpenSSL::X509::Certificate?
          nil
        end
      end

      def unbuffered_read(slice : Bytes) : Int32
        0
      end

      def unbuffered_write(slice : Bytes) : Nil
      end

      def unbuffered_flush : Nil
      end

      def unbuffered_rewind : Nil
        raise IO::Error.new("Can't rewind OpenSSL::SSL::KTLSSocket")
      end

      def unbuffered_close : Nil
      end

      def tls_version : String
        ""
      end

      def cipher : String
        ""
      end

      def hostname : String?
        nil
      end

      def alpn_protocol : String?
        nil
      end

      def ktls_send? : Bool
        false
      end

      def ktls_recv? : Bool
        false
      end

      def ktls_status : String?
        nil
      end

      def local_address
        nil
      end

      def remote_address
        nil
      end

      def read_timeout
        nil
      end

      def read_timeout=(value)
      end

      def write_timeout
        nil
      end

      def write_timeout=(value)
      end
    end
  end
{% end %}
