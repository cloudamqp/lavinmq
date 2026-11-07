{% unless flag?(:gc_none) %}
  require "openssl"
  require "./openssl_ktls"

  # Free the SSL object on close rather than in the finalizer, and drop the finalizer.
  # A fiber can be suspended inside SSL_read or SSL_write when another closes the
  # socket, so the last call to leave OpenSSL after the close frees it.
  abstract class OpenSSL::SSL::Socket < IO
    @ssl_calls = Atomic(Int32).new(0)
    @ssl_freed = Atomic(Bool).new(false)

    def unbuffered_read(slice : Bytes) : Int32
      @ssl_calls.add(1)
      begin
        previous_def
      ensure
        leave_ssl
      end
    end

    def unbuffered_write(slice : Bytes) : Nil
      @ssl_calls.add(1)
      begin
        previous_def
      ensure
        leave_ssl
      end
    end

    def unbuffered_close : Nil
      return if @closed
      @ssl_calls.add(1)
      begin
        previous_def
      ensure
        leave_ssl
      end
    end

    # These read the SSL object too, which is gone once closed
    {% for method in %w[alpn_protocol hostname cipher tls_version peer_certificate ktls_send? ktls_recv?] %}
      def {{ method.id }}
        @ssl_calls.add(1)
        begin
          raise IO::Error.new("Closed stream") if @ssl_freed.get
          previous_def
        ensure
          leave_ssl
        end
      end
    {% end %}

    def finalize
      LibSSL.ssl_free(@ssl) unless @ssl_freed.swap(true)
    end

    private def leave_ssl : Nil
      return unless @ssl_calls.sub(1) == 1 && @closed
      return if @ssl_freed.swap(true)
      LibSSL.ssl_free(@ssl)
      LibGC.register_finalizer_ignore_self(as(Void*),
        LibGC::Finalizer.new(Pointer(Void).null, Pointer(Void).null), nil, nil, nil)
    end
  end
{% end %}
