require "openssl"
require "./openssl_ktls"
require "./gc"

# Free the SSL object on close rather than in the finalizer, and drop the finalizer.
# A fiber can be suspended inside SSL_read or SSL_write when another closes the
# socket, so the SSL object is reference counted: the open socket holds one
# reference, every call into OpenSSL takes one, and whoever drops the last frees it.
abstract class OpenSSL::SSL::Socket < IO
  @ssl_refs = Atomic(Int32).new(1)
  @ssl_close_started = Atomic(Bool).new(false)

  def unbuffered_read(slice : Bytes) : Int32
    with_ssl { previous_def }
  end

  def unbuffered_write(slice : Bytes) : Nil
    with_ssl { previous_def }
  end

  def unbuffered_close : Nil
    return if @ssl_close_started.swap(true)
    begin
      previous_def
    ensure
      release_ssl # the socket's own reference
    end
  end

  # These read the SSL object too, which is gone once closed
  {% for method in %w[alpn_protocol hostname cipher tls_version peer_certificate ktls_send? ktls_recv?] %}
    def {{ method.id }}
      with_ssl { previous_def }
    end
  {% end %}

  def finalize
    LibSSL.ssl_free(@ssl) unless @ssl_refs.swap(0) == 0
  end

  private def with_ssl(&)
    acquire_ssl
    begin
      yield
    ensure
      release_ssl
    end
  end

  # Fails once the last reference is gone, in the same step as taking one
  private def acquire_ssl : Nil
    refs = @ssl_refs.get
    loop do
      raise IO::Error.new("Closed stream") if refs == 0
      refs, taken = @ssl_refs.compare_and_set(refs, refs + 1)
      break if taken
    end
  end

  private def release_ssl : Nil
    return unless @ssl_refs.sub(1) == 1
    LibSSL.ssl_free(@ssl)
    GC.remove_finalizer(self)
  end
end
