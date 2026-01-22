# kTLS (kernel TLS) support for OpenSSL 3.0+
# kTLS offloads TLS encryption/decryption to the kernel for improved performance

{% if compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 %}
  lib LibSSL
    # SSL_OP_ENABLE_KTLS enables kernel TLS if supported by the platform
    # Available in OpenSSL 3.0+
    SSL_OP_ENABLE_KTLS = 0x00000008_u64
  end

  lib LibCrypto
    # BIO_ctrl is the main BIO control function
    fun bio_ctrl = BIO_ctrl(bio : Bio*, cmd : LibC::Int, larg : LibC::Long, parg : Void*) : LibC::Long

    # BIO control commands for kTLS status (OpenSSL 3.0+)
    # BIO_get_ktls_send and BIO_get_ktls_recv are macros that call BIO_ctrl
    BIO_CTRL_GET_KTLS_SEND = 73
    BIO_CTRL_GET_KTLS_RECV = 76
  end

  lib LibSSL
    # SSL_get_rbio returns the read BIO for the SSL connection
    fun ssl_get_rbio = SSL_get_rbio(ssl : SSL) : LibCrypto::Bio*
    # SSL_get_wbio returns the write BIO for the SSL connection
    fun ssl_get_wbio = SSL_get_wbio(ssl : SSL) : LibCrypto::Bio*
  end

  module OpenSSL::SSL
    class Context
      # Enables kTLS (kernel TLS) for this context if supported by the platform.
      # kTLS offloads TLS record layer encryption/decryption to the kernel,
      # reducing CPU usage and improving throughput.
      #
      # Requirements:
      # - OpenSSL 3.0+
      # - Linux kernel 4.13+ with TLS module loaded
      # - Supported cipher suites (AES-GCM, ChaCha20-Poly1305)
      #
      # Returns true if kTLS option is set, false if kTLS is not available.
      def enable_ktls : Bool
        opt = add_options(OpenSSL::SSL::Options.new(LibSSL::SSL_OP_ENABLE_KTLS))
        (opt.to_u64 & 0x00000008_u64 != 0)
      end
    end
  end
{% else %}
  # kTLS is not available for OpenSSL < 3.0
  module OpenSSL::SSL
    class Context
      def enable_ktls : Bool
        false
      end
    end
  end
{% end %}

module OpenSSL::SSL
  KTLS_AVAILABLE = {% if compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 %}true{% else %}false{% end %}
end
