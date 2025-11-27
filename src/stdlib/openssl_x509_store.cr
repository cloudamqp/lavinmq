require "openssl"

module OpenSSL::X509
  class Store
    @store : LibCrypto::X509_STORE
    @assigned : Bool

    def initialize(ctx : LibSSL::SSLContext)
      @store = LibSSL.ssl_ctx_get_cert_store(ctx)
      raise OpenSSL::Error.new("SSL_CTX_get_cert_store") if @store.null?
      @assigned = true
    end

    def initialize
      @store = LibCrypto.x509_store_new
      raise OpenSSL::Error.new("X509_STORE_new") if @store.null?
      @assigned = false
    end

    def to_unsafe : LibCrypto::X509_STORE
      @store
    end

    def assign(ctx : LibSSL::SSLContext) : Nil
      LibSSL.ssl_ctx_set_cert_store(ctx, @store)
      @assigned = true
    end

    def finalize
      LibCrypto.x509_store_free(@store) unless @assigned
    end

    def enable_crl_checking
      flags = LibCrypto::X509VerifyFlags::CRL_CHECK |
              LibCrypto::X509VerifyFlags::CRL_CHECK_ALL |
              LibCrypto::X509VerifyFlags::EXTENDED_CRL_SUPPORT |
              LibCrypto::X509VerifyFlags::TRUSTED_FIRST
      ret = LibCrypto.x509_store_set_flags(@store, flags)
      raise OpenSSL::Error.new("X509_STORE_set_flags") unless ret == 1

      Log.info { "Enabled CRL checking on X509_STORE" }
    end

    # Load CA certificates from a PEM file into an X509_STORE
    def load_ca_certs_into_store(ca_cert_path : String)
      bio = LibCrypto.bio_new_file(ca_cert_path, "r")
      raise OpenSSL::Error.new("BIO_new_file: #{ca_cert_path}") if bio.null?

      begin
        cert_count = 0
        loop do
          cert = LibCrypto.pem_read_bio_x509(bio, nil, nil, nil)
          break if cert.null? # No more certificates

          begin
            ret = LibCrypto.x509_store_add_cert(@store, cert)
            raise OpenSSL::Error.new("X509_STORE_add_cert") unless ret == 1
            cert_count += 1
          ensure
            LibCrypto.x509_free(cert)
          end
        end

        Log.debug { "Loaded #{cert_count} CA certificate(s) from #{ca_cert_path}" }
      ensure
        LibCrypto.bio_free(bio)
      end
    end

    # Load a CRL from a PEM file into an X509_STORE
    # enable_flags: If true, enables CRL checking flags on the store
    def load_crl_into_store(crl_path : String)
      bio = LibCrypto.bio_new_file(crl_path, "r")
      raise OpenSSL::Error.new("BIO_new_file: #{crl_path}") if bio.null?

      begin
        crl = LibCrypto.pem_read_bio_x509_crl(bio, nil, nil, nil)
        raise OpenSSL::Error.new("PEM_read_bio_X509_CRL - CRL may be invalid: #{crl_path}") if crl.null?

        begin
          revoked_count = count_revoked_certificates(crl)
          Log.info { "Loading CRL with #{revoked_count} revoked certificate(s) from #{crl_path}" }

          ret = LibCrypto.x509_store_add_crl(@store, crl)
          raise OpenSSL::Error.new("X509_STORE_add_crl") unless ret == 1
        ensure
          LibCrypto.x509_crl_free(crl)
        end
      ensure
        LibCrypto.bio_free(bio)
      end
    end

    # Count the number of revoked certificates in a CRL
    private def count_revoked_certificates(crl : LibCrypto::X509CRL) : Int32
      revoked = LibCrypto.x509_crl_get_revoked(crl)
      return 0 if revoked.null?

      LibCrypto.openssl_sk_num(revoked)
    end
  end
end
