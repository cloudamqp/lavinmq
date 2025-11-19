require "openssl"

# Extensions to OpenSSL::SSL::Context for X509_STORE operations
# needed for CRL (Certificate Revocation List) support
module OpenSSL::SSL
  class Context
    # Loads CRL (Certificate Revocation List) from a file into the X509 store
    # and enables CRL checking for the entire certificate chain
    def load_crl(file_path : String)
      # Get the X509_STORE from the SSL context
      store = LibSSL.ssl_ctx_get_cert_store(@handle)
      raise OpenSSL::Error.new("SSL_CTX_get_cert_store") if store.null?

      # Read CRL from PEM file
      file = LibCrypto.bio_new_file(file_path, "r")
      raise OpenSSL::Error.new("BIO_new_file") if file.null?

      begin
        # Read CRL from the BIO
        crl = LibCrypto.pem_read_bio_x509_crl(file, nil, nil, nil)
        raise OpenSSL::Error.new("PEM_read_bio_X509_CRL - CRL file may be invalid") if crl.null?

        begin
          # Add CRL to the X509_STORE
          ret = LibCrypto.x509_store_add_crl(store, crl)
          raise OpenSSL::Error.new("X509_STORE_add_crl") unless ret == 1

          # Enable CRL checking for certificates
          # CRL_CHECK checks the end-entity certificate
          # Use CRL_CHECK_ALL to check the entire chain (requires CRLs for all certs)
          flags = LibCrypto::X509VerifyFlags::CRL_CHECK |
                  LibCrypto::X509VerifyFlags::EXTENDED_CRL_SUPPORT
          ret = LibCrypto.x509_store_set_flags(store, flags)
          raise OpenSSL::Error.new("X509_STORE_set_flags") unless ret == 1
        ensure
          LibCrypto.x509_crl_free(crl)
        end
      ensure
        LibCrypto.bio_free(file)
      end
    end
  end
end

# Add missing LibCrypto function bindings
lib LibCrypto
  alias X509CRL = Void*

  # Create a BIO from a file
  fun bio_new_file = BIO_new_file(
    filename : LibC::Char*,
    mode : LibC::Char*
  ) : Bio*

  # Free a BIO
  fun bio_free = BIO_free(bio : Bio*) : LibC::Int

  # Read a CRL from a PEM file
  fun pem_read_bio_x509_crl = PEM_read_bio_X509_CRL(
    bp : Bio*,
    x : X509CRL*,
    cb : Void*,
    u : Void*
  ) : X509CRL

  # Add a CRL to the X509_STORE
  fun x509_store_add_crl = X509_STORE_add_crl(
    ctx : X509_STORE,
    x : X509CRL
  ) : LibC::Int

  # Free a CRL
  fun x509_crl_free = X509_CRL_free(a : X509CRL)

  # Set X509 verification flags on the store
  fun x509_store_set_flags = X509_STORE_set_flags(
    ctx : X509_STORE,
    flags : X509VerifyFlags
  ) : LibC::Int
end
