require "openssl/lib_ssl"

# Additional LibSSL bindings for certificate verification
lib LibSSL
  # Get the result of certificate verification
  # Returns 0 (X509_V_OK) if verification succeeded
  fun ssl_get_verify_result = SSL_get_verify_result(ssl : LibSSL::SSL) : LibC::Long

  # Replace the X509_STORE in an SSL_CTX
  # The SSL_CTX takes ownership of the store and will free the old store
  fun ssl_ctx_set_cert_store = SSL_CTX_set_cert_store(ctx : SSLContext, store : LibCrypto::X509_STORE)
end
