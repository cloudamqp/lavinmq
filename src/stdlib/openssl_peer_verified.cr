require "openssl"

lib LibSSL
  fun ssl_get_verify_result = SSL_get_verify_result(handle : SSL) : LibC::Long
end

abstract class OpenSSL::SSL::Socket < IO
  X509_V_OK = 0

  def peer_certificate_verified? : Bool
    cert = LibSSL.ssl_get_peer_certificate(@ssl)
    return false unless cert
    LibCrypto.x509_free(cert)
    LibSSL.ssl_get_verify_result(@ssl) == X509_V_OK
  end
end
