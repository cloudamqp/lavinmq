# Expose whether the peer's certificate passed verification.
#
# `OpenSSL::SSL::Socket#peer_certificate` returns the certificate the peer
# sent, but says nothing about whether it was verified. Authenticating by a
# client certificate needs both.

require "openssl"

lib LibSSL
  fun ssl_get_verify_result = SSL_get_verify_result(handle : SSL) : LibC::Long
end

abstract class OpenSSL::SSL::Socket < IO
  # :nodoc:
  X509_V_OK = 0

  # Returns `true` if the peer sent a certificate and it was verified against
  # the context's CA certificates.
  def peer_certificate_verified? : Bool
    cert = LibSSL.ssl_get_peer_certificate(@ssl)
    return false unless cert
    LibCrypto.x509_free(cert)
    LibSSL.ssl_get_verify_result(@ssl) == X509_V_OK
  end
end
