class Fiber
  def self.list(&blk : Fiber -> Nil)
    @@fibers.unsafe_each(&blk)
  end
end

require "openssl"

lib LibSSL
  type SSLCipher = Void*
  fun ssl_get_current_cipher = SSL_get_current_cipher(ssl : SSL) : SSLCipher
  fun ssl_cipher_get_name = SSL_CIPHER_get_name(cipher : SSLCipher) : UInt8*
  fun ssl_get_version = SSL_get_version(ssl : SSL) : UInt8*
end

abstract class OpenSSL::SSL::Socket
  # Returns the cipher currently in use
  def cipher : String
    String.new(LibSSL.ssl_cipher_get_name(LibSSL.ssl_get_current_cipher(@ssl)))
  end

  # Returns the TLS version currently in use
  def tls_version : String
    String.new(LibSSL.ssl_get_version(@ssl))
  end
end
