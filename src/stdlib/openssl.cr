require "openssl/ssl/context"

# PR: https://github.com/crystal-lang/crystal/pull/13686
class OpenSSL::SSL::Context::Client
  # Don't set any default ciphers
  def initialize(method : LibSSL::SSLMethod = Context.default_method)
    super(method)

    self.verify_mode = OpenSSL::SSL::VerifyMode::PEER
    {% if LibSSL.has_method?(:x509_verify_param_lookup) %}
      self.default_verify_param = "ssl_server"
    {% end %}
  end
end

# PR: https://github.com/crystal-lang/crystal/pull/13695
class OpenSSL::SSL::Context::Server
  def initialize(method : LibSSL::SSLMethod = Context.default_method)
    super(method)

    {% if LibSSL.has_method?(:x509_verify_param_lookup) %}
      self.default_verify_param = "ssl_client"
    {% end %}
    set_tmp_ecdh_key(curve: LibCrypto::NID_X9_62_prime256v1)
    self.ciphers = CIPHERS_INTERMEDIATE
  end
end
