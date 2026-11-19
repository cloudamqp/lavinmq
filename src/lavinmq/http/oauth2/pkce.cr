require "openssl"
require "../../../stdlib/openssl_digest_free"
require "base64"
require "random/secure"

module LavinMQ
  module HTTP
    module OAuth2
      module PKCE
        # Generates a PKCE code_verifier and code_challenge pair.
        # Returns {code_verifier, code_challenge}.
        def self.generate : {String, String}
          verifier = Random::Secure.urlsafe_base64(32)
          challenge = Base64.urlsafe_encode(
            OpenSSL::Digest.digest("SHA256", &.update(verifier)),
            padding: false
          )
          {verifier, challenge}
        end
      end
    end
  end
end
