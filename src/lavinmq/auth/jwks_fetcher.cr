require "jwt"

module LavinMQ
  module Auth
    alias JWKSFetcher = ::JWT::JWKSFetcher
    alias PublicKeys = ::JWT::PublicKeys

    # Module-level accessor for the JWKS fetcher.
    @@jwks_fetcher : JWKSFetcher?

    def self.jwks_fetcher=(fetcher : JWKSFetcher?)
      @@jwks_fetcher = fetcher
    end

    def self.jwks_fetcher : JWKSFetcher?
      @@jwks_fetcher
    end
  end
end
