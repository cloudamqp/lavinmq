require "http/client"
require "json"
require "./lib_crypto_ext"
require "./public_keys"
require "../rough_time"

module LavinMQ
  module Auth
    # Module-level accessor for the JWKS fetcher.
    # This allows components to access the fetcher without passing it through layers.
    @@jwks_fetcher : JWKSFetcher?

    def self.jwks_fetcher=(fetcher : JWKSFetcher?)
      @@jwks_fetcher = fetcher
    end

    def self.jwks_fetcher : JWKSFetcher?
      @@jwks_fetcher
    end

    # Fetches JWKS (JSON Web Key Set) from OAuth provider.
    # Handles OIDC discovery and JWKS parsing.
    class JWKSFetcher
      Log = LavinMQ::Log.for "jwks_fetcher"

      record JWKSResult, keys : Hash(String, String), ttl : Time::Span

      getter public_keys : PublicKeys

      def initialize(issuer_url : String, @default_cache_ttl : Time::Span)
        @issuer_url = issuer_url.chomp("/")
        @public_keys = PublicKeys.new
        @refresh_trigger = Channel(Nil).new
      end

      def refresh_loop
        retry_delay = 5.seconds
        max_retry_delay = 5.minutes

        loop do
          begin
            result = fetch_jwks
            @public_keys.update(result.keys, result.ttl)
            Log.info { "Refreshed JWKS with #{result.keys.size} key(s), TTL=#{result.ttl}" }
            retry_delay = 5.seconds

            wait_time = calculate_wait_time
            select
            when @refresh_trigger.receive
              Log.info { "Immediate JWKS refresh triggered" }
            when timeout(wait_time)
            end
          rescue ex
            Log.error(exception: ex) { "Failed to fetch JWKS, retrying in #{retry_delay}: #{ex.message}" }

            select
            when @refresh_trigger.receive
              Log.info { "Immediate JWKS refresh triggered during retry" }
              retry_delay = 5.seconds
            when timeout(retry_delay)
            end

            retry_delay = {retry_delay * 2, max_retry_delay}.min
          end
        end
      end

      private def calculate_wait_time : Time::Span
        if expires_at = @public_keys.expires_at
          remaining = expires_at - RoughTime.utc
          return remaining if remaining > 0.seconds
        end
        5.seconds # Default minimum wait if keys expired or not set
      end

      def fetch_jwks : JWKSResult
        # Discover jwks_uri from OIDC configuration
        oidc_config, _ = fetch_url("#{@issuer_url}/.well-known/openid-configuration")

        # Verify issuer matches per OpenID Connect Discovery 1.0 Section 4.3
        oidc_issuer = oidc_config["issuer"]?.try(&.as_s?)
        if oidc_issuer.nil? || oidc_issuer.chomp("/") != @issuer_url
          raise "OIDC issuer mismatch: expected #{@issuer_url}, got #{oidc_issuer}"
        end

        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s?) || raise "Missing jwks_uri in OIDC configuration"

        jwks, headers = fetch_url(jwks_uri)
        public_keys = extract_public_keys_from_jwks(jwks)
        ttl = extract_jwks_ttl(headers)
        JWKSResult.new(public_keys, ttl)
      end

      private def extract_public_keys_from_jwks(jwks : JSON::Any)
        jwks_array = jwks["keys"]?.try(&.as_a?) || raise "Missing or invalid keys array in JWKS response"

        public_keys = {} of String => String
        jwks_array.each_with_index do |key, idx|
          # Only process RSA keys (RFC 7517 Section 4.1)
          next unless key["kty"]?.try(&.as_s) == "RSA"
          next unless key["n"]? && key["e"]?
          # Skip keys not intended for signatures (RFC 7517 Section 4.2)
          use = key["use"]?.try(&.as_s)
          next if use && use != "sig"
          # Skip keys for other algorithms (RFC 7517 Section 4.4)
          alg = key["alg"]?.try(&.as_s)
          next if alg && alg != "RS256"
          kid = key["kid"]?.try(&.as_s) || "unknown-#{idx}"
          public_keys[kid] = to_pem(key["n"].as_s, key["e"].as_s)
        end
        public_keys
      end

      private def extract_jwks_ttl(headers) : Time::Span
        if cache_control = headers["Cache-Control"]?
          if match = cache_control.match(/max-age=(\d+)/)
            # JWKS header overrides lavinmq config
            return match[1].to_i.seconds
          end
        end
        @default_cache_ttl
      end

      # Converts JWKS RSA key components (n, e) to PEM format for JWT verification.
      private def to_pem(n : String, e : String) : String
        # Decode base64url-encoded modulus and exponent
        n_bytes = JWT::RS256Parser.base64url_decode_bytes(n)
        e_bytes = JWT::RS256Parser.base64url_decode_bytes(e)

        # Convert bytes to BIGNUMs
        modulus = LibCrypto.bn_bin2bn(n_bytes, n_bytes.size, nil)
        raise "Failed to create modulus" if modulus.null?

        exponent = LibCrypto.bn_bin2bn(e_bytes, e_bytes.size, nil)
        if exponent.null?
          LibCrypto.bn_free(modulus)
          raise "Failed to create exponent"
        end

        # Create RSA structure
        rsa = LibCrypto.rsa_new
        if rsa.null?
          LibCrypto.bn_free(modulus)
          LibCrypto.bn_free(exponent)
          raise "Failed to create RSA structure"
        end

        result = LibCrypto.rsa_set0_key(rsa, modulus, exponent, nil)
        if result != 1
          LibCrypto.bn_free(modulus)
          LibCrypto.bn_free(exponent)
          LibCrypto.rsa_free(rsa)
          raise "Failed to set RSA key components"
        end

        # Create a memory BIO
        bio = LibCrypto.BIO_new(LibCrypto.bio_s_mem)
        if bio.null?
          LibCrypto.rsa_free(rsa)
          raise "Failed to create BIO"
        end

        begin
          # Write the public key to the BIO in PEM format
          result = LibCrypto.pem_write_bio_rsa_pubkey(bio, rsa)
          if result != 1
            raise "Failed to write PEM"
          end

          # Get the length of data in the BIO (BIO_CTRL_PENDING = 10)
          length = LibCrypto.bio_ctrl(bio, 10, 0, nil)
          # RSA-16384 (max) is ~4KB in PEM format, 10KB covers all RSA keys with margin
          raise "Suspiciously large PEM length: #{length}" if length > 10_000

          # Read the PEM data from the BIO
          buffer = Bytes.new(length)
          LibCrypto.bio_read(bio, buffer, length.to_i32)

          String.new(buffer)
        ensure
          LibCrypto.BIO_free(bio)
          LibCrypto.rsa_free(rsa)
        end
      end

      private def fetch_url(url : String) : {JSON::Any, ::HTTP::Headers}
        uri = URI.parse(url)
        ::HTTP::Client.new(uri) do |client|
          client.connect_timeout = 5.seconds
          client.read_timeout = 10.seconds
          response = client.get(uri.request_target)
          if !response.success?
            raise "HTTP request failed with status #{response.status_code}: #{response.body}"
          end
          {JSON.parse(response.body), response.headers}
        end
      end
    end
  end
end
