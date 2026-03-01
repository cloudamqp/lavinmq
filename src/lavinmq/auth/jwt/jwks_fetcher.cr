require "http/client"
require "json"
require "./lib_crypto_ext"
require "./public_keys"
require "../../rough_time"

module LavinMQ
  module Auth
    module JWT
      # Fetches JWKS (JSON Web Key Set) from OAuth provider.
      # Handles OIDC discovery and JWKS parsing.
      class JWKSFetcher
        Log = LavinMQ::Log.for "jwks_fetcher"

        record JWKSResult, keys : Hash(String, String), ttl : Time::Span

        struct OIDCConfiguration
          include JSON::Serializable

          property issuer : String
          property jwks_uri : String
          property authorization_endpoint : String?
          property token_endpoint : String?

          def initialize(*, @issuer : String, @jwks_uri : String,
                         @authorization_endpoint : String? = nil, @token_endpoint : String? = nil)
          end
        end

        struct JWK
          include JSON::Serializable

          property kty : String?
          property n : String?
          property e : String?
          property use : String?
          property alg : String?
          property kid : String?

          def initialize(*, @kty : String? = nil, @n : String? = nil, @e : String? = nil, @use : String? = nil, @alg : String? = nil, @kid : String? = nil)
          end
        end

        struct JWKSResponse
          include JSON::Serializable

          property keys : Array(JWK)

          def initialize(*, @keys : Array(JWK))
          end
        end

        getter public_keys : PublicKeys
        @stopped = BoolChannel.new(false)

        def initialize(issuer_url : URI, @default_cache_ttl : Time::Span)
          @issuer_url = issuer_url.to_s.chomp("/")
          @public_keys = PublicKeys.new
        end

        def cleanup
          @stopped.set(true)
        end

        def start_refresh_loop
          retry_delay = 5.seconds
          max_retry_delay = 5.minutes
          spawn do
            loop do
              begin
                result = fetch_jwks
                @public_keys.update(result.keys, result.ttl)
                Log.info { "Refreshed JWKS with #{result.keys.size} key(s), TTL=#{result.ttl}" }
                retry_delay = 5.seconds
                wait_time = calculate_wait_time
                select
                when @stopped.when_true.receive
                  break
                when timeout(wait_time)
                end
              rescue ex
                Log.error(exception: ex) { "Failed to fetch JWKS, retrying in #{retry_delay}: #{ex.message}" }
                sleep retry_delay
                retry_delay = {retry_delay * 2, max_retry_delay}.min
              end
            end
          end
        end

        private def calculate_wait_time : Time::Span
          if expires_at = @public_keys.expires_at
            remaining = expires_at - RoughTime.utc
            return remaining if remaining > 0.seconds
          end
          1.hour
        end

        def fetch_oidc_config : OIDCConfiguration
          body, _ = fetch_url("#{@issuer_url}/.well-known/openid-configuration")
          oidc_config = OIDCConfiguration.from_json(body)

          # Verify issuer matches per OpenID Connect Discovery 1.0 Section 4.3
          oidc_issuer = oidc_config.issuer
          if oidc_issuer.chomp("/") != @issuer_url
            raise "OIDC issuer mismatch: expected #{@issuer_url}, got #{oidc_issuer}"
          end

          oidc_config
        end

        def fetch_jwks : JWKSResult
          oidc_config = fetch_oidc_config

          body, headers = fetch_url(oidc_config.jwks_uri)
          jwks = JWKSResponse.from_json(body)
          public_keys = extract_public_keys_from_jwks(jwks)
          ttl = extract_jwks_ttl(headers)
          JWKSResult.new(public_keys, ttl)
        end

        private def extract_public_keys_from_jwks(jwks : JWKSResponse)
          jwks_array = jwks.keys

          public_keys = Hash(String, String).new
          jwks_array.each_with_index do |key, idx|
            # Only process RSA keys (RFC 7517 Section 4.1)
            next unless key.kty == "RSA"
            # Skip keys not intended for signatures (RFC 7517 Section 4.2)
            next if key.use != "sig"
            # Skip keys for other algorithms (RFC 7517 Section 4.4)
            next if key.alg != "RS256"
            next unless (n = key.n) && (e = key.e)
            kid = key.kid || "unknown-#{idx}"
            public_keys[kid] = to_pem(n, e)
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

        private def fetch_url(url : String) : {String, ::HTTP::Headers}
          uri = URI.parse(url)
          ::HTTP::Client.new(uri) do |client|
            client.connect_timeout = 5.seconds
            client.read_timeout = 10.seconds
            response = client.get(uri.request_target)
            if !response.success?
              raise "HTTP request failed with status #{response.status_code}: #{response.body}"
            end
            {response.body, response.headers}
          end
        end
      end
    end
  end
end
