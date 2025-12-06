require "http/client"
require "json"
require "./jwt"
require "./lib_crypto_ext"

module LavinMQ
  module Auth
    # Fetches JWKS (JSON Web Key Set) from OAuth provider and decodes JWT tokens.
    # Handles OIDC discovery, JWKS parsing, and JWT signature verification.
    class JWKSFetcher
      Log = LavinMQ::Log.for "jwks_fetcher"

      def initialize(@issuer_url : String, @default_cache_ttl : Time::Span)
      end

      def fetch_jwks : {Hash(String, String), Time::Span}
        # Discover jwks_uri from OIDC configuration
        oidc_config, _ = fetch_url("#{@issuer_url.chomp("/")}/.well-known/openid-configuration")
        jwks_uri = oidc_config["jwks_uri"]?.try(&.as_s?) || raise "Missing jwks_uri in OIDC configuration"

        jwks, headers = fetch_url(jwks_uri)
        public_keys = extract_public_keys_from_jwks(jwks)
        ttl = extract_jwks_ttl(headers)
        {public_keys, ttl}
      end

      def decode_token(token : String, public_keys : Hash(String, String)) : JWT::Token
        kid = JWT::RS256Parser.decode_header(token)["kid"]?.try(&.as_s) rescue nil
        # If we know the kid matches a key we can avoid iterating through all keys
        if kid && public_keys[kid]?
          return JWT::RS256Parser.decode(token, public_keys[kid], verify: true)
        end

        public_keys.each_value do |key|
          return JWT::RS256Parser.decode(token, key, verify: true)
        rescue JWT::DecodeError | JWT::VerificationError | Base64::Error
        end
        raise JWT::VerificationError.new("Could not verify JWT with any key")
      end

      private def extract_public_keys_from_jwks(jwks : JSON::Any)
        jwks_array = jwks["keys"]?.try(&.as_a?) || raise "Missing or invalid keys array in JWKS response"

        public_keys = {} of String => String
        jwks_array.each_with_index do |key, idx|
          next unless key["n"]? && key["e"]?
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
        n_bytes = base64url_decode_bytes(n)
        e_bytes = base64url_decode_bytes(e)

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

      private def base64url_decode_bytes(input : String) : Bytes
        padded = case input.size % 4
                 when 2 then input + "=="
                 when 3 then input + "="
                 else        input
                 end
        Base64.decode(padded.tr("-_", "+/"))
      end
    end
  end
end
