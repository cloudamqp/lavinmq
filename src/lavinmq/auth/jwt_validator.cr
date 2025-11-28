require "jwt"
require "http/client"
require "json"
require "openssl"
require "base64"
require "../config"

module LavinMQ
  # Token claims extracted from JWT
  record TokenClaims,
    sub : String,                    # Subject (user identifier)
    iss : String?,                   # Issuer
    aud : String | Array(String)?,   # Audience
    exp : Int64?,                    # Expiration time
    iat : Int64?,                    # Issued at
    nbf : Int64?,                    # Not before
    scope : String?,                 # OAuth scopes
    email : String?,                 # Email claim
    preferred_username : String?,    # Preferred username
    permissions : Hash(String, NamedTuple(configure: String, write: String, read: String))?, # Custom permissions
    custom_claims : Hash(String, JSON::Any) = Hash(String, JSON::Any).new # Other claims

  # JSON Web Key from JWKS endpoint
  record JWK,
    kid : String?,              # Key ID
    kty : String,               # Key type (RSA, EC, etc.)
    use : String?,              # Public key use (sig, enc)
    alg : String?,              # Algorithm
    n : String?,                # RSA modulus
    e : String?,                # RSA exponent
    x : String?,                # EC x coordinate
    y : String?,                # EC y coordinate
    crv : String?,              # EC curve
    raw_json : JSON::Any        # Store raw JSON for debugging

  # JWKS client that fetches and caches public keys
  class JWKSClient
    Log = ::Log.for("lavinmq.auth.jwks")

    @cache : Hash(String, JWK)
    @last_fetch : Time?
    @fetch_mutex : Mutex

    def initialize(@jwks_url : String, @cache_ttl : Time::Span = 1.hour)
      @cache = Hash(String, JWK).new
      @fetch_mutex = Mutex.new
    end

    # Get a key by kid (key ID)
    def get_key(kid : String?) : JWK?
      refresh_if_needed

      if kid
        @cache[kid]?
      else
        # If no kid specified, return first key (single-key JWKS)
        @cache.values.first?
      end
    rescue ex
      Log.error { "Failed to get JWK key: #{ex.message}" }
      nil
    end

    # Fetch keys from JWKS endpoint
    def fetch_keys : Array(JWK)
      Log.info { "Fetching JWKS from #{@jwks_url}" }

      response = ::HTTP::Client.get(@jwks_url, headers: ::HTTP::Headers{
        "Accept" => "application/json",
        "User-Agent" => "LavinMQ-JWKS-Client"
      })

      unless response.status.success?
        raise "JWKS fetch failed: HTTP #{response.status}"
      end

      jwks_data = JSON.parse(response.body)
      keys_array = jwks_data["keys"]?.try(&.as_a) || [] of JSON::Any

      keys = keys_array.compact_map do |key_json|
        parse_jwk(key_json)
      end

      @fetch_mutex.synchronize do
        @cache.clear
        keys.each do |key|
          cache_key = key.kid || "default"
          @cache[cache_key] = key
        end
        @last_fetch = Time.utc
      end

      Log.info { "Cached #{keys.size} JWK keys" }
      keys
    rescue ex
      Log.error { "Failed to fetch JWKS: #{ex.message}" }
      [] of JWK
    end

    private def parse_jwk(json : JSON::Any) : JWK?
      JWK.new(
        kid: json["kid"]?.try(&.as_s),
        kty: json["kty"].as_s,
        use: json["use"]?.try(&.as_s),
        alg: json["alg"]?.try(&.as_s),
        n: json["n"]?.try(&.as_s),
        e: json["e"]?.try(&.as_s),
        x: json["x"]?.try(&.as_s),
        y: json["y"]?.try(&.as_s),
        crv: json["crv"]?.try(&.as_s),
        raw_json: json
      )
    rescue ex
      Log.warn { "Failed to parse JWK: #{ex.message}" }
      nil
    end

    private def refresh_if_needed
      should_refresh = @fetch_mutex.synchronize do
        @cache.empty? || cache_expired?
      end

      fetch_keys if should_refresh
    end

    private def cache_expired? : Bool
      if last = @last_fetch
        Time.utc - last > @cache_ttl
      else
        true
      end
    end

    # Get cache statistics
    def stats
      {
        keys_cached: @cache.size,
        last_fetch: @last_fetch,
        cache_expired: cache_expired?
      }
    end
  end

  # JWT token validator
  class JWTValidator
    Log = ::Log.for("lavinmq.auth.jwt")

    def initialize(
      @jwks_client : JWKSClient,
      @required_issuer : String? = nil,
      @required_audience : String? = nil,
      @clock_skew : Time::Span = 60.seconds
    )
    end

    # Validate a JWT token and extract claims
    def validate(token : String) : TokenClaims?
      # Decode token to get header (without verification first)
      header = decode_header(token)
      return nil unless header

      # Get the appropriate key from JWKS
      kid = header["kid"]?.try(&.as_s)
      jwk = @jwks_client.get_key(kid)

      unless jwk
        Log.warn { "No JWK key found for kid: #{kid}" }
        return nil
      end

      # Convert JWK to PEM format for JWT library
      public_key = jwk_to_public_key(jwk)
      return nil unless public_key

      # Determine algorithm from JWK
      algorithm = determine_algorithm(jwk)
      return nil unless algorithm

      # Decode and verify JWT
      payload, _ = JWT.decode(token, public_key, algorithm, verify: true)

      # Verify standard claims
      return nil unless verify_claims(payload)

      # Extract claims into our record
      extract_claims(payload)
    rescue ex : JWT::DecodeError
      Log.warn { "JWT decode failed: #{ex.message}" }
      nil
    rescue ex : JWT::VerificationError
      Log.warn { "JWT verification failed: #{ex.message}" }
      nil
    rescue ex
      Log.error { "JWT validation error: #{ex.message}" }
      nil
    end

    private def decode_header(token : String) : Hash(String, JSON::Any)?
      parts = token.split('.')
      return nil unless parts.size == 3

      header_json = Base64.decode_string(parts[0])
      JSON.parse(header_json).as_h
    rescue
      nil
    end

    private def jwk_to_public_key(jwk : JWK) : String?
      case jwk.kty
      when "RSA"
        build_rsa_public_key(jwk)
      when "EC"
        Log.warn { "EC keys not yet fully supported" }
        nil
      else
        Log.warn { "Unsupported key type: #{jwk.kty}" }
        nil
      end
    end

    private def build_rsa_public_key(jwk : JWK) : String?
      # TODO: Full JWK to PEM conversion requires deeper OpenSSL bindings
      # Crystal's standard library has limited OpenSSL::RSA support
      #
      # For now, JWT validation with JWKS is not fully supported.
      # Use one of these alternatives:
      # 1. Userinfo endpoint validation (oauth2_userinfo_url) - fully working
      # 2. Provide PEM public key directly in config
      # 3. Contribute improved OpenSSL bindings to Crystal

      Log.warn { "JWK to PEM conversion not yet supported in Crystal. " \
                 "Falling back to userinfo validation." }
      nil
    end


    private def determine_algorithm(jwk : JWK) : JWT::Algorithm?
      alg_string = jwk.alg || "RS256"

      case alg_string
      when "RS256" then JWT::Algorithm::RS256
      when "RS384" then JWT::Algorithm::RS384
      when "RS512" then JWT::Algorithm::RS512
      when "ES256" then JWT::Algorithm::ES256
      when "ES384" then JWT::Algorithm::ES384
      when "ES512" then JWT::Algorithm::ES512
      else
        Log.warn { "Unsupported algorithm: #{alg_string}" }
        nil
      end
    end

    private def verify_claims(payload : Hash(String, JSON::Any)) : Bool
      now = Time.utc.to_unix

      # Verify expiration
      if exp = payload["exp"]?.try(&.as_i64)
        if exp < now - @clock_skew.total_seconds.to_i64
          Log.warn { "Token expired" }
          return false
        end
      end

      # Verify not before
      if nbf = payload["nbf"]?.try(&.as_i64)
        if nbf > now + @clock_skew.total_seconds.to_i64
          Log.warn { "Token not yet valid" }
          return false
        end
      end

      # Verify issuer
      if required_iss = @required_issuer
        token_iss = payload["iss"]?.try(&.as_s)
        unless token_iss == required_iss
          Log.warn { "Invalid issuer: #{token_iss} != #{required_iss}" }
          return false
        end
      end

      # Verify audience
      if required_aud = @required_audience
        token_aud = payload["aud"]?

        aud_match = case token_aud
        when JSON::Any
          if token_aud.as_s?
            token_aud.as_s == required_aud
          elsif token_aud.as_a?
            token_aud.as_a.any? { |a| a.as_s? == required_aud }
          else
            false
          end
        else
          false
        end

        unless aud_match
          Log.warn { "Invalid audience: #{token_aud}" }
          return false
        end
      end

      true
    end

    private def extract_claims(payload : Hash(String, JSON::Any)) : TokenClaims
      # Extract standard claims
      sub = payload["sub"]?.try(&.as_s) || "unknown"
      iss = payload["iss"]?.try(&.as_s)
      exp = payload["exp"]?.try(&.as_i64)
      iat = payload["iat"]?.try(&.as_i64)
      nbf = payload["nbf"]?.try(&.as_i64)
      scope = payload["scope"]?.try(&.as_s)
      email = payload["email"]?.try(&.as_s)
      preferred_username = payload["preferred_username"]?.try(&.as_s)

      # Extract audience (can be string or array)
      aud = if aud_json = payload["aud"]?
        if aud_str = aud_json.as_s?
          aud_str
        elsif aud_arr = aud_json.as_a?
          aud_arr.map(&.as_s)
        end
      end

      # Extract permissions if present
      permissions = extract_permissions(payload)

      # Store all other claims
      custom_claims = payload.reject { |k, _|
        ["sub", "iss", "aud", "exp", "iat", "nbf", "scope", "email", "preferred_username", "permissions"].includes?(k)
      }

      TokenClaims.new(
        sub: sub,
        iss: iss,
        aud: aud,
        exp: exp,
        iat: iat,
        nbf: nbf,
        scope: scope,
        email: email,
        preferred_username: preferred_username,
        permissions: permissions,
        custom_claims: custom_claims
      )
    end

    private def extract_permissions(payload : Hash(String, JSON::Any))
      perms_json = payload["permissions"]?
      return nil unless perms_json

      result = Hash(String, NamedTuple(configure: String, write: String, read: String)).new

      perms_json.as_h.each do |vhost, perm_obj|
        if perm_hash = perm_obj.as_h?
          result[vhost] = {
            configure: perm_hash["configure"]?.try(&.as_s) || "^$",
            write: perm_hash["write"]?.try(&.as_s) || ".*",
            read: perm_hash["read"]?.try(&.as_s) || ".*",
          }
        end
      end

      result.empty? ? nil : result
    rescue
      nil
    end
  end
end
