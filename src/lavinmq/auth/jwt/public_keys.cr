require "./jwt"

module LavinMQ
  module Auth
    module JWT
      # Stores public keys (JWK) with expiration time.
      # Thread-safe storage for public keys used in JWT verification.
      class PublicKeys
        @keys : Hash(String, String)?
        @expires_at : Time?
        @mutex = Mutex.new

        def initialize
        end

        def empty?
          @keys.nil?
        end

        def get? : Hash(String, String)?
          @mutex.synchronize do
            return nil if @keys.nil?
            return nil if expired?
            @keys
          end
        end

        def update(keys : Hash(String, String), ttl : Time::Span)
          @mutex.synchronize do
            @keys = keys
            @expires_at = RoughTime.utc + ttl
          end
        end

        def clear
          @mutex.synchronize do
            @keys = nil
            @expires_at = nil
          end
        end

        def expires_at : Time?
          @expires_at
        end

        # Decodes and verifies a JWT token using the stored public keys.
        # Returns the decoded JWT::Token if verification succeeds.
        # Raises JWT::VerificationError if no key can verify the token or keys are unavailable/expired.
        #
        # The token must carry a `kid` header that matches one of the cached
        # keys; we then verify against that single key only. Iterating over
        # every key would let an unauthenticated caller force an RS256
        # verification per cached key with a forged token (GH #2077).
        def decode(token : String) : JWT::Token
          keys = get?
          raise JWT::ExpiredKeysError.new("Public keys unavailable or expired") unless keys

          kid = JWT::RS256Parser.decode_header(token).kid
          raise JWT::VerificationError.new("Token is missing kid header") if kid.nil? || kid.empty?

          key = keys[kid]?
          raise JWT::VerificationError.new("No key matching kid '#{kid}'") unless key

          JWT::RS256Parser.decode(token, key, verify: true)
        end

        private def expired? : Bool
          if expires_at = @expires_at
            RoughTime.utc >= expires_at
          else
            true
          end
        end
      end
    end
  end
end
