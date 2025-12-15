require "./jwt"

module LavinMQ
  module Auth
    # Stores public keys (JWK) with expiration time.
    # Thread-safe storage for public keys used in JWT verification.
    class PublicKeys
      @keys : Hash(String, String)?
      @expires_at : Time?
      @mutex = Mutex.new

      def initialize
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

      # Decodes and verifies a JWT token using the stored public keys.
      # Returns the decoded JWT::Token if verification succeeds.
      # Raises JWT::VerificationError if no key can verify the token or keys are unavailable/expired.
      def decode(token : String) : JWT::Token
        keys = get?
        raise JWT::ExpiredKeysError.new("Public keys unavailable or expired") unless keys

        kid = JWT::RS256Parser.decode_header(token)["kid"]?.try(&.as_s) rescue nil
        # If we know the kid matches a key we can avoid iterating through all keys
        if kid && keys[kid]?
          return JWT::RS256Parser.decode(token, keys[kid], verify: true)
        end

        keys.each_value do |key|
          return JWT::RS256Parser.decode(token, key, verify: true)
        rescue JWT::VerificationError
        end
        raise JWT::VerificationError.new("Could not verify JWT with any key")
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
