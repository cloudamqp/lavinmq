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
          return nil if expired_unlocked?
          @keys
        end
      end

      def update(keys : Hash(String, String), ttl : Time::Span)
        @mutex.synchronize do
          @keys = keys
          @expires_at = Time.utc + ttl
        end
      end

      def clear
        @mutex.synchronize do
          @keys = nil
          @expires_at = nil
        end
      end

      private def expired_unlocked? : Bool
        if expires_at = @expires_at
          Time.utc >= expires_at
        else
          true
        end
      end
    end
  end
end
