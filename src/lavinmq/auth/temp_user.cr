require "json"
require "./user"

module LavinMQ
  module Auth
    class TempUser < User
      @expires_at : Time

      def initialize(name : String, tags : Array(Tag), permissions : Hash(String, Permissions), expires_at : Time)
        @name = name
        @tags = tags
        @permissions = permissions
        @expires_at = expires_at
        @password = nil
        @plain_text_password = nil
      end

      # Override password-related methods - temp users don't have passwords
      def password : Password?
        nil
      end

      def update_password_hash(password_hash, hash_algorithm)
        # Temp users cannot have passwords
        return nil
      end

      def update_password(password, hash_algorithm = "sha256")
        # Temp users cannot have passwords
        return nil
      end

      def set_expiration(time : Time)
        @expires_at = time
      end

      def expired? : Bool
        Time.utc > @expires_at
      end

      # Override permission checks to include expiration checks
      def can_write?(vhost, name) : Bool
        return false if expired?
        super
      end

      def can_read?(vhost, name) : Bool
        return false if expired?
        super
      end

      def can_config?(vhost, name) : Bool
        return false if expired?
        super
      end

      def can_impersonate?
        return false if expired?
        super
      end

      def hidden?
        false
      end

      # Override user_details to match parent structure but without password data
      def user_details
        {
          name:              @name,
          password_hash:     nil,
          hashing_algorithm: nil,
          tags:              @tags.map(&.to_s.downcase).join(","),
          expires_at:        @expires_at.to_s,
        }
      end
    end
  end
end
