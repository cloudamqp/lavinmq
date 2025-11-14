require "json"

module LavinMQ
  module Auth
    alias CacheKey = Tuple(String, String)

    class PermissionCache
      @cache = Hash(CacheKey, Bool).new
      property revision = 0_u32

      forward_missing_to @cache
    end

    abstract class User
      alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)
      alias CacheKey = Tuple(String, String)

      abstract def name : String
      abstract def tags : Array(Tag)
      abstract def permissions : Hash(String, Permissions)
      abstract def permissions_details(vhost : String, p : Permissions)

      @acl_cache = Hash(CacheKey, Bool).new
      @permission_revision = Atomic(UInt32).new(0_u32)

      def can_write?(vhost : String, name : String, cache : PermissionCache) : Bool
        permission_revision = @permission_revision.lazy_get
        if permission_revision != cache.revision
          cache.clear
          cache.revision = permission_revision
        end

        result = cache[{vhost, name}]?
        return result unless result.nil?

        cache[{vhost, name}] = can_write?(vhost, name)
      end

      def can_write?(vhost : String, name : String) : Bool
        perm = permissions[vhost]?
        perm ? perm_match?(perm[:write], name) : false
      end

      def can_read?(vhost : String, name : String) : Bool
        perm = permissions[vhost]?
        perm ? perm_match?(perm[:read], name) : false
      end

      def can_config?(vhost : String, name : String) : Bool
        perm = permissions[vhost]?
        perm ? perm_match?(perm[:config], name) : false
      end

      def can_impersonate? : Bool
        tags.includes? Tag::Impersonator
      end

      def update_secret(new_secret : Bytes) : Bool
        false
      end

      def clear_permissions_cache
        @permission_revision.add(1, :relaxed)
      end

      private def perm_match?(perm : Regex, name : String) : Bool
        perm != /^$/ && perm != // && perm.matches? name
      end
    end
  end
end
