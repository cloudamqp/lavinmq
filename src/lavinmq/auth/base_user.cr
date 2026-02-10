require "json"
require "./permission_cache"

module LavinMQ
  module Auth
    abstract class BaseUser
      alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)

      abstract def name : String
      abstract def tags : Array(Tag)
      abstract def permissions : Hash(String, Permissions)

      @permission_revision = Atomic(UInt32).new(0_u32)

      def details_tuple
        user_details.merge(permissions: @permissions)
      end

      def user_details
        {
          name: name,
          tags: tags.map(&.to_downcase_s).join(","),
        }
      end

      def permissions_details(vhost, p)
        {
          user:      name,
          vhost:     vhost,
          configure: p[:config],
          read:      p[:read],
          write:     p[:write],
        }
      end

      def can_write?(vhost : String, name : String, cache : PermissionCache) : Bool
        permission_revision = @permission_revision.lazy_get
        if permission_revision != cache.revision
          cache.clear
          cache.revision = permission_revision
        end

        key = PermissionKey.new(vhost, name)
        result = cache[key]?
        return result unless result.nil?

        cache[key] = can_write?(vhost, name)
      end

      def can_write?(vhost : String, name : String) : Bool
        perm = find_permission(vhost)
        perm ? perm_match?(perm[:write], name) : false
      end

      def can_read?(vhost : String, name : String) : Bool
        perm = find_permission(vhost)
        perm ? perm_match?(perm[:read], name) : false
      end

      def can_config?(vhost : String, name : String) : Bool
        perm = find_permission(vhost)
        perm ? perm_match?(perm[:config], name) : false
      end

      def find_permission(vhost : String) : Permissions?
        permissions[vhost]?
      end

      def can_impersonate? : Bool
        tags.includes? Tag::Impersonator
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
