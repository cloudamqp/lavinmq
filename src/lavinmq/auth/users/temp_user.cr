require "json"
require "../user"
require "../../sortable_json"
require "../../tag"

module LavinMQ
  module Auth
    module Users
      class TempUser < User
        include SortableJSON
        getter name, permissions
        property tags
        alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)

        @name : String
        @permissions = Hash(String, Permissions).new
        @tags = Array(Tag).new
        @expires_at : Time

        def initialize(name : String, tags : Array(Tag), permissions : Hash(String, Permissions), expires_at : Time)
          @name = name
          @tags = tags
          @permissions = permissions
          @expires_at = expires_at
        end

        def set_expiration(time : Time)
          @expires_at = time
        end

        def expired? : Bool
          Time.utc > @expires_at
        end

        def can_write?(vhost, name) : Bool
          return false if expired?
          perm = permissions[vhost]?
          perm ? perm_match?(perm[:write], name) : false
        end

        def can_read?(vhost, name) : Bool
          return false if expired?
          perm = permissions[vhost]?
          perm ? perm_match?(perm[:read], name) : false
        end

        def can_config?(vhost, name) : Bool
          return false if expired?
          perm = permissions[vhost]?
          perm ? perm_match?(perm[:config], name) : false
        end

        def can_impersonate?
          return false if expired?
          @tags.includes? Tag::Impersonator
        end

        def hidden?
          false
        end

        def details_tuple
          user_details.merge(permissions: @permissions)
        end

        def user_details
          {
            name: @name,
            tags: @tags.map(&.to_s.downcase).join(","),
          }
        end

        def permissions_details
          @permissions.map { |k, p| permissions_details(k, p) }
        end

        def permissions_details(vhost, p)
          {
            user:      @name,
            vhost:     vhost,
            configure: p[:config],
            read:      p[:read],
            write:     p[:write],
          }
        end

        private def perm_match?(perm, name)
          perm != /^$/ && perm != // && perm.matches? name
        end
      end
    end
  end
end
