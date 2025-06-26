require "../user"
require "../password"
require "../../tag"

module LavinMQ
  module Auth
    module Users
      class TempUser < User
        include SortableJSON

        getter name, password, permissions
        property tags
        alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)

        @name : String
        @permissions = Hash(String, Permissions).new
        @password : Password? = nil
        @tags = Array(Tag).new
        @expiration_time : Time?

        def initialize
          pp "hello"
          @name = "guest"
          @password = Password::MD5Password.create("guest")
          @tags = [Tag::Administrator]
          @permissions["/"] = {config: /.*/, read: /.*/, write: /.*/}
          @expiration_time = nil
        end

        def set_expiration(time : Time)
          @expiration_time = time
        end

        def expired? : Bool
          return false unless @expiration_time
          Time.utc > @expiration_time.not_nil!
        end

        def can_write?(vhost, name) : Bool
          pp "hellooo"
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
            name:              @name,
            password_hash:     @password,
            hashing_algorithm: @password.try &.hash_algorithm,
            tags:              @tags.map(&.to_s.downcase).join(","),
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
