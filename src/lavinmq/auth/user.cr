require "json"

module LavinMQ
  module Auth
    abstract class User
      alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)

      abstract def name : String
      abstract def tags : Array(Tag)
      abstract def permissions : Hash(String, Permissions)
      abstract def permissions_details(vhost : String, p : Permissions)

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

      private def perm_match?(perm : Regex, name : String) : Bool
        perm != /^$/ && perm != // && perm.matches? name
      end
    end
  end
end
