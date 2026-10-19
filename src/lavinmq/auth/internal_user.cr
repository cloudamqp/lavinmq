require "./base_user"
require "../tag"

module LavinMQ
  module Auth
    # The broker's own identity, used where the server acts on its own behalf:
    # requests over the internal control socket (lavinmqctl). It has no
    # password and is never stored in the UserStore, so no authenticator can
    # ever resolve a login to it; the only way to act as it is from inside the
    # process. In-process shovels and federation links don't act as any user at
    # all: they are authorized when their parameter is created (see
    # Shovel::Store.validate_config!).
    class InternalUser < BaseUser
      NAME = "__internal"

      FULL_ACCESS = {config: /.*/, read: /.*/, write: /.*/}

      getter name : String = NAME
      getter tags : Array(Tag) = [Tag::Administrator]
      # Full access is granted by find_permission, for every vhost including
      # ones created later; nothing is stored per vhost.
      getter permissions : Hash(String, Permissions) = Hash(String, Permissions).new

      def find_permission(vhost : String) : Permissions?
        FULL_ACCESS
      end
    end
  end
end
