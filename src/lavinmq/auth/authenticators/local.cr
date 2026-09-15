require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class LocalAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(context : Context) : User?
        return unless user = find_user(context)
        return unless default_user_only_loopback?(context)
        return unless passwd = user.password
        return unless passwd.verify(context.password)
        user
      rescue ex : Exception
        Log.error(exception: ex) { "Local authentication failed: #{ex.message}" }
      end

      def cleanup
      end

      # Resolves the user for the login. If the vhost is known, a user scoped
      # to that vhost takes precedence over a global user with the same name.
      private def find_user(context) : User?
        if vhost = context.vhost
          @users.find(context.username, vhost)
        else
          @users[context.username]?
        end
      end

      private def default_user_only_loopback?(context) : Bool
        return true unless context.username == Config.instance.default_user
        return true unless Config.instance.default_user_only_loopback?
        context.loopback?
      end
    end
  end
end
