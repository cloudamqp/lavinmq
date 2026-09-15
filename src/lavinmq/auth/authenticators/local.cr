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

      # Resolves the user for the login. If the vhost is known (MQTT) a user
      # scoped to that vhost takes precedence over a global user. Otherwise
      # (AMQP, where the vhost isn't known until Connection.Open) a global
      # user is looked up first, then the username is interpreted as
      # `vhost:name` to find a vhost scoped user.
      private def find_user(context) : User?
        if vhost = context.vhost
          return @users.find(context.username, vhost)
        end
        if user = @users[context.username]?
          return user
        end
        if idx = context.username.index(':')
          @users[context.username[idx + 1..], context.username[0, idx]]?
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
