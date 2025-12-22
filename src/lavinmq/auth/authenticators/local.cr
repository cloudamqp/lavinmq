require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class LocalAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(context : Context) : User?
        if user = @users[context.username]?
          if default_user_only_loopback?(context)
            if passwd = user.password
              if passwd.verify(context.password)
                return user
              end
            end
          end
        end
      rescue ex : Exception
        Log.error { "Local authentication failed: #{ex.message}" }
      end

      private def default_user_only_loopback?(context) : Bool
        return true unless context.username == Config.instance.default_user
        return true unless Config.instance.default_user_only_loopback?
        context.loopback?
      end
    end
  end
end
