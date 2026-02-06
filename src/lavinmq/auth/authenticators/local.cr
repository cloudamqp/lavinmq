require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class LocalAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(context : Context) : User?
        return unless user = @users[context.username]?
        return unless default_user_only_loopback?(context)
        return unless passwd = user.password
        return unless passwd.verify(context.password)
        user
      rescue ex : Exception
        Log.error(exception: ex) { "Local authentication failed: #{ex.message}" }
      end

      def cleanup
      end

      private def default_user_only_loopback?(context) : Bool
        return true unless context.username == Config.instance.default_user
        return true unless Config.instance.default_user_only_loopback?
        context.loopback?
      end
    end
  end
end
