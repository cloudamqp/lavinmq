require "./context"

module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.authenticator"

      abstract def authenticate(context : Context) : BaseUser?
      abstract def cleanup

      # Called with the new config after a successful SIGHUP config reload
      def reload(config : Config) : Nil
      end
    end
  end
end
